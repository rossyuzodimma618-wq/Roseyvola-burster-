const express = require("express");
const path = require("path");
const WebSocket = require("ws");

const app = express();
const PORT = process.env.PORT || 3000;

/* =========================================================
   EXPRESS
========================================================= */

app.use(express.json());
app.use(express.static(path.join(__dirname, "public")));

/* =========================================================
   DERIV
========================================================= */

const DERIV_WS_URL =
  "wss://api.derivws.com/trading/v1/options/ws/public";

/* =========================================================
   TELEGRAM
========================================================= */

const TELEGRAM_BOT_TOKEN = process.env.TELEGRAM_BOT_TOKEN;
const TELEGRAM_CHAT_ID = process.env.TELEGRAM_CHAT_ID;

/* =========================================================
   TIMEFRAMES
========================================================= */

const H1_GRANULARITY = 3600;
const M15_GRANULARITY = 900;
const M5_GRANULARITY = 300;

/*
  120 candles is more than enough for:
  EMA20
  ATR14
  RSI
  BOS/CHoCH
  Liquidity
  Burst Fader

  Reducing from 250 does NOT make the strategy stricter.
*/
const CANDLE_COUNT = 120;

/* =========================================================
   SCANNER SETTINGS
========================================================= */

const SCAN_INTERVAL_MS = 60000;
const REQUEST_TIMEOUT_MS = 15000;

/*
  Minimum delay between Deriv requests.
  This protects the API from bursts.
*/
const DERIV_REQUEST_GAP_MS = 400;

/*
  Active symbols do not need to be requested every scan.
*/
const ACTIVE_SYMBOL_REFRESH_MS = 10 * 60 * 1000;

/*
  Retry after rate-limit errors.
*/
const MAX_REQUEST_RETRIES = 3;

/*
  Telegram alert cooldown.
*/
const ALERT_COOLDOWN_MS = 15 * 60 * 1000;

/* =========================================================
   STATE
========================================================= */

const state = {
  online: true,
  derivConnected: false,
  markets: [],
  lastScan: null,
  error: null
};

const alertState = new Map();
const burstWarningState = new Map();

let scanInProgress = false;

/* =========================================================
   DERIV PERSISTENT CONNECTION STATE
========================================================= */

let derivWs = null;
let derivConnectPromise = null;
let derivRequestId = 1;

let lastDerivRequestAt = 0;

/*
  All requests pass through this queue.
  This prevents multiple requests from hitting Deriv
  simultaneously.
*/
let derivQueue = Promise.resolve();

const derivPending = new Map();

/*
  Cache active symbols.
*/
let activeSymbolsCache = [];
let activeSymbolsFetchedAt = 0;

/* =========================================================
   UTILITIES
========================================================= */

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function round(value, decimals = 3) {
  if (!Number.isFinite(value)) return null;

  const factor = Math.pow(10, decimals);

  return Math.round(value * factor) / factor;
}

function formatPrice(value, decimals = 3) {
  if (!Number.isFinite(value)) return "—";

  return Number(value).toFixed(decimals);
}

function getDecimals(symbol) {
  const s = String(symbol || "").toUpperCase();

  if (s.includes("XAU")) return 2;

  if (
    s.includes("JPY") ||
    /^1HZ\d+V$/i.test(s)
  ) {
    return 3;
  }

  return 3;
}

function isRateLimitError(error) {
  if (!error) return false;

  const text = String(
    error.message ||
    error.code ||
    error ||
    ""
  ).toLowerCase();

  return (
    text.includes("429") ||
    text.includes("rate limit") ||
    text.includes("ratelimit") ||
    text.includes("too many")
  );
}

/* =========================================================
   DERIV CONNECTION
========================================================= */

function rejectAllPending(error) {
  for (const [reqId, pending] of derivPending.entries()) {
    clearTimeout(pending.timer);

    try {
      pending.reject(error);
    } catch (_) {}

    derivPending.delete(reqId);
  }
}

function attachDerivHandlers(ws) {
  ws.on("message", (raw) => {
    let data;

    try {
      data = JSON.parse(raw.toString());
    } catch (error) {
      return;
    }

    /*
      Match the response to the request.
    */
    let reqId = data.req_id;

    /*
      Some responses may not include req_id.
      Because we only allow one request at a time,
      safely match the only pending request.
    */
    if (
      reqId == null &&
      derivPending.size === 1
    ) {
      reqId = [...derivPending.keys()][0];
    }

    if (reqId == null) return;

    const pending = derivPending.get(reqId);

    if (!pending) return;

    clearTimeout(pending.timer);
    derivPending.delete(reqId);

    if (data.error) {
      const error = new Error(
        data.error.message ||
        data.error.code ||
        "Deriv API error"
      );

      error.code = data.error.code || "DERIV_ERROR";

      pending.reject(error);
      return;
    }

    pending.resolve(data);
  });

  ws.on("close", () => {
    if (derivWs === ws) {
      derivWs = null;
      state.derivConnected = false;
    }

    rejectAllPending(
      new Error("Deriv WebSocket connection closed")
    );
  });

  ws.on("error", (error) => {
    state.error = error.message;

    /*
      If the socket was already connected,
      the close event will clean up pending requests.
    */
  });
}

function openDerivSocket() {
  return new Promise((resolve, reject) => {
    let settled = false;

    const ws = new WebSocket(DERIV_WS_URL);

    /*
      IMPORTANT:
      HTTP 429 can happen before the WebSocket
      handshake completes.
    */
    ws.on("unexpected-response", (request, response) => {
      const statusCode = response.statusCode;

      const error = new Error(
        `Deriv WebSocket HTTP ${statusCode}`
      );

      if (statusCode === 429) {
        error.code = "DERIV_RATE_LIMIT";
      }

      if (!settled) {
        settled = true;
        reject(error);
      }

      try {
        response.resume();
      } catch (_) {}
    });

    ws.on("open", () => {
      if (settled) return;

      settled = true;

      derivWs = ws;
      state.derivConnected = true;
      state.error = null;

      attachDerivHandlers(ws);

      console.log("Deriv WebSocket connected");

      resolve();
    });

    ws.on("error", (error) => {
      if (!settled) {
        settled = true;
        reject(error);
      }
    });

    ws.on("close", () => {
      if (!settled) {
        settled = true;

        reject(
          new Error(
            "Deriv WebSocket closed before connection"
          )
        );
      }
    });
  });
}

async function ensureDerivConnection() {
  if (
    derivWs &&
    derivWs.readyState === WebSocket.OPEN
  ) {
    state.derivConnected = true;
    return;
  }

  if (derivConnectPromise) {
    return derivConnectPromise;
  }

  derivConnectPromise = (async () => {
    let lastError = null;

    for (let attempt = 1; attempt <= 5; attempt++) {
      try {
        await openDerivSocket();

        return;
      } catch (error) {
        lastError = error;

        state.derivConnected = false;

        const backoff =
          Math.min(
            30000,
            3000 * Math.pow(2, attempt - 1)
          );

        console.log(
          `Deriv connection attempt ${attempt} failed: ${error.message}`
        );

        console.log(
          `Waiting ${backoff}ms before reconnecting...`
        );

        await sleep(backoff);
      }
    }

    throw (
      lastError ||
      new Error("Unable to connect to Deriv")
    );
  })();

  try {
    await derivConnectPromise;
  } finally {
    derivConnectPromise = null;
  }
}

/* =========================================================
   SEND ONE REQUEST THROUGH THE PERSISTENT CONNECTION
========================================================= */

async function executeDerivRequest(payload) {
  let lastError = null;

  for (
    let attempt = 1;
    attempt <= MAX_REQUEST_RETRIES;
    attempt++
  ) {
    try {
      await ensureDerivConnection();

      /*
        Pace requests.
      */
      const elapsed =
        Date.now() - lastDerivRequestAt;

      if (
        elapsed < DERIV_REQUEST_GAP_MS
      ) {
        await sleep(
          DERIV_REQUEST_GAP_MS - elapsed
        );
      }

      if (
        !derivWs ||
        derivWs.readyState !== WebSocket.OPEN
      ) {
        throw new Error(
          "Deriv WebSocket is not open"
        );
      }

      const reqId = derivRequestId++;

      const request = {
        ...payload,
        req_id: reqId
      };

      const response = await new Promise(
        (resolve, reject) => {
          const timer = setTimeout(() => {
            derivPending.delete(reqId);

            reject(
              new Error(
                "Deriv request timed out"
              )
            );
          }, REQUEST_TIMEOUT_MS);

          derivPending.set(reqId, {
            resolve,
            reject,
            timer
          });

          try {
            derivWs.send(
              JSON.stringify(request)
            );

            lastDerivRequestAt = Date.now();
          } catch (error) {
            clearTimeout(timer);
            derivPending.delete(reqId);
            reject(error);
          }
        }
      );

      return response;
    } catch (error) {
      lastError = error;

      console.log(
        `Deriv request failed: ${error.message}`
      );

      if (isRateLimitError(error)) {
        const waitTime =
          5000 * attempt;

        console.log(
          `Rate limit detected. Waiting ${waitTime}ms...`
        );

        await sleep(waitTime);

        /*
          If the server rate-limited the connection,
          close it so the next attempt can reconnect.
        */
        if (derivWs) {
          try {
            derivWs.close();
          } catch (_) {}

          derivWs = null;
        }

        state.derivConnected = false;

        continue;
      }

      /*
        Connection errors can also be retried.
      */
      const text =
        String(error.message || "").toLowerCase();

      if (
        text.includes("closed") ||
        text.includes("not open") ||
        text.includes("timed out") ||
        text.includes("socket")
      ) {
        await sleep(2000);

        continue;
      }

      throw error;
    }
  }

  throw (
    lastError ||
    new Error("Deriv request failed")
  );
}

/* =========================================================
   PUBLIC DERIV REQUEST QUEUE
========================================================= */

function derivRequest(payload) {
  const run = derivQueue.then(
    () => executeDerivRequest(payload),
    () => executeDerivRequest(payload)
  );

  /*
    Keep the queue alive even if this request fails.
  */
  derivQueue = run.catch(() => {});

  return run;
}

/* =========================================================
   ACTIVE SYMBOLS
========================================================= */

async function getActiveSymbols() {
  const now = Date.now();

  if (
    activeSymbolsCache.length > 0 &&
    now - activeSymbolsFetchedAt <
      ACTIVE_SYMBOL_REFRESH_MS
  ) {
    return activeSymbolsCache;
  }

  const data = await derivRequest({
    active_symbols: "brief"
  });

  const symbols =
    Array.isArray(data.active_symbols)
      ? data.active_symbols
      : [];

  const result = [];

  for (const item of symbols) {
    const symbol = String(
      item.symbol ||
      item.underlying_symbol ||
      ""
    );

    const name = String(
      item.display_name ||
      item.underlying_symbol_name ||
      ""
    );

    const type = String(
      item.symbol_type ||
      item.underlying_symbol_type ||
      ""
    );

    const market = String(
      item.market ||
      ""
    );

    const combined = `${symbol} ${name} ${type} ${market}`;

    /*
      Volatility indices.

      This supports both older and newer
      Deriv symbol formats.
    */
    const isVolatility =
      /volatility/i.test(combined) ||
      /^1HZ\d+V$/i.test(symbol) ||
      /^R_\d+$/i.test(symbol);

    if (!isVolatility) continue;

    result.push({
      symbol,
      name: name || symbol,
      type,
      market
    });
  }

  activeSymbolsCache = result;
  activeSymbolsFetchedAt = now;

  console.log(
    `Found ${result.length} Volatility indices`
  );

  return result;
}

/* =========================================================
   CANDLE REQUEST
========================================================= */

async function requestCandles(
  symbol,
  granularity
) {
  const data = await derivRequest({
    ticks_history: symbol,
    end: "latest",
    count: CANDLE_COUNT,
    style: "candles",
    granularity,
    subscribe: 0
  });

  if (
    data &&
    data.history &&
    Array.isArray(data.history.candles)
  ) {
    return data.history.candles;
  }

  if (
    data &&
    Array.isArray(data.candles)
  ) {
    return data.candles;
  }

  throw new Error(
    `No candles returned for ${symbol}`
  );
}

/* =========================================================
   CLEAN CANDLES
========================================================= */

function cleanCandles(rawCandles) {
  if (!Array.isArray(rawCandles)) {
    return [];
  }

  const candles = rawCandles
    .map((candle) => ({
      time: Number(
        candle.epoch ||
        candle.time ||
        0
      ),

      open: Number(candle.open),

      high: Number(candle.high),

      low: Number(candle.low),

      close: Number(candle.close)
    }))
    .filter(
      (c) =>
        Number.isFinite(c.open) &&
        Number.isFinite(c.high) &&
        Number.isFinite(c.low) &&
        Number.isFinite(c.close)
    )
    .sort((a, b) => a.time - b.time);

  /*
    Remove the latest candle because it can still be forming.
  */
  if (candles.length > 1) {
    candles.pop();
  }

  return candles;
}

/* =========================================================
   INDICATORS
========================================================= */

function ema(values, period = 20) {
  if (
    !Array.isArray(values) ||
    values.length < period
  ) {
    return null;
  }

  const multiplier =
    2 / (period + 1);

  let emaValue = 0;

  for (let i = 0; i < period; i++) {
    emaValue += values[i];
  }

  emaValue /= period;

  for (
    let i = period;
    i < values.length;
    i++
  ) {
    emaValue =
      (values[i] - emaValue) *
        multiplier +
      emaValue;
  }

  return emaValue;
}

function atr(candles, period = 14) {
  if (
    !Array.isArray(candles) ||
    candles.length < period + 1
  ) {
    return null;
  }

  const trueRanges = [];

  for (let i = 1; i < candles.length; i++) {
    const current = candles[i];
    const previous = candles[i - 1];

    const tr = Math.max(
      current.high - current.low,

      Math.abs(
        current.high - previous.close
      ),

      Math.abs(
        current.low - previous.close
      )
    );

    trueRanges.push(tr);
  }

  if (trueRanges.length < period) {
    return null;
  }

  let value = 0;

  for (let i = 0; i < period; i++) {
    value += trueRanges[i];
  }

  value /= period;

  for (
    let i = period;
    i < trueRanges.length;
    i++
  ) {
    value =
      (value * (period - 1) +
        trueRanges[i]) /
      period;
  }

  return value;
}

function rsi(values, period = 14) {
  if (
    !Array.isArray(values) ||
    values.length < period + 1
  ) {
    return null;
  }

  let gains = 0;
  let losses = 0;

  for (let i = 1; i <= period; i++) {
    const change =
      values[i] - values[i - 1];

    if (change >= 0) {
      gains += change;
    } else {
      losses += Math.abs(change);
    }
  }

  let averageGain =
    gains / period;

  let averageLoss =
    losses / period;

  for (
    let i = period + 1;
    i < values.length;
    i++
  ) {
    const change =
      values[i] - values[i - 1];

    const gain =
      change > 0 ? change : 0;

    const loss =
      change < 0 ? Math.abs(change) : 0;

    averageGain =
      (averageGain * (period - 1) +
        gain) /
      period;

    averageLoss =
      (averageLoss * (period - 1) +
        loss) /
      period;
  }

  if (averageLoss === 0) {
    return 100;
  }

  const rs =
    averageGain / averageLoss;

  return 100 - 100 / (1 + rs);
}

/* =========================================================
   DIRECTION
========================================================= */

function getDirection(candles) {
  if (
    !candles ||
    candles.length < 25
  ) {
    return "NEUTRAL";
  }

  const closes =
    candles.map((c) => c.close);

  const current =
    closes[closes.length - 1];

  const ema20 =
    ema(closes, 20);

  const previous =
    closes[closes.length - 6];

  if (
    current > ema20 &&
    current > previous
  ) {
    return "BULLISH";
  }

  if (
    current < ema20 &&
    current < previous
  ) {
    return "BEARISH";
  }

  return "NEUTRAL";
}

/* =========================================================
   STRUCTURE
========================================================= */

function getStructure(candles) {
  if (
    !candles ||
    candles.length < 20
  ) {
    return "NEUTRAL";
  }

  const recent =
    candles.slice(-12);

  const last =
    recent[recent.length - 1];

  const previous =
    recent.slice(0, -1);

  const previousHigh = Math.max(
    ...previous.map(
      (c) => c.high
    )
  );

  const previousLow = Math.min(
    ...previous.map(
      (c) => c.low
    )
  );

  if (last.close > previousHigh) {
    return "BULLISH BOS";
  }

  if (last.close < previousLow) {
    return "BEARISH BOS";
  }

  const a =
    recent[recent.length - 2];

  const b =
    recent[recent.length - 1];

  if (
    b.high > a.high &&
    b.low > a.low &&
    b.close > a.close
  ) {
    return "BULLISH CHoCH";
  }

  if (
    b.high < a.high &&
    b.low < a.low &&
    b.close < a.close
  ) {
    return "BEARISH CHoCH";
  }

  return "NEUTRAL";
}

/* =========================================================
   LIQUIDITY SWEEP
========================================================= */

function getLiquiditySweep(candles) {
  if (
    !candles ||
    candles.length < 15
  ) {
    return "NONE";
  }

  const last =
    candles[candles.length - 1];

  const previous =
    candles.slice(-11, -1);

  const previousLow = Math.min(
    ...previous.map(
      (c) => c.low
    )
  );

  const previousHigh = Math.max(
    ...previous.map(
      (c) => c.high
    )
  );

  /*
    Bullish liquidity sweep:
    price takes previous low
    then closes back above it.
  */
  if (
    last.low < previousLow &&
    last.close > previousLow
  ) {
    return "BULLISH";
  }

  /*
    Bearish liquidity sweep:
    price takes previous high
    then closes back below it.
  */
  if (
    last.high > previousHigh &&
    last.close < previousHigh
  ) {
    return "BEARISH";
  }

  return "NONE";
}

/* =========================================================
   STRONG BURST FADER
========================================================= */

function getStrongBurstFader(candles) {
  if (
    !candles ||
    candles.length < 25
  ) {
    return {
      direction: "NONE",
      stack: 0,
      heat: "COOL",
      fadeBias: "NONE"
    };
  }

  const closes =
    candles.map((c) => c.close);

  const basis =
    ema(closes, 20);

  const currentAtr =
    atr(candles, 14);

  if (
    !Number.isFinite(basis) ||
    !Number.isFinite(currentAtr) ||
    currentAtr <= 0
  ) {
    return {
      direction: "NONE",
      stack: 0,
      heat: "COOL",
      fadeBias: "NONE"
    };
  }

  const last =
    candles[candles.length - 1];

  const upper =
    basis + 2 * currentAtr;

  const lower =
    basis - 2 * currentAtr;

  let direction = "NONE";
  let stack = 0;

  /*
    UP BURST
  */
  if (last.high > upper) {
    direction = "UP BURST";

    stack = Math.ceil(
      (last.high - upper) /
        (0.5 * currentAtr)
    );

    stack = Math.max(
      1,
      Math.min(6, stack)
    );
  }

  /*
    DOWN BURST
  */
  else if (last.low < lower) {
    direction = "DOWN BURST";

    stack = Math.ceil(
      (lower - last.low) /
        (0.5 * currentAtr)
    );

    stack = Math.max(
      1,
      Math.min(6, stack)
    );
  }

  let heat = "COOL";

  if (stack >= 5) {
    heat = "EXTREME";
  } else if (stack >= 3) {
    heat = "HOT";
  } else if (stack >= 1) {
    heat = "WARM";
  }

  let fadeBias = "NONE";

  if (direction === "UP BURST") {
    fadeBias = "BEARISH";
  }

  if (direction === "DOWN BURST") {
    fadeBias = "BULLISH";
  }

  return {
    direction,
    stack,
    heat,
    fadeBias
  };
}

/* =========================================================
   BURST SUPPORT
========================================================= */

function getBurstSupport(
  burst,
  liquidity,
  m5Structure
) {
  let supportsBuy = false;
  let supportsSell = false;

  /*
    Downside burst can support a BUY
    when liquidity or structure confirms.
  */
  if (
    burst.direction === "DOWN BURST" &&
    (
      liquidity === "BULLISH" ||
      m5Structure.startsWith("BULLISH")
    )
  ) {
    supportsBuy = true;
  }

  /*
    Upside burst can support a SELL
    when liquidity or structure confirms.
  */
  if (
    burst.direction === "UP BURST" &&
    (
      liquidity === "BEARISH" ||
      m5Structure.startsWith("BEARISH")
    )
  ) {
    supportsSell = true;
  }

  return {
    supportsBuy,
    supportsSell
  };
}

/* =========================================================
   BURST WARNING
========================================================= */

function getBurstWarningStage(burst) {
  if (!burst || burst.stack <= 0) {
    return null;
  }

  if (burst.stack >= 3) {
    return "STRONG";
  }

  if (burst.stack === 2) {
    return "EARLY";
  }

  return null;
}

function shouldSendBurstWarning(
  symbol,
  burst
) {
  const stage =
    getBurstWarningStage(burst);

  const previous =
    burstWarningState.get(symbol);

  /*
    Reset when no burst exists.
  */
  if (!stage) {
    burstWarningState.delete(symbol);
    return false;
  }

  if (!previous) {
    burstWarningState.set(
      symbol,
      {
        direction: burst.direction,
        stage
      }
    );

    return true;
  }

  /*
    Direction changed.
  */
  if (
    previous.direction !==
    burst.direction
  ) {
    burstWarningState.set(
      symbol,
      {
        direction: burst.direction,
        stage
      }
    );

    return true;
  }

  /*
    EARLY -> STRONG
  */
  if (
    previous.stage === "EARLY" &&
    stage === "STRONG"
  ) {
    burstWarningState.set(
      symbol,
      {
        direction: burst.direction,
        stage
      }
    );

    return true;
  }

  return false;
}

/* =========================================================
   TELEGRAM
========================================================= */

async function sendTelegramMessage(
  message
) {
  if (
    !TELEGRAM_BOT_TOKEN ||
    !TELEGRAM_CHAT_ID
  ) {
    return;
  }

  try {
    const url =
      `https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage`;

    const response =
      await fetch(url, {
        method: "POST",
        headers: {
          "Content-Type":
            "application/json"
        },
        body: JSON.stringify({
          chat_id:
            TELEGRAM_CHAT_ID,
          text: message
        })
      });

    if (!response.ok) {
      console.log(
        "Telegram error:",
        await response.text()
      );
    }
  } catch (error) {
    console.log(
      "Telegram request failed:",
      error.message
    );
  }
}

/* =========================================================
   BURST WARNING ALERT
========================================================= */

async function sendBurstWarning(
  symbol,
  burst,
  price
) {
  const direction =
    burst.direction === "UP BURST"
      ? "UPSIDE"
      : "DOWNSIDE";

  const possible =
    burst.fadeBias === "BEARISH"
      ? "SELL REVERSAL"
      : "BUY REVERSAL";

  const message =
`⚠️ STRONG BURST WARNING

📊 ${symbol}
💥 ${burst.direction}
🔥 Heat: ${burst.heat}
📚 Stack: ${burst.stack}/6

💰 Price: ${formatPrice(
      price,
      getDecimals(symbol)
    )}

🔄 Possible: ${possible}

⚠️ WAIT FOR CONFIRMATION
Liquidity → BOS/CHoCH → Retracement

This is a warning, NOT an entry signal.`;

  await sendTelegramMessage(message);
}

/* =========================================================
   NORMAL ALERT
========================================================= */

async function sendTelegramAlert(
  result
) {
  if (
    result.signal !== "BUY" &&
    result.signal !== "SELL"
  ) {
    return;
  }

  const now = Date.now();

  const previous =
    alertState.get(result.symbol);

  if (
    previous &&
    now - previous <
      ALERT_COOLDOWN_MS
  ) {
    return;
  }

  alertState.set(
    result.symbol,
    now
  );

  const emoji =
    result.signal === "BUY"
      ? "🟢"
      : "🔴";

  const decimals =
    getDecimals(result.symbol);

  const message =
`${emoji} STRONG ${result.signal}: ${result.symbol}

📍 Entry: ${formatPrice(
    result.entry,
    decimals
  )}

🛑 Stop Loss: ${formatPrice(
    result.stopLoss,
    decimals
  )}

🎯 Take Profit: ${formatPrice(
    result.takeProfit,
    decimals
  )}

⭐ Score: ${result.score}/5

📊 1H: ${result.h1Direction}
📊 15M: ${result.m15Direction}
📊 5M: ${result.m5Direction}

🔎 15M Structure:
${result.m15Structure}

🔎 5M Structure:
${result.m5Structure}

💧 Liquidity:
${result.liquidity}

💥 Strong Burst Fader:
${result.burst.direction}

🔥 Burst Heat:
${result.burst.heat}

📈 RSI:
${result.rsi !== null
    ? result.rsi.toFixed(1)
    : "—"}

📐 ATR:
${result.atr !== null
    ? formatPrice(
        result.atr,
        decimals
      )
    : "—"}

⚖️ Risk/Reward: 1:2`;

  await sendTelegramMessage(
    message
  );
}

/* =========================================================
   ANALYZE ONE SYMBOL
========================================================= */

async function analyzeSymbol(
  symbol
) {
  try {
    /*
      H1
    */
    const h1Raw =
      await requestCandles(
        symbol,
        H1_GRANULARITY
      );

    const h1 =
      cleanCandles(h1Raw);

    await sleep(250);

    /*
      15M
    */
    const m15Raw =
      await requestCandles(
        symbol,
        M15_GRANULARITY
      );

    const m15 =
      cleanCandles(m15Raw);

    await sleep(250);

    /*
      5M
    */
    const m5Raw =
      await requestCandles(
        symbol,
        M5_GRANULARITY
      );

    const m5 =
      cleanCandles(m5Raw);

    if (
      h1.length < 25 ||
      m15.length < 25 ||
      m5.length < 25
    ) {
      throw new Error(
        "Not enough candle data"
      );
    }

    /* =====================================================
       DIRECTIONS
    ===================================================== */

    const h1Direction =
      getDirection(h1);

    const m15Direction =
      getDirection(m15);

    const m5Direction =
      getDirection(m5);

    /* =====================================================
       STRUCTURE
    ===================================================== */

    const m15Structure =
      getStructure(m15);

    const m5Structure =
      getStructure(m5);

    /* =====================================================
       LIQUIDITY
    ===================================================== */

    const liquidity =
      getLiquiditySweep(m5);

    /* =====================================================
       BURST FADER
    ===================================================== */

    const burst =
      getStrongBurstFader(m5);

    const burstSupport =
      getBurstSupport(
        burst,
        liquidity,
        m5Structure
      );

    /* =====================================================
       INDICATORS
    ===================================================== */

    const closes =
      m5.map((c) => c.close);

    const rsiValue =
      rsi(closes, 14);

    const atrValue =
      atr(m5, 14);

    /* =====================================================
       SCORING
    ===================================================== */

    let buyScore = 0;
    let sellScore = 0;

    /*
      1. H1 direction
    */

    if (
      h1Direction === "BULLISH"
    ) {
      buyScore++;
    }

    if (
      h1Direction === "BEARISH"
    ) {
      sellScore++;
    }

    /*
      2. 15M direction
    */

    if (
      m15Direction === "BULLISH"
    ) {
      buyScore++;
    }

    if (
      m15Direction === "BEARISH"
    ) {
      sellScore++;
    }

    /*
      3. 15M structure
    */

    if (
      m15Structure.startsWith(
        "BULLISH"
      )
    ) {
      buyScore++;
    }

    if (
      m15Structure.startsWith(
        "BEARISH"
      )
    ) {
      sellScore++;
    }

    /*
      4. 5M structure
    */

    if (
      m5Structure.startsWith(
        "BULLISH"
      )
    ) {
      buyScore++;
    }

    if (
      m5Structure.startsWith(
        "BEARISH"
      )
    ) {
      sellScore++;
    }

    /*
      5. Burst support
    */

    if (
      burstSupport.supportsBuy
    ) {
      buyScore++;
    }

    if (
      burstSupport.supportsSell
    ) {
      sellScore++;
    }

    /*
      6. Liquidity
    */

    if (
      liquidity === "BULLISH"
    ) {
      buyScore++;
    }

    if (
      liquidity === "BEARISH"
    ) {
      sellScore++;
    }

    /*
      Keep dashboard score at 5 maximum.
    */
    buyScore =
      Math.min(5, buyScore);

    sellScore =
      Math.min(5, sellScore);

    /* =====================================================
       SIGNAL
    ===================================================== */

    let signal = "WAIT";

    /*
      BUY

      Strong Burst Fader is SUPPORTING,
      not mandatory.
    */
    if (
      h1Direction === "BULLISH" &&
      m15Direction === "BULLISH" &&
      (
        m15Structure.startsWith(
          "BULLISH"
        ) ||
        m5Structure.startsWith(
          "BULLISH"
        ) ||
        burstSupport.supportsBuy
      ) &&
      buyScore >= 3
    ) {
      signal = "BUY";
    }

    /*
      SELL

      Strong Burst Fader is SUPPORTING,
      not mandatory.
    */
    else if (
      h1Direction === "BEARISH" &&
      m15Direction === "BEARISH" &&
      (
        m15Structure.startsWith(
          "BEARISH"
        ) ||
        m5Structure.startsWith(
          "BEARISH"
        ) ||
        burstSupport.supportsSell
      ) &&
      sellScore >= 3
    ) {
      signal = "SELL";
    }

    /* =====================================================
       ENTRY / SL / TP
    ===================================================== */

    const lastM5 =
      m5[m5.length - 1];

    const entry =
      lastM5.close;

    let stopLoss = null;
    let takeProfit = null;

    const decimals =
      getDecimals(symbol);

    if (
      signal === "BUY" &&
      Number.isFinite(atrValue)
    ) {
      const recent =
        m5.slice(-8);

      const lowest =
        Math.min(
          ...recent.map(
            (c) => c.low
          )
        );

      stopLoss =
        lowest -
        0.2 * atrValue;

      const risk =
        entry - stopLoss;

      takeProfit =
        entry + 2 * risk;
    }

    if (
      signal === "SELL" &&
      Number.isFinite(atrValue)
    ) {
      const recent =
        m5.slice(-8);

      const highest =
        Math.max(
          ...recent.map(
            (c) => c.high
          )
        );

      stopLoss =
        highest +
        0.2 * atrValue;

      const risk =
        stopLoss - entry;

      takeProfit =
        entry - 2 * risk;
    }

    return {
      symbol,

      signal,

      score:
        signal === "BUY"
          ? buyScore
          : signal === "SELL"
            ? sellScore
            : Math.max(
                buyScore,
                sellScore
              ),

      buyScore,
      sellScore,

      entry:
        Number.isFinite(entry)
          ? round(
              entry,
              decimals
            )
          : null,

      stopLoss:
        Number.isFinite(stopLoss)
          ? round(
              stopLoss,
              decimals
            )
          : null,

      takeProfit:
        Number.isFinite(
          takeProfit
        )
          ? round(
              takeProfit,
              decimals
            )
          : null,

      h1Direction,
      m15Direction,
      m5Direction,

      m15Structure,
      m5Structure,

      liquidity,

      burst,

      burstSupport,

      rsi:
        Number.isFinite(rsiValue)
          ? round(rsiValue, 2)
          : null,

      atr:
        Number.isFinite(atrValue)
          ? round(
              atrValue,
              decimals
            )
          : null,

      updated:
        new Date().toISOString()
    };
  } catch (error) {
    console.log(
      `Analysis failed for ${symbol}: ${error.message}`
    );

    return {
      symbol,
      signal: "WAIT",
      score: 0,

      buyScore: 0,
      sellScore: 0,

      entry: null,
      stopLoss: null,
      takeProfit: null,

      h1Direction: "ERROR",
      m15Direction: "ERROR",
      m5Direction: "ERROR",

      m15Structure: "ERROR",
      m5Structure: "ERROR",

      liquidity: "NONE",

      burst: {
        direction: "NONE",
        stack: 0,
        heat: "COOL",
        fadeBias: "NONE"
      },

      burstSupport: {
        supportsBuy: false,
        supportsSell: false
      },

      rsi: null,
      atr: null,

      error: error.message,

      updated:
        new Date().toISOString()
    };
  }
}

/* =========================================================
   SCAN ALL MARKETS
========================================================= */

async function scanMarkets() {
  /*
    Prevent overlapping scans.

    If one scan is still running when the next
    interval arrives, the new scan is skipped.
  */
  if (scanInProgress) {
    console.log(
      "Previous scan still running. Skipping this cycle."
    );

    return;
  }

  scanInProgress = true;

  try {
    state.lastScan =
      new Date().toISOString();

    state.error = null;

    const markets =
      await getActiveSymbols();

    state.markets = [];

    /*
      Sequential scanning is intentional.
      It prevents a burst of requests.
    */
    for (const market of markets) {
      const result =
        await analyzeSymbol(
          market.symbol
        );

      state.markets.push({
        ...market,
        ...result
      });

      /*
        Burst warning.
      */
      if (
        result.burst &&
        shouldSendBurstWarning(
          market.symbol,
          result.burst
        )
      ) {
        const price =
          result.entry;

        await sendBurstWarning(
          market.symbol,
          result.burst,
          price
        );
      }

      /*
        Normal BUY / SELL alert.
      */
      if (
        result.signal === "BUY" ||
        result.signal === "SELL"
      ) {
        await sendTelegramAlert(
          result
        );
      }

      /*
        Extra breathing room between symbols.
      */
      await sleep(400);
    }

    state.lastScan =
      new Date().toISOString();

    console.log(
      `Scan complete: ${state.markets.length} markets`
    );
  } catch (error) {
    state.error = error.message;

    console.log(
      "Scan error:",
      error.message
    );
  } finally {
    scanInProgress = false;
  }
}

/* =========================================================
   DASHBOARD
========================================================= */

app.get("/", (req, res) => {
  res.sendFile(
    path.join(
      __dirname,
      "public",
      "index.html"
    )
  );
});

app.get("/health", (req, res) => {
  res.json({
    ok: true,
    online: state.online,
    derivConnected:
      state.derivConnected,
    lastScan: state.lastScan,
    error: state.error
  });
});

app.get("/api/status", (req, res) => {
  res.json({
    ok: true,
    online: state.online,

    derivConnected:
      state.derivConnected,

    lastScan:
      state.lastScan,

    error:
      state.error,

    markets:
      state.markets,

    marketCount:
      state.markets.length,

    scannerRunning:
      scanInProgress
  });
});

/* =========================================================
   START SERVER
========================================================= */

app.listen(
  PORT,
  "0.0.0.0",
  () => {
    console.log(
      `Deriv Volatility Burst Fader running on port ${PORT}`
    );

    console.log(
      "Using persistent Deriv WebSocket connection"
    );

    console.log(
      `Candle count: ${CANDLE_COUNT}`
    );

    console.log(
      `Scan interval: ${SCAN_INTERVAL_MS / 1000}s`
    );

    /*
      Start first scan after a short delay.
    */
    setTimeout(() => {
      scanMarkets().catch(
        (error) => {
          console.log(
            "Initial scan failed:",
            error.message
          );
        }
      );
    }, 3000);

    /*
      Continue scanning.
    */
    setInterval(() => {
      scanMarkets().catch(
        (error) => {
          console.log(
            "Scheduled scan failed:",
            error.message
          );
        }
      );
    }, SCAN_INTERVAL_MS);
  }
);

/* =========================================================
   CLEAN SHUTDOWN
========================================================= */

process.on(
  "SIGTERM",
  () => {
    console.log(
      "SIGTERM received. Closing..."
    );

    if (derivWs) {
      try {
        derivWs.close();
      } catch (_) {}
    }

    process.exit(0);
  }
);

process.on(
  "SIGINT",
  () => {
    console.log(
      "SIGINT received. Closing..."
    );

    if (derivWs) {
      try {
        derivWs.close();
      } catch (_) {}
    }

    process.exit(0);
  }
);
