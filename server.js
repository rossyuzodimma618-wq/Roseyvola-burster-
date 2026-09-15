const express = require("express");
const path = require("path");
const WebSocket = require("ws");

const app = express();

app.use(express.json());
app.use(express.static(path.join(__dirname, "public")));

const PORT = process.env.PORT || 3000;

const DERIV_WS_URL =
  "wss://api.derivws.com/trading/v1/options/ws/public";

const TELEGRAM_BOT_TOKEN = process.env.TELEGRAM_BOT_TOKEN;
const TELEGRAM_CHAT_ID = process.env.TELEGRAM_CHAT_ID;

/*
=========================================================
SETTINGS
=========================================================
*/

const H1 = 3600;
const M15 = 900;
const M5 = 300;

const CANDLE_COUNT = 120;

const SCAN_INTERVAL_MS = 60000;
const REQUEST_TIMEOUT_MS = 15000;

const REQUEST_DELAY_MS = 400;
const ACTIVE_SYMBOL_CACHE_MS = 10 * 60 * 1000;

const ALERT_COOLDOWN_MS = 15 * 60 * 1000;

/*
=========================================================
STATE
=========================================================
*/

const state = {
  online: true,
  derivConnected: false,
  markets: [],
  lastScan: null,
  error: null
};

const alertState = new Map();
const burstWarningState = new Map();

let ws = null;
let wsReady = false;
let requestId = 1;

const pendingRequests = new Map();

let requestQueue = Promise.resolve();

let cachedSymbols = [];
let cachedSymbolsAt = 0;

/*
=========================================================
HELPERS
=========================================================
*/

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function round(value, decimals = 2) {
  if (!Number.isFinite(value)) return null;

  const factor = Math.pow(10, decimals);

  return Math.round(value * factor) / factor;
}

function formatPrice(value, decimals = 2) {
  if (!Number.isFinite(value)) return "N/A";

  return Number(value).toFixed(decimals);
}

/*
=========================================================
DERIV WEBSOCKET
=========================================================
*/

function connectDeriv() {
  return new Promise((resolve, reject) => {
    if (ws && wsReady) {
      resolve();
      return;
    }

    console.log("Connecting to Deriv...");

    ws = new WebSocket(DERIV_WS_URL);

    let settled = false;

    const connectionTimeout = setTimeout(() => {
      if (!settled) {
        settled = true;
        reject(new Error("Deriv WebSocket connection timeout"));
      }
    }, 15000);

    ws.on("open", () => {
      wsReady = true;
      state.derivConnected = true;
      state.error = null;

      console.log("Deriv WebSocket connected");

      if (!settled) {
        settled = true;
        clearTimeout(connectionTimeout);
        resolve();
      }
    });

    ws.on("message", raw => {
      let data;

      try {
        data = JSON.parse(raw.toString());
      } catch {
        return;
      }

      const reqId = data.req_id;

      if (reqId && pendingRequests.has(reqId)) {
        const pending = pendingRequests.get(reqId);

        pendingRequests.delete(reqId);

        if (data.error) {
          pending.reject(
            new Error(
              data.error.message ||
              data.error.code ||
              "Deriv request failed"
            )
          );
        } else {
          pending.resolve(data);
        }
      }
    });

    ws.on("error", err => {
      console.log(
        "Deriv WebSocket error:",
        err.message
      );

      state.derivConnected = false;
      state.error = err.message;

      if (!settled) {
        settled = true;
        clearTimeout(connectionTimeout);
        reject(err);
      }
    });

    ws.on("close", () => {
      wsReady = false;
      state.derivConnected = false;

      console.log("Deriv WebSocket disconnected");

      for (const [id, pending] of pendingRequests) {
        pending.reject(
          new Error("Deriv WebSocket closed")
        );

        pendingRequests.delete(id);
      }

      setTimeout(() => {
        if (!wsReady) {
          connectDeriv().catch(() => {});
        }
      }, 5000);
    });
  });
}

/*
=========================================================
QUEUED DERIV REQUEST
=========================================================
*/

function derivRequest(payload) {
  requestQueue = requestQueue.then(async () => {
    await sleep(REQUEST_DELAY_MS);

    return performDerivRequest(payload);
  });

  return requestQueue;
}

async function performDerivRequest(payload) {
  let attempts = 0;

  while (attempts < 3) {
    attempts++;

    try {
      await connectDeriv();

      const reqId = requestId++;

      const request = {
        ...payload,
        req_id: reqId
      };

      return await new Promise((resolve, reject) => {
        const timeout = setTimeout(() => {
          pendingRequests.delete(reqId);

          reject(
            new Error("Deriv request timeout")
          );
        }, REQUEST_TIMEOUT_MS);

        pendingRequests.set(reqId, {
          resolve: data => {
            clearTimeout(timeout);
            resolve(data);
          },

          reject: error => {
            clearTimeout(timeout);
            reject(error);
          }
        });

        try {
          ws.send(JSON.stringify(request));
        } catch (err) {
          clearTimeout(timeout);
          pendingRequests.delete(reqId);
          reject(err);
        }
      });
    } catch (error) {
      console.log(
        `Deriv request attempt ${attempts} failed:`,
        error.message
      );

      if (attempts < 3) {
        await sleep(1500 * attempts);
      } else {
        throw error;
      }
    }
  }

  throw new Error("Deriv request failed");
}

/*
=========================================================
ACTIVE SYMBOLS
=========================================================
*/

async function getVolatilitySymbols() {
  const now = Date.now();

  if (
    cachedSymbols.length > 0 &&
    now - cachedSymbolsAt < ACTIVE_SYMBOL_CACHE_MS
  ) {
    return cachedSymbols;
  }

  const data = await derivRequest({
    active_symbols: "brief"
  });

  const symbols =
    data &&
    Array.isArray(data.active_symbols)
      ? data.active_symbols
      : [];

  const volatility = symbols.filter(item => {
    const symbol =
      item.symbol ||
      item.underlying_symbol ||
      "";

    const display =
      item.display_name ||
      item.underlying_symbol_name ||
      "";

    return (
      /volatility/i.test(display) ||
      /volatility/i.test(symbol) ||
      /^1HZ\d+V$/i.test(symbol) ||
      /^R_\d+$/i.test(symbol) ||
      /V\d+/i.test(symbol)
    );
  });

  cachedSymbols = volatility;
  cachedSymbolsAt = now;

  console.log(
    `Found ${volatility.length} Volatility indices`
  );

  return volatility;
}

/*
=========================================================
CANDLES
=========================================================
*/

async function requestCandles(symbol, granularity) {
  const data = await derivRequest({
    ticks_history: symbol,
    end: "latest",
    count: CANDLE_COUNT,
    style: "candles",
    granularity
  });

  /*
  Some Deriv responses return normal candles.
  */

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

  /*
  Some API responses return prices + times.
  Convert them into simple OHLC candles.
  */

  if (
    data &&
    data.history &&
    Array.isArray(data.history.prices) &&
    Array.isArray(data.history.times)
  ) {
    const prices = data.history.prices;
    const times = data.history.times;

    const candles = [];

    for (let i = 0; i < prices.length; i++) {
      const price = Number(prices[i]);
      const time = Number(times[i]);

      if (
        !Number.isFinite(price) ||
        !Number.isFinite(time)
      ) {
        continue;
      }

      candles.push({
        epoch: time,
        open: price,
        high: price,
        low: price,
        close: price
      });
    }

    return candles;
  }

  throw new Error(
    `No candles returned for ${symbol}`
  );
}

/*
=========================================================
CLEAN CANDLES
=========================================================
*/

function cleanCandles(raw) {
  if (!Array.isArray(raw)) return [];

  const candles = raw
    .map(c => ({
      epoch: Number(
        c.epoch ||
        c.time ||
        c.open_time
      ),

      open: Number(c.open),
      high: Number(c.high),
      low: Number(c.low),
      close: Number(c.close)
    }))
    .filter(c =>
      Number.isFinite(c.epoch) &&
      Number.isFinite(c.open) &&
      Number.isFinite(c.high) &&
      Number.isFinite(c.low) &&
      Number.isFinite(c.close)
    )
    .sort((a, b) => a.epoch - b.epoch);

  /*
  Remove duplicate candles.
  */

  const unique = [];

  for (const candle of candles) {
    if (
      unique.length === 0 ||
      unique[unique.length - 1].epoch !== candle.epoch
    ) {
      unique.push(candle);
    }
  }

  return unique.slice(-CANDLE_COUNT);
}

/*
=========================================================
EMA
=========================================================
*/

function ema(values, period = 20) {
  if (!values || values.length < period) {
    return null;
  }

  const multiplier = 2 / (period + 1);

  let result = 0;

  for (let i = 0; i < period; i++) {
    result += values[i];
  }

  result /= period;

  for (let i = period; i < values.length; i++) {
    result =
      (values[i] - result) * multiplier +
      result;
  }

  return result;
}

/*
=========================================================
ATR
=========================================================
*/

function atr(candles, period = 14) {
  if (candles.length < period + 1) {
    return null;
  }

  const trs = [];

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

    trs.push(tr);
  }

  if (trs.length < period) {
    return null;
  }

  let value = 0;

  for (let i = 0; i < period; i++) {
    value += trs[i];
  }

  value /= period;

  for (let i = period; i < trs.length; i++) {
    value =
      ((value * (period - 1)) + trs[i]) /
      period;
  }

  return value;
}

/*
=========================================================
RSI
=========================================================
*/

function rsi(candles, period = 14) {
  if (candles.length < period + 1) {
    return null;
  }

  let gain = 0;
  let loss = 0;

  for (let i = 1; i <= period; i++) {
    const change =
      candles[i].close -
      candles[i - 1].close;

    if (change >= 0) {
      gain += change;
    } else {
      loss += Math.abs(change);
    }
  }

  let avgGain = gain / period;
  let avgLoss = loss / period;

  for (
    let i = period + 1;
    i < candles.length;
    i++
  ) {
    const change =
      candles[i].close -
      candles[i - 1].close;

    const currentGain =
      change > 0 ? change : 0;

    const currentLoss =
      change < 0 ? Math.abs(change) : 0;

    avgGain =
      ((avgGain * (period - 1)) +
        currentGain) /
      period;

    avgLoss =
      ((avgLoss * (period - 1)) +
        currentLoss) /
      period;
  }

  if (avgLoss === 0) {
    return 100;
  }

  const rs = avgGain / avgLoss;

  return 100 - 100 / (1 + rs);
}

/*
=========================================================
DIRECTION
=========================================================
*/

function getDirection(candles) {
  if (candles.length < 25) {
    return "NEUTRAL";
  }

  const closes = candles.map(c => c.close);

  const e20 = ema(closes, 20);

  const last = candles[candles.length - 1];

  const previous =
    candles[candles.length - 6];

  if (!Number.isFinite(e20)) {
    return "NEUTRAL";
  }

  if (
    last.close > e20 &&
    last.close > previous.close
  ) {
    return "BULLISH";
  }

  if (
    last.close < e20 &&
    last.close < previous.close
  ) {
    return "BEARISH";
  }

  return "NEUTRAL";
}

/*
=========================================================
STRUCTURE / BOS / CHOCH
=========================================================
*/

function getStructure(candles) {
  if (candles.length < 15) {
    return "NEUTRAL";
  }

  const last =
    candles[candles.length - 1];

  const previous =
    candles.slice(
      Math.max(0, candles.length - 12),
      candles.length - 2
    );

  const highest = Math.max(
    ...previous.map(c => c.high)
  );

  const lowest = Math.min(
    ...previous.map(c => c.low)
  );

  if (last.close > highest) {
    return "BULLISH BOS";
  }

  if (last.close < lowest) {
    return "BEARISH BOS";
  }

  const a =
    candles[candles.length - 2];

  const b =
    candles[candles.length - 3];

  if (
    last.close > last.open &&
    a.close > a.open &&
    b.close > b.open
  ) {
    return "BULLISH CHOCH";
  }

  if (
    last.close < last.open &&
    a.close < a.open &&
    b.close < b.open
  ) {
    return "BEARISH CHOCH";
  }

  return "NEUTRAL";
}

/*
=========================================================
LIQUIDITY SWEEP
=========================================================
*/

function getLiquiditySweep(candles) {
  if (candles.length < 12) {
    return "NONE";
  }

  const last =
    candles[candles.length - 1];

  const previous =
    candles.slice(
      candles.length - 11,
      candles.length - 1
    );

  const previousLow = Math.min(
    ...previous.map(c => c.low)
  );

  const previousHigh = Math.max(
    ...previous.map(c => c.high)
  );

  /*
  Bullish liquidity sweep:
  price takes previous low then closes back above.
  */

  if (
    last.low < previousLow &&
    last.close > previousLow
  ) {
    return "BULLISH SWEEP";
  }

  /*
  Bearish liquidity sweep:
  price takes previous high then closes back below.
  */

  if (
    last.high > previousHigh &&
    last.close < previousHigh
  ) {
    return "BEARISH SWEEP";
  }

  return "NONE";
}

/*
=========================================================
STRONG BURST FADER
=========================================================
*/

function getBurstFader(candles) {
  if (candles.length < 25) {
    return {
      direction: "NONE",
      stack: 0,
      heat: "COOL",
      fadeBias: "NONE"
    };
  }

  const closes =
    candles.map(c => c.close);

  const basis =
    ema(closes, 20);

  const currentAtr =
    atr(candles, 14);

  const last =
    candles[candles.length - 1];

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

  const upper =
    basis + currentAtr * 2;

  const lower =
    basis - currentAtr * 2;

  let direction = "NONE";
  let stack = 0;

  /*
  UP BURST
  */

  if (last.high > upper) {
    direction = "UP BURST";

    stack = Math.ceil(
      (last.high - upper) /
        (currentAtr * 0.5)
    );
  }

  /*
  DOWN BURST
  */

  else if (last.low < lower) {
    direction = "DOWN BURST";

    stack = Math.ceil(
      (lower - last.low) /
        (currentAtr * 0.5)
    );
  }

  stack = Math.max(
    0,
    Math.min(6, stack)
  );

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

/*
=========================================================
BURST SUPPORT
=========================================================
*/

function getBurstSupport(
  burst,
  liquidity,
  m5Structure
) {
  let supportsBuy = false;
  let supportsSell = false;

  if (
    burst.direction === "DOWN BURST" &&
    (
      liquidity === "BULLISH SWEEP" ||
      m5Structure.startsWith("BULLISH")
    )
  ) {
    supportsBuy = true;
  }

  if (
    burst.direction === "UP BURST" &&
    (
      liquidity === "BEARISH SWEEP" ||
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

/*
=========================================================
BURST WARNING
=========================================================
*/

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

  if (!stage) {
    burstWarningState.delete(symbol);
    return false;
  }

  const previous =
    burstWarningState.get(symbol);

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

/*
=========================================================
TELEGRAM
=========================================================
*/

async function sendTelegram(message) {
  if (
    !TELEGRAM_BOT_TOKEN ||
    !TELEGRAM_CHAT_ID
  ) {
    console.log(
      "Telegram not configured - skipping Telegram message"
    );

    return false;
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
          chat_id: TELEGRAM_CHAT_ID,
          text: message
        })
      });

    if (!response.ok) {
      const text =
        await response.text();

      console.log(
        "Telegram error:",
        text
      );

      return false;
    }

    return true;
  } catch (error) {
    console.log(
      "Telegram request failed:",
      error.message
    );

    return false;
  }
}

/*
=========================================================
BURST WARNING TELEGRAM
=========================================================
*/

async function sendBurstWarning(
  symbol,
  burst,
  price,
  decimals
) {
  const direction =
    burst.direction === "UP BURST"
      ? "UPSIDE BURST"
      : "DOWNSIDE BURST";

  const possibleReversal =
    burst.fadeBias === "BEARISH"
      ? "Possible BEARISH reversal"
      : "Possible BULLISH reversal";

  const message =
`⚠️ STRONG BURST WARNING

📊 ${symbol}

💥 ${direction}
🔥 Heat: ${burst.heat}
📶 Stack: ${burst.stack}/6

${possibleReversal}

💰 Price: ${formatPrice(
    price,
    decimals
  )}

⏳ WAIT FOR CONFIRMATION

This is a warning, NOT an entry signal.`;

  await sendTelegram(message);
}

/*
=========================================================
TRADE ALERT
=========================================================
*/

async function sendTradeAlert(
  symbol,
  signal,
  entry,
  sl,
  tp,
  score,
  h1Direction,
  m15Direction,
  m5Structure,
  burst,
  rsiValue,
  decimals
) {
  const message =
`${signal === "BUY" ? "🟢" : "🔴"} STRONG ${signal}: ${symbol}

📍 Entry: ${formatPrice(entry, decimals)}
🛑 Stop Loss: ${formatPrice(sl, decimals)}
🎯 Take Profit: ${formatPrice(tp, decimals)}

⭐ Score: ${score}/5

📊 1H: ${h1Direction}
📊 15M: ${m15Direction}
📊 5M: ${m5Structure}

💥 Burst: ${burst.direction}
🔥 Heat: ${burst.heat}
📶 Stack: ${burst.stack}/6

RSI: ${
    Number.isFinite(rsiValue)
      ? rsiValue.toFixed(1)
      : "N/A"
  }

⚠️ Scanner alert only.
Manage risk carefully.`;

  await sendTelegram(message);
}

/*
=========================================================
ALERT COOLDOWN
=========================================================
*/

function canSendTradeAlert(symbol) {
  const now = Date.now();

  const last =
    alertState.get(symbol) || 0;

  if (
    now - last <
    ALERT_COOLDOWN_MS
  ) {
    return false;
  }

  alertState.set(symbol, now);

  return true;
}

/*
=========================================================
DECIMALS
=========================================================
*/

function getDecimals(symbol) {
  if (/JPY/i.test(symbol)) {
    return 3;
  }

  return 2;
}

/*
=========================================================
ANALYZE ONE SYMBOL
=========================================================
*/

async function analyzeSymbol(
  item
) {
  const symbol =
    item.symbol ||
    item.underlying_symbol;

  const displayName =
    item.display_name ||
    item.underlying_symbol_name ||
    symbol;

  if (!symbol) {
    return null;
  }

  const decimals =
    getDecimals(symbol);

  console.log("");
  console.log(
    "========================================"
  );
  console.log(
    `ANALYZING: ${displayName} (${symbol})`
  );
  console.log(
    "========================================"
  );

  try {
    /*
    -----------------------------------------
    H1
    -----------------------------------------
    */

    const h1Raw =
      await requestCandles(
        symbol,
        H1
      );

    const h1 =
      cleanCandles(h1Raw);

    /*
    -----------------------------------------
    M15
    -----------------------------------------
    */

    const m15Raw =
      await requestCandles(
        symbol,
        M15
      );

    const m15 =
      cleanCandles(m15Raw);

    /*
    -----------------------------------------
    M5
    -----------------------------------------
    */

    const m5Raw =
      await requestCandles(
        symbol,
        M5
      );

    const m5 =
      cleanCandles(m5Raw);

    if (
      h1.length < 30 ||
      m15.length < 30 ||
      m5.length < 30
    ) {
      console.log(
        "Not enough candle data"
      );

      return null;
    }

    /*
    -----------------------------------------
    ANALYSIS
    -----------------------------------------
    */

    const h1Direction =
      getDirection(h1);

    const m15Direction =
      getDirection(m15);

    const m5Direction =
      getDirection(m5);

    const m15Structure =
      getStructure(m15);

    const m5Structure =
      getStructure(m5);

    const liquidity =
      getLiquiditySweep(m5);

    const burst =
      getBurstFader(m5);

    const burstSupport =
      getBurstSupport(
        burst,
        liquidity,
        m5Structure
      );

    const rsiValue =
      rsi(m5);

    const entry =
      m5[m5.length - 1].close;

    /*
    -----------------------------------------
    SCORE
    -----------------------------------------
    */

    let score = 0;

    if (
      h1Direction === "BULLISH" ||
      h1Direction === "BEARISH"
    ) {
      score++;
    }

    if (
      m15Direction === h1Direction
    ) {
      score++;
    }

    if (
      (
        h1Direction === "BULLISH" &&
        m15Structure.startsWith(
          "BULLISH"
        )
      ) ||
      (
        h1Direction === "BEARISH" &&
        m15Structure.startsWith(
          "BEARISH"
        )
      )
    ) {
      score++;
    }

    if (
      (
        h1Direction === "BULLISH" &&
        m5Structure.startsWith(
          "BULLISH"
        )
      ) ||
      (
        h1Direction === "BEARISH" &&
        m5Structure.startsWith(
          "BEARISH"
        )
      )
    ) {
      score++;
    }

    if (
      liquidity === "BULLISH SWEEP" ||
      liquidity === "BEARISH SWEEP"
    ) {
      score++;
    }

    if (
      burstSupport.supportsBuy ||
      burstSupport.supportsSell
    ) {
      score++;
    }

    score = Math.min(5, score);

    /*
    -----------------------------------------
    SIGNAL
    -----------------------------------------
    */

    let signal = "WAIT";

    const buySetup =
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
      score >= 3;

    const sellSetup =
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
      score >= 3;

    if (buySetup) {
      signal = "BUY";
    }

    if (sellSetup) {
      signal = "SELL";
    }

    /*
    -----------------------------------------
    STOP / TARGET
    -----------------------------------------
    */

    const recentM5 =
      m5.slice(-8);

    const currentAtr =
      atr(m5, 14) || 0;

    let sl = null;
    let tp = null;

    if (signal === "BUY") {
      const lowest =
        Math.min(
          ...recentM5.map(
            c => c.low
          )
        );

      sl =
        lowest -
        currentAtr * 0.2;

      const risk =
        entry - sl;

      tp =
        entry +
        risk * 2;
    }

    if (signal === "SELL") {
      const highest =
        Math.max(
          ...recentM5.map(
            c => c.high
          )
        );

      sl =
        highest +
        currentAtr * 0.2;

      const risk =
        sl - entry;

      tp =
        entry -
        risk * 2;
    }

    /*
    -----------------------------------------
    RAILWAY DETAILED LOG
    -----------------------------------------
    */

    console.log("");
    console.log(
      `📊 ${displayName}`
    );

    console.log(
      `H1: ${h1Direction}`
    );

    console.log(
      `15M: ${m15Direction}`
    );

    console.log(
      `5M: ${m5Direction}`
    );

    console.log(
      `15M Structure: ${m15Structure}`
    );

    console.log(
      `5M Structure: ${m5Structure}`
    );

    console.log(
      `Liquidity: ${liquidity}`
    );

    console.log(
      `Burst Fader: ${burst.direction}`
    );

    console.log(
      `Burst Heat: ${burst.heat}`
    );

    console.log(
      `Burst Stack: ${burst.stack}/6`
    );

    console.log(
      `Burst Fade Bias: ${burst.fadeBias}`
    );

    console.log(
      `RSI: ${
        Number.isFinite(rsiValue)
          ? rsiValue.toFixed(1)
          : "N/A"
      }`
    );

    console.log(
      `Score: ${score}/5`
    );

    console.log(
      `Entry: ${formatPrice(
        entry,
        decimals
      )}`
    );

    console.log(
      `SIGNAL: ${signal}`
    );

    if (signal === "BUY" || signal === "SELL") {
      console.log(
        `SL: ${formatPrice(
          sl,
          decimals
        )}`
      );

      console.log(
        `TP: ${formatPrice(
          tp,
          decimals
        )}`
      );
    }

    console.log(
      "========================================"
    );

    /*
    -----------------------------------------
    BURST WARNING
    -----------------------------------------
    */

    if (
      shouldSendBurstWarning(
        symbol,
        burst
      )
    ) {
      await sendBurstWarning(
        symbol,
        burst,
        entry,
        decimals
      );
    }

    /*
    -----------------------------------------
    TRADE ALERT
    -----------------------------------------
    */

    if (
      (signal === "BUY" ||
        signal === "SELL") &&
      canSendTradeAlert(symbol)
    ) {
      await sendTradeAlert(
        symbol,
        signal,
        entry,
        sl,
        tp,
        score,
        h1Direction,
        m15Direction,
        m5Structure,
        burst,
        rsiValue,
        decimals
      );
    }

    return {
      symbol,
      name: displayName,

      price: entry,

      signal,
      score,

      h1: h1Direction,
      m15: m15Direction,
      m5: m5Direction,

      m15Structure,
      m5Structure,

      liquidity,

      burst: burst.direction,
      burstHeat: burst.heat,
      burstStack: burst.stack,
      burstFadeBias: burst.fadeBias,

      rsi: Number.isFinite(rsiValue)
        ? round(rsiValue, 1)
        : null,

      entry,

      sl,
      tp,

      updatedAt:
        new Date().toISOString()
    };
  } catch (error) {
    console.log(
      `Analysis failed for ${symbol}:`,
      error.message
    );

    return {
      symbol,
      name: displayName,
      signal: "ERROR",
      score: 0,
      error: error.message
    };
  }
}

/*
=========================================================
SCAN ALL MARKETS
=========================================================
*/

async function scanMarkets() {
  const scanStarted =
    new Date();

  console.log("");
  console.log(
    "################################################"
  );
  console.log(
    `SCAN START: ${scanStarted.toISOString()}`
  );
  console.log(
    "################################################"
  );

  state.lastScan =
    scanStarted.toISOString();

  try {
    const symbols =
      await getVolatilitySymbols();

    console.log(
      `Markets available: ${symbols.length}`
    );

    const results = [];

    /*
    Analyze sequentially.
    This helps reduce API pressure.
    */

    for (const item of symbols) {
      const result =
        await analyzeSymbol(item);

      if (result) {
        results.push(result);
      }

      await sleep(500);
    }

    state.markets = results;
    state.error = null;

    const buyCount =
      results.filter(
        r => r.signal === "BUY"
      ).length;

    const sellCount =
      results.filter(
        r => r.signal === "SELL"
      ).length;

    const waitCount =
      results.filter(
        r => r.signal === "WAIT"
      ).length;

    console.log("");
    console.log(
      "=============== SCAN SUMMARY ==============="
    );

    console.log(
      `Markets scanned: ${results.length}`
    );

    console.log(
      `BUY signals: ${buyCount}`
    );

    console.log(
      `SELL signals: ${sellCount}`
    );

    console.log(
      `WAIT: ${waitCount}`
    );

    console.log(
      `Scan completed: ${new Date().toISOString()}`
    );

    console.log(
      "============================================="
    );

  } catch (error) {
    state.error =
      error.message;

    console.log(
      "SCAN ERROR:",
      error.message
    );
  }
}

/*
=========================================================
DASHBOARD
=========================================================
*/

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
    online: true,
    derivConnected:
      state.derivConnected,
    lastScan:
      state.lastScan,
    error:
      state.error
  });
});

app.get("/api/status", (req, res) => {
  res.json({
    ok: true,
    online:
      state.online,

    derivConnected:
      state.derivConnected,

    marketCount:
      state.markets.length,

    lastScan:
      state.lastScan,

    error:
      state.error,

    markets:
      state.markets
  });
});

/*
=========================================================
START SERVER
=========================================================
*/

app.listen(
  PORT,
  "0.0.0.0",
  () => {
    console.log("");
    console.log(
      "=============================================="
    );

    console.log(
      "Deriv Volatility Burst Fader running"
    );

    console.log(
      `Port: ${PORT}`
    );

    console.log(
      "Strategy: H1 → 15M → 5M"
    );

    console.log(
      "Strong Burst Fader: SUPPORTING SIGNAL"
    );

    console.log(
      "Telegram:",
      TELEGRAM_BOT_TOKEN &&
      TELEGRAM_CHAT_ID
        ? "CONFIGURED"
        : "NOT CONFIGURED"
    );

    console.log(
      "=============================================="
    );

    /*
    Connect first, then start scanner.
    */

    connectDeriv()
      .then(() => {
        console.log(
          "Initial Deriv connection successful"
        );

        scanMarkets();
      })
      .catch(error => {
        console.log(
          "Initial Deriv connection failed:",
          error.message
        );

        /*
        Still start scanner.
        It will retry connection.
        */

        scanMarkets();
      });

    setInterval(
      () => {
        scanMarkets();
      },
      SCAN_INTERVAL_MS
    );
  }
);
