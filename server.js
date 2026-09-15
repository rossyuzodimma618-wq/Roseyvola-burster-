const express = require("express");
const path = require("path");
const WebSocket = require("ws");

const app = express();
const PORT = process.env.PORT || 3000;

/*
=========================================================
DERIV VOLATILITY BURST FADER
---------------------------------------------------------
1H  = Overall direction
15M = Structure + liquidity
5M  = Entry + Strong Burst Fader

This is an ALERT/SCANNER bot.
It does NOT place trades automatically.
=========================================================
*/

// IMPORTANT:
// Do NOT add ?app_id=1089 here.
const DERIV_WS_URL =
  "wss://ws.binaryws.com/websockets/v3";

const TELEGRAM_BOT_TOKEN =
  process.env.TELEGRAM_BOT_TOKEN || "";

const TELEGRAM_CHAT_ID =
  process.env.TELEGRAM_CHAT_ID || "";

/*
=========================================================
EXPRESS / DASHBOARD
=========================================================
*/

app.use(express.json());

app.use(
  express.static(path.join(__dirname, "public"))
);

app.get("/", (req, res) => {
  res.sendFile(
    path.join(__dirname, "public", "index.html")
  );
});

app.get("/health", (req, res) => {
  res.json({
    ok: true,
    online: true,
    service: "Deriv Volatility Burst Fader",
    time: new Date().toISOString()
  });
});

/*
=========================================================
TIMEFRAMES
=========================================================
*/

const TIMEFRAMES = {
  H1: 3600,
  M15: 900,
  M5: 300
};

/*
=========================================================
SETTINGS
=========================================================
*/

const CANDLE_COUNT = 180;

const SCAN_INTERVAL_MS = 30000;

const REQUEST_DELAY_MS = 300;

const ALERT_COOLDOWN_MS = 15 * 60 * 1000;

const MAX_SYMBOLS = 100;

/*
=========================================================
STATE
=========================================================
*/

let derivWs = null;

let connected = false;

let connecting = false;

let reconnectTimer = null;

let scanTimer = null;

let scanning = false;

let symbols = [];

let lastScan = null;

let lastError = null;

let lastDerivMessage = null;

const scanResults = [];

const alertHistory = [];

const lastAlertTimes = new Map();

let requestCounter = 1000;

const pendingRequests = new Map();

/*
=========================================================
HELPERS
=========================================================
*/

function sleep(ms) {
  return new Promise(resolve => {
    setTimeout(resolve, ms);
  });
}

function nextReqId() {
  requestCounter += 1;
  return requestCounter;
}

function cleanNumber(value) {
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

function round(value, decimals = 5) {
  if (!Number.isFinite(Number(value))) {
    return null;
  }

  return Number(
    Number(value).toFixed(decimals)
  );
}

function symbolName(item) {
  return (
    item.underlying_symbol_name ||
    item.display_name ||
    item.symbol_display_name ||
    item.symbol ||
    item.underlying_symbol ||
    "Unknown"
  );
}

function symbolCode(item) {
  return (
    item.underlying_symbol ||
    item.symbol ||
    item.code ||
    ""
  );
}

function isVolatilitySymbol(item) {
  const code = symbolCode(item);
  const name = symbolName(item);

  const text =
    `${code} ${name}`.toLowerCase();

  return (
    text.includes("volatility") ||
    /[0-9]+v/.test(text) ||
    /hz[0-9]+v/.test(text) ||
    /1s/.test(text) && text.includes("volatility")
  );
}

function getDecimals(symbol) {
  const code = symbol.toUpperCase();

  if (
    code.includes("R_10") ||
    code.includes("R_25") ||
    code.includes("R_50") ||
    code.includes("R_75") ||
    code.includes("R_100")
  ) {
    return 2;
  }

  return 2;
}

/*
=========================================================
WEBSOCKET CONNECTION
=========================================================
*/

function connectDeriv() {
  if (connecting || connected) {
    return;
  }

  connecting = true;

  console.log("Connecting to Deriv...");

  lastError = null;

  try {
    derivWs = new WebSocket(DERIV_WS_URL);

    derivWs.on("open", () => {
      connected = true;
      connecting = false;

      console.log("Connected to Deriv.");

      requestActiveSymbols();
    });

    derivWs.on("message", raw => {
      handleDerivMessage(raw);
    });

    derivWs.on("error", error => {
      connected = false;
      connecting = false;

      lastError =
        error?.message ||
        "Deriv WebSocket error";

      console.error(
        "Deriv WebSocket error:",
        lastError
      );
    });

    /*
    This gives us the actual HTTP status/body if
    the WebSocket handshake fails.
    */
    derivWs.on(
      "unexpected-response",
      (request, response) => {
        connected = false;
        connecting = false;

        console.error(
          "Deriv handshake failed:",
          response.statusCode
        );

        let body = "";

        response.on("data", chunk => {
          body += chunk.toString();
        });

        response.on("end", () => {
          console.error(
            "Deriv handshake body:",
            body
          );

          lastError =
            `Deriv handshake failed: HTTP ${response.statusCode}` +
            (body ? ` - ${body}` : "");
        });
      }
    );

    derivWs.on("close", () => {
      connected = false;
      connecting = false;

      console.log(
        "Deriv connection closed."
      );

      rejectPendingRequests(
        "Deriv connection closed"
      );

      scheduleReconnect();
    });

  } catch (error) {
    connected = false;
    connecting = false;

    lastError =
      error?.message ||
      "Connection error";

    console.error(
      "Connection exception:",
      lastError
    );

    scheduleReconnect();
  }
}

/*
=========================================================
RECONNECT
=========================================================
*/

function scheduleReconnect() {
  if (reconnectTimer) {
    return;
  }

  reconnectTimer = setTimeout(() => {
    reconnectTimer = null;
    connectDeriv();
  }, 5000);
}

/*
=========================================================
PENDING REQUESTS
=========================================================
*/

function rejectPendingRequests(reason) {
  for (const [reqId, pending] of pendingRequests) {
    clearTimeout(pending.timeout);

    pending.reject(
      new Error(reason)
    );
  }

  pendingRequests.clear();
}

/*
=========================================================
SEND DERIV REQUEST
=========================================================
*/

function sendDerivRequest(payload, timeoutMs = 15000) {
  return new Promise((resolve, reject) => {

    if (
      !derivWs ||
      derivWs.readyState !== WebSocket.OPEN
    ) {
      reject(
        new Error(
          "Deriv WebSocket is not connected"
        )
      );

      return;
    }

    const reqId = nextReqId();

    const request = {
      ...payload,
      req_id: reqId
    };

    const timeout = setTimeout(() => {
      pendingRequests.delete(reqId);

      reject(
        new Error(
          `Deriv request timeout: ${payload.msg_type || Object.keys(payload)[0]}`
        )
      );
    }, timeoutMs);

    pendingRequests.set(reqId, {
      resolve,
      reject,
      timeout
    });

    try {
      derivWs.send(
        JSON.stringify(request)
      );
    } catch (error) {
      clearTimeout(timeout);

      pendingRequests.delete(reqId);

      reject(error);
    }
  });
}

/*
=========================================================
HANDLE DERIV MESSAGE
=========================================================
*/

function handleDerivMessage(raw) {
  try {
    const data = JSON.parse(
      raw.toString()
    );

    lastDerivMessage = data.msg_type || null;

    if (data.error) {
      const reqId = data.req_id;

      if (
        reqId &&
        pendingRequests.has(reqId)
      ) {
        const pending =
          pendingRequests.get(reqId);

        clearTimeout(pending.timeout);

        pendingRequests.delete(reqId);

        pending.reject(
          new Error(
            `${data.error.code || "DerivError"}: ${
              data.error.message || "Unknown error"
            }`
          )
        );
      }

      console.error(
        "Deriv API error:",
        data.error
      );

      return;
    }

    const reqId = data.req_id;

    if (
      reqId &&
      pendingRequests.has(reqId)
    ) {
      const pending =
        pendingRequests.get(reqId);

      clearTimeout(pending.timeout);

      pendingRequests.delete(reqId);

      pending.resolve(data);
    }

  } catch (error) {
    console.error(
      "Could not parse Deriv response:",
      error.message
    );
  }
}

/*
=========================================================
ACTIVE SYMBOLS
=========================================================
*/

async function requestActiveSymbols() {
  try {
    /*
    Newer Deriv API versions removed some old filtering
    parameters. We request the basic list and filter
    Volatility indices locally.
    */
    const response =
      await sendDerivRequest({
        active_symbols: "brief"
      });

    const active =
      Array.isArray(response.active_symbols)
        ? response.active_symbols
        : [];

    const volatility =
      active.filter(
        isVolatilitySymbol
      );

    symbols = volatility
      .map(item => ({
        symbol: symbolCode(item),
        name: symbolName(item),
        market:
          item.market ||
          "synthetic",
        type:
          item.underlying_symbol_type ||
          item.symbol_type ||
          "synthetic",
        open:
          item.exchange_is_open !== 0
      }))
      .filter(item => item.symbol)
      .slice(0, MAX_SYMBOLS);

    console.log(
      `Found ${symbols.length} Volatility indices.`
    );

    if (symbols.length === 0) {
      console.log(
        "No Volatility symbols were found."
      );

      console.log(
        "Deriv returned:",
        active.length,
        "active symbols."
      );
    }

    if (!scanTimer) {
      startScanner();
    }

  } catch (error) {
    lastError =
      error?.message ||
      "Could not get active symbols";

    console.error(
      "Active symbols error:",
      lastError
    );

    scheduleReconnect();
  }
}

/*
=========================================================
GET CANDLES
=========================================================
*/

async function getCandles(
  symbol,
  granularity,
  count = CANDLE_COUNT
) {
  const response =
    await sendDerivRequest(
      {
        ticks_history: symbol,
        end: "latest",
        style: "candles",
        granularity,
        count,
        subscribe: 0
      },
      20000
    );

  if (
    !Array.isArray(
      response.candles
    )
  ) {
    throw new Error(
      `No candles returned for ${symbol}`
    );
  }

  return response.candles
    .map(c => ({
      time: Number(c.epoch),
      open: Number(c.open),
      high: Number(c.high),
      low: Number(c.low),
      close: Number(c.close)
    }))
    .filter(c =>
      Number.isFinite(c.time) &&
      Number.isFinite(c.open) &&
      Number.isFinite(c.high) &&
      Number.isFinite(c.low) &&
      Number.isFinite(c.close)
    )
    .sort(
      (a, b) => a.time - b.time
    );
}

/*
=========================================================
INDICATORS
=========================================================
*/

function ema(values, period = 20) {
  if (
    !Array.isArray(values) ||
    values.length < period
  ) {
    return null;
  }

  const multiplier =
    2 / (period + 1);

  let value = 0;

  for (
    let i = 0;
    i < period;
    i++
  ) {
    value += values[i];
  }

  value /= period;

  for (
    let i = period;
    i < values.length;
    i++
  ) {
    value =
      (values[i] - value) *
        multiplier +
      value;
  }

  return value;
}

function trueRanges(candles) {
  const tr = [];

  for (
    let i = 0;
    i < candles.length;
    i++
  ) {
    if (i === 0) {
      tr.push(
        candles[i].high -
        candles[i].low
      );

      continue;
    }

    const current =
      candles[i];

    const previous =
      candles[i - 1];

    tr.push(
      Math.max(
        current.high -
          current.low,

        Math.abs(
          current.high -
            previous.close
        ),

        Math.abs(
          current.low -
            previous.close
        )
      )
    );
  }

  return tr;
}

function atr(
  candles,
  period = 14
) {
  if (
    candles.length <
    period + 1
  ) {
    return null;
  }

  const tr =
    trueRanges(candles);

  const recent =
    tr.slice(-period);

  return (
    recent.reduce(
      (sum, value) =>
        sum + value,
      0
    ) / recent.length
  );
}

function rsi(
  candles,
  period = 14
) {
  if (
    candles.length <
    period + 1
  ) {
    return null;
  }

  let gains = 0;
  let losses = 0;

  for (
    let i = 1;
    i <= period;
    i++
  ) {
    const change =
      candles[i].close -
      candles[i - 1].close;

    if (change >= 0) {
      gains += change;
    } else {
      losses += Math.abs(change);
    }
  }

  let avgGain =
    gains / period;

  let avgLoss =
    losses / period;

  for (
    let i = period + 1;
    i < candles.length;
    i++
  ) {
    const change =
      candles[i].close -
      candles[i - 1].close;

    const gain =
      change > 0
        ? change
        : 0;

    const loss =
      change < 0
        ? Math.abs(change)
        : 0;

    avgGain =
      ((avgGain *
        (period - 1)) +
        gain) /
      period;

    avgLoss =
      ((avgLoss *
        (period - 1)) +
        loss) /
      period;
  }

  if (avgLoss === 0) {
    return 100;
  }

  const rs =
    avgGain / avgLoss;

  return (
    100 -
    100 / (1 + rs)
  );
}

/*
=========================================================
STRONG BURST FADER
=========================================================
*/

function strongBurstFader(
  candles
) {
  if (
    candles.length <
    30
  ) {
    return {
      direction: "NEUTRAL",
      stack: 0,
      heat: "COOL",
      burst: false,
      extreme: false,
      basis: null,
      upper: null,
      lower: null
    };
  }

  /*
  Use completed candle.
  This prevents the current forming candle from
  constantly changing the signal.
  */
  const completed =
    candles.slice(0, -1);

  const closes =
    completed.map(
      c => c.close
    );

  const basis =
    ema(closes, 20);

  const atrValue =
    atr(completed, 14);

  if (
    !Number.isFinite(basis) ||
    !Number.isFinite(atrValue) ||
    atrValue <= 0
  ) {
    return {
      direction: "NEUTRAL",
      stack: 0,
      heat: "COOL",
      burst: false,
      extreme: false,
      basis: null,
      upper: null,
      lower: null
    };
  }

  const candle =
    completed[
      completed.length - 1
    ];

  const upper =
    basis +
    2 * atrValue;

  const lower =
    basis -
    2 * atrValue;

  let stack = 0;

  let direction =
    "NEUTRAL";

  /*
  Wick pokes count.
  */
  if (
    candle.high >
    upper
  ) {
    direction = "UP";

    const extension =
      candle.high -
      upper;

    stack =
      Math.min(
        6,
        1 +
          Math.floor(
            extension /
              (0.5 *
                atrValue)
          )
      );
  }

  if (
    candle.low <
    lower
  ) {
    const extension =
      lower -
      candle.low;

    const downStack =
      Math.min(
        6,
        1 +
          Math.floor(
            extension /
              (0.5 *
                atrValue)
          )
      );

    if (
      direction === "NEUTRAL" ||
      downStack > stack
    ) {
      direction = "DOWN";
      stack = downStack;
    }
  }

  let heat = "COOL";

  if (stack >= 2) {
    heat = "WARM";
  }

  if (stack >= 3) {
    heat = "HOT";
  }

  if (stack >= 5) {
    heat = "EXTREME";
  }

  return {
    direction,
    stack,
    heat,
    burst: stack >= 1,
    extreme: stack >= 5,
    basis: round(basis, 4),
    upper: round(upper, 4),
    lower: round(lower, 4)
  };
}

/*
=========================================================
STRUCTURE
=========================================================
*/

function structureAnalysis(
  candles
) {
  if (
    candles.length <
    30
  ) {
    return {
      direction: "NEUTRAL",
      structure: "NEUTRAL",
      bos: false,
      choch: false,
      liquidity: "NONE"
    };
  }

  const completed =
    candles.slice(0, -1);

  const last =
    completed[
      completed.length - 1
    ];

  const previous =
    completed[
      completed.length - 2
    ];

  const lookback =
    completed.slice(
      -12,
      -2
    );

  const recentHigh =
    Math.max(
      ...lookback.map(
        c => c.high
      )
    );

  const recentLow =
    Math.min(
      ...lookback.map(
        c => c.low
      )
    );

  let bos = false;
  let choch = false;

  let direction =
    "NEUTRAL";

  let liquidity =
    "NONE";

  /*
  BUY structure
  */
  if (
    last.close >
    recentHigh
  ) {
    bos = true;
    direction = "BULLISH";
  }

  /*
  SELL structure
  */
  if (
    last.close <
    recentLow
  ) {
    bos = true;
    direction = "BEARISH";
  }

  /*
  Liquidity sweep:
  wick takes a recent high/low and closes back inside.
  */
  if (
    last.low <
      recentLow &&
    last.close >
      recentLow
  ) {
    liquidity =
      "SELL-SIDE SWEEP";
    direction =
      "BULLISH";
    choch = true;
  }

  if (
    last.high >
      recentHigh &&
    last.close <
      recentHigh
  ) {
    liquidity =
      "BUY-SIDE SWEEP";
    direction =
      "BEARISH";
    choch = true;
  }

  /*
  EMA backup direction.
  This keeps the bot from becoming too strict.
  */
  const closes =
    completed.map(
      c => c.close
    );

  const ema20 =
    ema(closes, 20);

  if (
    direction === "NEUTRAL" &&
    Number.isFinite(ema20)
  ) {
    if (
      last.close >
      ema20
    ) {
      direction =
        "BULLISH";
    } else if (
      last.close <
      ema20
    ) {
      direction =
        "BEARISH";
    }
  }

  return {
    direction,
    structure:
      direction === "BULLISH"
        ? "BULLISH"
        : direction === "BEARISH"
        ? "BEARISH"
        : "NEUTRAL",
    bos,
    choch,
    liquidity
  };
}

/*
=========================================================
ANALYZE TIMEFRAME
=========================================================
*/

function analyzeTimeframe(
  candles
) {
  if (
    !candles ||
    candles.length < 30
  ) {
    return {
      direction: "NEUTRAL",
      structure: "NEUTRAL",
      bos: false,
      choch: false,
      liquidity: "NONE",
      rsi: null,
      atr: null,
      price: null
    };
  }

  const completed =
    candles.slice(0, -1);

  const closes =
    completed.map(
      c => c.close
    );

  const last =
    completed[
      completed.length - 1
    ];

  const structure =
    structureAnalysis(
      candles
    );

  return {
    ...structure,

    rsi: round(
      rsi(completed, 14),
      1
    ),

    atr: round(
      atr(completed, 14),
      5
    ),

    price: last.close
  };
}

/*
=========================================================
SIGNAL ENGINE
=========================================================
*/

function buildSignal(
  symbol,
  candlesH1,
  candlesM15,
  candlesM5
) {
  const h1 =
    analyzeTimeframe(
      candlesH1
    );

  const m15 =
    analyzeTimeframe(
      candlesM15
    );

  const m5 =
    analyzeTimeframe(
      candlesM5
    );

  const burst =
    strongBurstFader(
      candlesM5
    );

  let score = 0;

  let signal = "WAIT";

  /*
  =========================================
  1. H1 DIRECTION
  =========================================
  */

  const h1Bull =
    h1.direction ===
    "BULLISH";

  const h1Bear =
    h1.direction ===
    "BEARISH";

  /*
  =========================================
  2. M15 ALIGNMENT
  =========================================
  */

  if (
    h1Bull &&
    m15.direction ===
      "BULLISH"
  ) {
    score++;
  }

  if (
    h1Bear &&
    m15.direction ===
      "BEARISH"
  ) {
    score++;
  }

  /*
  =========================================
  3. M15 STRUCTURE
  =========================================
  */

  if (
    h1Bull &&
    (
      m15.bos ||
      m15.choch ||
      m15.direction ===
        "BULLISH"
    )
  ) {
    score++;
  }

  if (
    h1Bear &&
    (
      m15.bos ||
      m15.choch ||
      m15.direction ===
        "BEARISH"
    )
  ) {
    score++;
  }

  /*
  =========================================
  4. M5 STRUCTURE
  =========================================
  */

  if (
    h1Bull &&
    (
      m5.direction ===
        "BULLISH" ||
      m5.choch
    )
  ) {
    score++;
  }

  if (
    h1Bear &&
    (
      m5.direction ===
        "BEARISH" ||
      m5.choch
    )
  ) {
    score++;
  }

  /*
  =========================================
  5. BURST FADER
  =========================================

  IMPORTANT:
  Burst Fader supports the setup.
  It is NOT mandatory.

  This prevents the bot from becoming
  unnecessarily strict.
  */

  if (
    h1Bull &&
    (
      burst.direction ===
        "DOWN" &&
      burst.stack >= 2
    )
  ) {
    score++;
  }

  if (
    h1Bear &&
    (
      burst.direction ===
        "UP" &&
      burst.stack >= 2
    )
  ) {
    score++;
  }

  /*
  =========================================
  SIGNAL LOGIC
  =========================================
  */

  const buySetup =
    h1Bull &&
    m15.direction ===
      "BULLISH" &&
    (
      m5.direction ===
        "BULLISH" ||
      m5.choch ||
      (
        burst.direction ===
          "DOWN" &&
        burst.stack >= 2
      )
    );

  const sellSetup =
    h1Bear &&
    m15.direction ===
      "BEARISH" &&
    (
      m5.direction ===
        "BEARISH" ||
      m5.choch ||
      (
        burst.direction ===
          "UP" &&
        burst.stack >= 2
      )
    );

  if (
    buySetup &&
    score >= 3
  ) {
    signal = "BUY";
  }

  if (
    sellSetup &&
    score >= 3
  ) {
    signal = "SELL";
  }

  /*
  =========================================
  ENTRY / SL / TP
  =========================================
  */

  const completedM5 =
    candlesM5.slice(0, -1);

  const lastM5 =
    completedM5[
      completedM5.length - 1
    ];

  const entry =
    lastM5.close;

  const m5Atr =
    atr(
      completedM5,
      14
    ) || 0;

  const recentM5 =
    completedM5.slice(-8);

  let stopLoss =
    null;

  let takeProfit =
    null;

  if (signal === "BUY") {
    const recentLow =
      Math.min(
        ...recentM5.map(
          c => c.low
        )
      );

    stopLoss =
      recentLow -
      m5Atr * 0.25;

    const risk =
      entry - stopLoss;

    if (risk > 0) {
      takeProfit =
        entry +
        risk * 2;
    }
  }

  if (signal === "SELL") {
    const recentHigh =
      Math.max(
        ...recentM5.map(
          c => c.high
        )
      );

    stopLoss =
      recentHigh +
      m5Atr * 0.25;

    const risk =
      stopLoss - entry;

    if (risk > 0) {
      takeProfit =
        entry -
        risk * 2;
    }
  }

  const decimals =
    getDecimals(symbol);

  return {
    symbol,

    signal,

    score: Math.min(
      score,
      5
    ),

    h1: {
      direction:
        h1.direction,
      structure:
        h1.structure
    },

    m15: {
      direction:
        m15.direction,
      structure:
        m15.structure,
      bos:
        m15.bos,
      choch:
        m15.choch,
      liquidity:
        m15.liquidity
    },

    m5: {
      direction:
        m5.direction,
      structure:
        m5.structure,
      bos:
        m5.bos,
      choch:
        m5.choch,
      liquidity:
        m5.liquidity,
      rsi:
        m5.rsi
    },

    burst: {
      direction:
        burst.direction,
      stack:
        burst.stack,
      heat:
        burst.heat,
      burst:
        burst.burst,
      extreme:
        burst.extreme
    },

    entry:
      round(entry, decimals),

    stopLoss:
      round(stopLoss, decimals),

    takeProfit:
      round(takeProfit, decimals),

    price:
      round(entry, decimals),

    updated:
      new Date().toISOString()
  };
}

/*
=========================================================
TELEGRAM
=========================================================
*/

async function sendTelegram(
  result,
  displayName
) {
  if (
    !TELEGRAM_BOT_TOKEN ||
    !TELEGRAM_CHAT_ID
  ) {
    return;
  }

  const key =
    result.symbol;

  const now =
    Date.now();

  const last =
    lastAlertTimes.get(
      key
    ) || 0;

  if (
    now - last <
    ALERT_COOLDOWN_MS
  ) {
    return;
  }

  lastAlertTimes.set(
    key,
    now
  );

  const emoji =
    result.signal === "BUY"
      ? "🟢"
      : "🔴";

  const message = [
    `${emoji} STRONG ${result.signal}: ${displayName || result.symbol}`,
    "",
    `📍 Entry: ${result.entry}`,
    `🛑 Stop Loss: ${result.stopLoss}`,
    `🎯 Take Profit: ${result.takeProfit}`,
    `⭐ Score: ${result.score}/5`,
    "",
    `📊 1H: ${result.h1.direction}`,
    `📊 15M: ${result.m15.direction}`,
    `📊 5M: ${result.m5.direction}`,
    "",
    `💥 Burst Fader: ${result.burst.direction}`,
    `🔥 Burst Stack: ${result.burst.stack}/6`,
    `🌡 Heat: ${result.burst.heat}`,
    "",
    `🔎 15M Liquidity: ${result.m15.liquidity}`,
    `🔎 5M Liquidity: ${result.m5.liquidity}`,
    `RSI: ${result.m5.rsi ?? "N/A"}`,
    "",
    `⏱ ${new Date().toUTCString()}`
  ].join("\n");

  const url =
    `https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage`;

  try {
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
      console.error(
        "Telegram error:",
        await response.text()
      );

      return;
    }

    alertHistory.unshift({
      symbol:
        result.symbol,

      signal:
        result.signal,

      score:
        result.score,

      time:
        new Date().toISOString()
    });

    if (
      alertHistory.length >
      30
    ) {
      alertHistory.pop();
    }

    console.log(
      `Telegram alert sent: ${result.signal} ${result.symbol}`
    );

  } catch (error) {
    console.error(
      "Telegram send error:",
      error.message
    );
  }
}

/*
=========================================================
SCAN ONE SYMBOL
=========================================================
*/

async function scanSymbol(
  item
) {
  const symbol =
    item.symbol;

  try {
    const candlesH1 =
      await getCandles(
        symbol,
        TIMEFRAMES.H1
      );

    await sleep(
      REQUEST_DELAY_MS
    );

    const candlesM15 =
      await getCandles(
        symbol,
        TIMEFRAMES.M15
      );

    await sleep(
      REQUEST_DELAY_MS
    );

    const candlesM5 =
      await getCandles(
        symbol,
        TIMEFRAMES.M5
      );

    if (
      candlesH1.length < 30 ||
      candlesM15.length < 30 ||
      candlesM5.length < 30
    ) {
      return null;
    }

    const result =
      buildSignal(
        symbol,
        candlesH1,
        candlesM15,
        candlesM5
      );

    result.name =
      item.name;

    /*
    Only alert BUY/SELL.
    WAIT setups remain visible
    on the dashboard.
    */
    if (
      result.signal === "BUY" ||
      result.signal === "SELL"
    ) {
      await sendTelegram(
        result,
        item.name
      );
    }

    return result;

  } catch (error) {
    console.error(
      `Scan error ${symbol}:`,
      error.message
    );

    return {
      symbol,
      name: item.name,
      signal: "WAIT",
      score: 0,
      error:
        error.message,
      h1: {
        direction: "NEUTRAL"
      },
      m15: {
        direction: "NEUTRAL"
      },
      m5: {
        direction: "NEUTRAL"
      },
      burst: {
        direction: "NEUTRAL",
        stack: 0,
        heat: "COOL"
      }
    };
  }
}

/*
=========================================================
FULL SCAN
=========================================================
*/

async function runScan() {
  if (scanning) {
    return;
  }

  if (!connected) {
    return;
  }

  if (
    !symbols ||
    symbols.length === 0
  ) {
    return;
  }

  scanning = true;

  const results = [];

  console.log(
    `Starting scan of ${symbols.length} Volatility indices...`
  );

  try {
    for (
      const item of symbols
    ) {
      if (!connected) {
        break;
      }

      const result =
        await scanSymbol(
          item
        );

      if (result) {
        results.push(
          result
        );
      }

      await sleep(
        REQUEST_DELAY_MS
      );
    }

    scanResults.length = 0;

    for (
      const result of results
    ) {
      scanResults.push(
        result
      );
    }

    lastScan =
      new Date().toISOString();

    console.log(
      `Scan complete: ${results.length} Volatility indices`
    );

  } catch (error) {
    lastError =
      error.message;

    console.error(
      "Scanner error:",
      error.message
    );

  } finally {
    scanning = false;
  }
}

/*
=========================================================
SCANNER START
=========================================================
*/

function startScanner() {
  if (scanTimer) {
    return;
  }

  console.log(
    "Scanner started."
  );

  runScan();

  scanTimer =
    setInterval(
      runScan,
      SCAN_INTERVAL_MS
    );
}

/*
=========================================================
DASHBOARD API
=========================================================
*/

app.get(
  "/api/status",
  (req, res) => {
    res.json({
      ok: true,

      online: true,

      connected,

      connecting,

      derivConnected:
        connected,

      marketOpen: true,

      symbolCount:
        symbols.length,

      symbols:
        symbols,

      volatilityIndices:
        symbols,

      lastScan,

      lastError,

      lastDerivMessage,

      scanning,

      scanResults,

      results:
        scanResults,

      scans:
        scanResults,

      alerts:
        alertHistory,

      recentAlerts:
        alertHistory,

      strategy: {
        h1:
          "Overall direction",

        m15:
          "Structure + liquidity",

        m5:
          "Entry + Strong Burst Fader",

        riskReward:
          "1:2"
      },

      burstFader: {
        enabled: true,

        mandatory: false,

        basis:
          "EMA20",

        atr:
          "ATR14",

        band:
          "2 ATR",

        maximumStack: 6
      },

      time:
        new Date().toISOString()
    });
  }
);

/*
=========================================================
START SERVER
=========================================================
*/

app.listen(
  PORT,
  () => {
    console.log(
      `Deriv Volatility Burst Fader running on port ${PORT}`
    );

    console.log(
      `Dashboard: http://localhost:${PORT}`
    );

    connectDeriv();
  }
);
