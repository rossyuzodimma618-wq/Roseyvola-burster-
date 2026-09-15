const express = require("express");
const path = require("path");
const WebSocket = require("ws");

const app = express();

app.use(express.json());
app.use(express.static(path.join(__dirname, "public")));

const PORT = process.env.PORT || 3000;

/*
=========================================================
DERIV PUBLIC MARKET DATA
=========================================================
*/

const DERIV_WS_URL =
  "wss://api.derivws.com/trading/v1/options/ws/public";

/*
=========================================================
TELEGRAM
=========================================================
*/

const TELEGRAM_BOT_TOKEN =
  process.env.TELEGRAM_BOT_TOKEN;

const TELEGRAM_CHAT_ID =
  process.env.TELEGRAM_CHAT_ID;

/*
=========================================================
SETTINGS
=========================================================
*/

const H1_GRANULARITY = 3600;
const M15_GRANULARITY = 900;
const M5_GRANULARITY = 300;

const CANDLE_COUNT = 250;

const SCAN_INTERVAL_MS = 30000;
const REQUEST_TIMEOUT_MS = 15000;
const ALERT_COOLDOWN_MS = 15 * 60 * 1000;

/*
Strong Burst Fader
*/

const EMA_PERIOD = 20;
const ATR_PERIOD = 14;
const BURST_ATR_MULTIPLIER = 2;
const MAX_BURST_STACK = 6;

/*
=========================================================
GLOBAL STATE
=========================================================
*/

let ws = null;
let wsConnected = false;
let requestId = 1;

const pendingRequests = new Map();

let volatilitySymbols = [];
let lastScan = null;
let scannerRunning = false;

const alertHistory = new Map();
const scanResults = {};

/*
=========================================================
EXPRESS
=========================================================
*/

app.get("/", (req, res) => {
  res.sendFile(
    path.join(__dirname, "public", "index.html")
  );
});

app.get("/health", (req, res) => {
  res.json({
    ok: true,
    online: true,
    derivConnected: wsConnected,
    marketCount: volatilitySymbols.length,
    lastScan
  });
});

app.get("/api/status", (req, res) => {
  res.json({
    ok: true,
    online: true,
    derivConnected: wsConnected,
    symbolCount: volatilitySymbols.length,
    symbols: volatilitySymbols,
    lastScan,
    results: Object.values(scanResults)
  });
});

/*
=========================================================
UTILITY
=========================================================
*/

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function roundPrice(value, decimals = 3) {
  if (!Number.isFinite(value)) {
    return null;
  }

  return Number(value.toFixed(decimals));
}

function getDecimals(symbol) {
  const s = String(symbol || "").toUpperCase();

  if (
    s.includes("XAU") ||
    s.includes("GOLD")
  ) {
    return 2;
  }

  return 3;
}

/*
=========================================================
DERIV WEBSOCKET CONNECTION
=========================================================
*/

function connectDeriv() {
  return new Promise((resolve, reject) => {

    if (
      ws &&
      ws.readyState === WebSocket.OPEN
    ) {
      wsConnected = true;
      resolve();
      return;
    }

    console.log(
      "Connecting to Deriv public WebSocket..."
    );

    try {
      ws = new WebSocket(DERIV_WS_URL);
    } catch (error) {
      wsConnected = false;
      reject(error);
      return;
    }

    let settled = false;

    const connectionTimeout = setTimeout(() => {

      if (!settled) {

        settled = true;

        try {
          ws.close();
        } catch (_) {}

        wsConnected = false;

        reject(
          new Error(
            "Deriv WebSocket connection timeout"
          )
        );
      }

    }, REQUEST_TIMEOUT_MS);

    ws.on("open", () => {

      clearTimeout(connectionTimeout);

      wsConnected = true;

      console.log(
        "Deriv public WebSocket connected."
      );

      if (!settled) {

        settled = true;
        resolve();

      }

    });

    ws.on("message", raw => {

      let data;

      try {

        data = JSON.parse(
          raw.toString()
        );

      } catch (error) {

        console.log(
          "Invalid Deriv message:",
          raw.toString()
        );

        return;
      }

      if (
        data.req_id === undefined
      ) {
        return;
      }

      const request =
        pendingRequests.get(
          data.req_id
        );

      if (!request) {
        return;
      }

      pendingRequests.delete(
        data.req_id
      );

      clearTimeout(
        request.timeout
      );

      if (data.error) {

        const errorMessage =
          data.error.message ||
          data.error.code ||
          "Deriv API error";

        request.reject(
          new Error(
            errorMessage
          )
        );

        return;
      }

      request.resolve(data);

    });

    ws.on("error", error => {

      console.log(
        "Deriv WebSocket error:",
        error.message || error
      );

      wsConnected = false;

      if (!settled) {

        clearTimeout(
          connectionTimeout
        );

        settled = true;

        reject(error);
      }

    });

    ws.on("close", (code, reason) => {

      wsConnected = false;

      console.log(
        "Deriv WebSocket closed:",
        code,
        reason
          ? reason.toString()
          : ""
      );

      for (
        const [
          id,
          request
        ] of pendingRequests.entries()
      ) {

        clearTimeout(
          request.timeout
        );

        request.reject(
          new Error(
            "Deriv WebSocket closed"
          )
        );

        pendingRequests.delete(id);
      }

      setTimeout(() => {

        if (!wsConnected) {

          connectDeriv()
            .catch(() => {});

        }

      }, 5000);

    });

  });
}

/*
=========================================================
DERIV REQUEST
=========================================================
IMPORTANT:
No subscribe field is automatically added.
=========================================================
*/

async function derivRequest(payload) {

  await connectDeriv();

  if (
    !ws ||
    ws.readyState !== WebSocket.OPEN
  ) {

    throw new Error(
      "Deriv WebSocket is not open"
    );

  }

  /*
  Build request explicitly.
  We do NOT add subscribe here.
  */

  const id = requestId++;

  const requestPayload = {
    req_id: id
  };

  Object.keys(payload).forEach(key => {

    /*
    Safety guard:
    never send subscribe.
    */

    if (
      key !== "subscribe"
    ) {

      requestPayload[key] =
        payload[key];

    }

  });

  console.log(
    "Deriv request:",
    JSON.stringify(
      requestPayload
    )
  );

  return new Promise(
    (resolve, reject) => {

      const timeout =
        setTimeout(() => {

          pendingRequests.delete(id);

          reject(
            new Error(
              "Deriv request timeout"
            )
          );

        }, REQUEST_TIMEOUT_MS);

      pendingRequests.set(id, {
        resolve,
        reject,
        timeout
      });

      try {

        ws.send(
          JSON.stringify(
            requestPayload
          )
        );

      } catch (error) {

        clearTimeout(timeout);

        pendingRequests.delete(id);

        reject(error);
      }

    }
  );
}

/*
=========================================================
ACTIVE SYMBOLS
=========================================================
*/

async function getActiveSymbols() {

  const data =
    await derivRequest({
      active_symbols: "brief"
    });

  if (
    !Array.isArray(
      data.active_symbols
    )
  ) {

    throw new Error(
      "Deriv returned no active symbols"
    );

  }

  const result = [];

  for (
    const item of
    data.active_symbols
  ) {

    const symbol =
      item.underlying_symbol ||
      item.symbol;

    const name =
      item.underlying_symbol_name ||
      item.display_name ||
      symbol;

    const type =
      item.underlying_symbol_type ||
      item.symbol_type ||
      "";

    const market =
      item.market ||
      "";

    if (!symbol) {
      continue;
    }

    const combined =
      `${symbol} ${name} ${type} ${market}`
        .toLowerCase();

    const isVolatility =
      combined.includes(
        "volatility"
      ) ||
      /^1hz\d+v/i.test(symbol) ||
      /^r_\d+/i.test(symbol);

    if (!isVolatility) {
      continue;
    }

    result.push({
      symbol,
      name,
      type,
      market
    });

  }

  const unique = [];
  const seen = new Set();

  for (
    const item of result
  ) {

    if (!item.symbol) {
      continue;
    }

    if (
      seen.has(item.symbol)
    ) {
      continue;
    }

    seen.add(item.symbol);

    unique.push(item);

  }

  volatilitySymbols =
    unique;

  console.log(
    `Found ${volatilitySymbols.length} Volatility/synthetic symbols.`
  );

  return volatilitySymbols;
}

/*
=========================================================
GET CANDLES
=========================================================
NO subscribe parameter.
=========================================================
*/

async function getCandles(
  symbol,
  granularity
) {

  const data =
    await derivRequest({

      ticks_history: symbol,

      end: "latest",

      count: CANDLE_COUNT,

      style: "candles",

      granularity

    });

  if (
    !Array.isArray(
      data.candles
    )
  ) {

    throw new Error(
      `No candles returned for ${symbol} ${granularity}`
    );

  }

  const candles =
    data.candles

      .map(c => ({

        time:
          Number(c.epoch),

        open:
          Number(c.open),

        high:
          Number(c.high),

        low:
          Number(c.low),

        close:
          Number(c.close)

      }))

      .filter(c =>

        Number.isFinite(c.time) &&

        Number.isFinite(c.open) &&

        Number.isFinite(c.high) &&

        Number.isFinite(c.low) &&

        Number.isFinite(c.close)

      )

      .sort(
        (a, b) =>
          a.time - b.time
      );

  /*
  Remove currently forming candle.
  */

  if (
    candles.length > 1
  ) {

    candles.pop();

  }

  return candles;
}

/*
=========================================================
EMA
=========================================================
*/

function calculateEMA(
  values,
  period
) {

  if (
    !Array.isArray(values) ||
    values.length < period
  ) {

    return null;

  }

  const multiplier =
    2 / (period + 1);

  let ema = 0;

  for (
    let i = 0;
    i < period;
    i++
  ) {

    ema += values[i];

  }

  ema /= period;

  for (
    let i = period;
    i < values.length;
    i++
  ) {

    ema =
      (values[i] - ema) *
      multiplier +
      ema;

  }

  return ema;
}

/*
=========================================================
ATR
=========================================================
*/

function calculateATR(
  candles,
  period = ATR_PERIOD
) {

  if (
    !Array.isArray(candles) ||
    candles.length <
      period + 1
  ) {

    return null;

  }

  const trueRanges = [];

  for (
    let i = 1;
    i < candles.length;
    i++
  ) {

    const current =
      candles[i];

    const previous =
      candles[i - 1];

    const tr =
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

      );

    trueRanges.push(tr);

  }

  if (
    trueRanges.length <
    period
  ) {

    return null;

  }

  let atr = 0;

  for (
    let i = 0;
    i < period;
    i++
  ) {

    atr +=
      trueRanges[i];

  }

  atr /= period;

  for (
    let i = period;
    i < trueRanges.length;
    i++
  ) {

    atr =
      (
        atr *
        (period - 1) +
        trueRanges[i]
      ) / period;

  }

  return atr;
}

/*
=========================================================
RSI
=========================================================
*/

function calculateRSI(
  candles,
  period = 14
) {

  if (
    candles.length <
    period + 2
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

    const difference =
      candles[i].close -
      candles[i - 1].close;

    if (
      difference >= 0
    ) {

      gains +=
        difference;

    } else {

      losses +=
        Math.abs(
          difference
        );

    }

  }

  let averageGain =
    gains / period;

  let averageLoss =
    losses / period;

  for (
    let i = period + 1;
    i < candles.length;
    i++
  ) {

    const difference =
      candles[i].close -
      candles[i - 1].close;

    const gain =
      difference > 0
        ? difference
        : 0;

    const loss =
      difference < 0
        ? Math.abs(difference)
        : 0;

    averageGain =
      (
        averageGain *
          (period - 1) +
        gain
      ) / period;

    averageLoss =
      (
        averageLoss *
          (period - 1) +
        loss
      ) / period;

  }

  if (
    averageLoss === 0
  ) {

    return 100;

  }

  const rs =
    averageGain /
    averageLoss;

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
    EMA_PERIOD +
      ATR_PERIOD +
      5
  ) {

    return {
      signal: "NEUTRAL",
      stack: 0,
      heat: "COOL",
      basis: null,
      upper: null,
      lower: null
    };

  }

  const closes =
    candles.map(
      c => c.close
    );

  const basis =
    calculateEMA(
      closes,
      EMA_PERIOD
    );

  const atr =
    calculateATR(
      candles,
      ATR_PERIOD
    );

  if (
    basis === null ||
    atr === null ||
    atr <= 0
  ) {

    return {
      signal: "NEUTRAL",
      stack: 0,
      heat: "COOL",
      basis: null,
      upper: null,
      lower: null
    };

  }

  const upper =
    basis +
    BURST_ATR_MULTIPLIER *
      atr;

  const lower =
    basis -
    BURST_ATR_MULTIPLIER *
      atr;

  const candle =
    candles[
      candles.length - 1
    ];

  let upStack = 0;
  let downStack = 0;

  if (
    candle.high > upper
  ) {

    upStack =
      Math.min(

        MAX_BURST_STACK,

        Math.max(

          1,

          Math.floor(
            (
              candle.high -
              basis
            ) / atr
          )

        )

      );

  }

  if (
    candle.low < lower
  ) {

    downStack =
      Math.min(

        MAX_BURST_STACK,

        Math.max(

          1,

          Math.floor(
            (
              basis -
              candle.low
            ) / atr
          )

        )

      );

  }

  let signal =
    "NEUTRAL";

  let stack = 0;

  if (
    upStack >
      downStack &&
    upStack > 0
  ) {

    signal =
      "UP BURST";

    stack =
      upStack;

  }

  if (
    downStack >
      upStack &&
    downStack > 0
  ) {

    signal =
      "DOWN BURST";

    stack =
      downStack;

  }

  let heat =
    "COOL";

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
    signal,
    stack,
    heat,
    basis,
    upper,
    lower
  };
}

/*
=========================================================
MARKET STRUCTURE
=========================================================
*/

function analyzeStructure(
  candles
) {

  if (
    candles.length < 30
  ) {

    return {
      direction: "NEUTRAL",
      structure: "NEUTRAL",
      bos: "NONE",
      choch: "NONE"
    };

  }

  const closes =
    candles.map(
      c => c.close
    );

  const highs =
    candles.map(
      c => c.high
    );

  const lows =
    candles.map(
      c => c.low
    );

  const ema =
    calculateEMA(
      closes,
      EMA_PERIOD
    );

  if (ema === null) {

    return {
      direction: "NEUTRAL",
      structure: "NEUTRAL",
      bos: "NONE",
      choch: "NONE"
    };

  }

  const last =
    candles[
      candles.length - 1
    ];

  const previous =
    candles[
      candles.length - 2
    ];

  const lookback = 10;

  const recentHighs =
    highs.slice(
      Math.max(
        0,
        highs.length -
          lookback -
          1
      ),
      highs.length - 1
    );

  const recentLows =
    lows.slice(
      Math.max(
        0,
        lows.length -
          lookback -
          1
      ),
      lows.length - 1
    );

  const previousHigh =
    Math.max(
      ...recentHighs
    );

  const previousLow =
    Math.min(
      ...recentLows
    );

  let direction =
    "NEUTRAL";

  let bos =
    "NONE";

  let choch =
    "NONE";

  if (
    last.close > ema
  ) {

    direction =
      "BULLISH";

  }

  if (
    last.close < ema
  ) {

    direction =
      "BEARISH";

  }

  if (
    last.close >
    previousHigh
  ) {

    bos =
      "BULLISH BOS";

  }

  if (
    last.close <
    previousLow
  ) {

    bos =
      "BEARISH BOS";

  }

  if (
    previous.close < ema &&
    last.close > ema &&
    last.close >
      previous.close
  ) {

    choch =
      "BULLISH CHoCH";

  }

  if (
    previous.close > ema &&
    last.close < ema &&
    last.close <
      previous.close
  ) {

    choch =
      "BEARISH CHoCH";

  }

  let structure =
    direction;

  if (
    bos !== "NONE"
  ) {

    structure =
      bos.includes(
        "BULLISH"
      )
        ? "BULLISH"
        : "BEARISH";

  }

  if (
    choch !== "NONE"
  ) {

    structure =
      choch.includes(
        "BULLISH"
      )
        ? "BULLISH"
        : "BEARISH";

  }

  return {
    direction,
    structure,
    bos,
    choch
  };
}

/*
=========================================================
LIQUIDITY SWEEP
=========================================================
*/

function detectLiquiditySweep(
  candles
) {

  if (
    candles.length < 20
  ) {

    return "NONE";

  }

  const current =
    candles[
      candles.length - 1
    ];

  const previousCandles =
    candles.slice(
      candles.length - 11,
      candles.length - 1
    );

  const previousHigh =
    Math.max(
      ...previousCandles.map(
        c => c.high
      )
    );

  const previousLow =
    Math.min(
      ...previousCandles.map(
        c => c.low
      )
    );

  /*
  Downside liquidity sweep.
  */

  if (
    current.low <
      previousLow &&
    current.close >
      previousLow
  ) {

    return "BEARISH LIQUIDITY SWEEP";

  }

  /*
  Upside liquidity sweep.
  */

  if (
    current.high >
      previousHigh &&
    current.close <
      previousHigh
  ) {

    return "BULLISH LIQUIDITY SWEEP";

  }

  return "NONE";
}

/*
=========================================================
TIMEFRAME ANALYSIS
=========================================================
*/

function analyzeTimeframe(
  candles
) {

  const structure =
    analyzeStructure(
      candles
    );

  const rsi =
    calculateRSI(
      candles
    );

  const atr =
    calculateATR(
      candles
    );

  const burst =
    strongBurstFader(
      candles
    );

  const sweep =
    detectLiquiditySweep(
      candles
    );

  return {

    direction:
      structure.direction,

    structure:
      structure.structure,

    bos:
      structure.bos,

    choch:
      structure.choch,

    rsi,

    atr,

    burst,

    sweep

  };
}

/*
=========================================================
SIGNAL ENGINE
=========================================================
*/

function generateSignal(
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

  let score = 0;

  /*
  1. H1 direction.
  */

  if (
    h1.direction ===
      "BULLISH" ||
    h1.direction ===
      "BEARISH"
  ) {

    score++;

  }

  /*
  2. M15 agrees with H1.
  */

  if (
    h1.direction !==
      "NEUTRAL" &&
    m15.direction ===
      h1.direction
  ) {

    score++;

  }

  /*
  3. M15 structure.
  */

  if (
    m15.structure ===
      h1.direction &&
    h1.direction !==
      "NEUTRAL"
  ) {

    score++;

  }

  /*
  4. M5 confirmation.
  */

  if (
    m5.structure ===
      h1.direction &&
    h1.direction !==
      "NEUTRAL"
  ) {

    score++;

  }

  /*
  5. Burst Fader support.
  NOT mandatory.
  */

  let burstSupport =
    false;

  if (
    h1.direction ===
      "BULLISH" &&
    m5.burst.signal ===
      "DOWN BURST"
  ) {

    burstSupport = true;

  }

  if (
    h1.direction ===
      "BEARISH" &&
    m5.burst.signal ===
      "UP BURST"
  ) {

    burstSupport = true;

  }

  if (burstSupport) {
    score++;
  }

  let signal =
    "WAIT";

  const bullishBase =
    h1.direction ===
      "BULLISH" &&
    m15.direction ===
      "BULLISH";

  const bearishBase =
    h1.direction ===
      "BEARISH" &&
    m15.direction ===
      "BEARISH";

  /*
  BUY
  */

  if (
    bullishBase &&
    (
      m5.structure ===
        "BULLISH" ||

      m5.burst.signal ===
        "DOWN BURST" ||

      m5.sweep ===
        "BEARISH LIQUIDITY SWEEP"
    ) &&
    score >= 3
  ) {

    signal =
      "BUY";

  }

  /*
  SELL
  */

  if (
    bearishBase &&
    (
      m5.structure ===
        "BEARISH" ||

      m5.burst.signal ===
        "UP BURST" ||

      m5.sweep ===
        "BULLISH LIQUIDITY SWEEP"
    ) &&
    score >= 3
  ) {

    signal =
      "SELL";

  }

  /*
  Entry / SL / TP
  */

  const last =
    candlesM5[
      candlesM5.length - 1
    ];

  const entry =
    last.close;

  const decimals =
    getDecimals(symbol);

  const atr =
    m5.atr ||
    Math.abs(
      last.high -
      last.low
    );

  let stopLoss = null;
  let takeProfit = null;

  if (
    signal === "BUY"
  ) {

    const recentLow =
      Math.min(
        ...candlesM5
          .slice(-8)
          .map(
            c => c.low
          )
      );

    stopLoss =
      recentLow -
      atr * 0.20;

    const risk =
      entry -
      stopLoss;

    if (risk > 0) {

      takeProfit =
        entry +
        risk * 2;

    }

  }

  if (
    signal === "SELL"
  ) {

    const recentHigh =
      Math.max(
        ...candlesM5
          .slice(-8)
          .map(
            c => c.high
          )
      );

    stopLoss =
      recentHigh +
      atr * 0.20;

    const risk =
      stopLoss -
      entry;

    if (risk > 0) {

      takeProfit =
        entry -
        risk * 2;

    }

  }

  return {

    symbol,

    signal,

    score,

    h1:
      h1.direction,

    m15:
      m15.direction,

    m5:
      m5.direction,

    h1Structure:
      h1.structure,

    m15Structure:
      m15.structure,

    m5Structure:
      m5.structure,

    h1BOS:
      h1.bos,

    m15BOS:
      m15.bos,

    m5BOS:
      m5.bos,

    h1CHoCH:
      h1.choch,

    m15CHoCH:
      m15.choch,

    m5CHoCH:
      m5.choch,

    rsi:
      m5.rsi !== null
        ? Number(
            m5.rsi.toFixed(1)
          )
        : null,

    burstSignal:
      m5.burst.signal,

    burstStack:
      m5.burst.stack,

    burstHeat:
      m5.burst.heat,

    liquiditySweep:
      m5.sweep,

    entry:
      roundPrice(
        entry,
        decimals
      ),

    stopLoss:
      stopLoss !== null
        ? roundPrice(
            stopLoss,
            decimals
          )
        : null,

    takeProfit:
      takeProfit !== null
        ? roundPrice(
            takeProfit,
            decimals
          )
        : null,

    price:
      roundPrice(
        entry,
        decimals
      ),

    timestamp:
      new Date().toISOString()

  };
}

/*
=========================================================
TELEGRAM
=========================================================
*/

async function sendTelegram(
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
      await fetch(
        url,
        {

          method: "POST",

          headers: {
            "Content-Type":
              "application/json"
          },

          body:
            JSON.stringify({

              chat_id:
                TELEGRAM_CHAT_ID,

              text:
                message

            })

        }
      );

    if (!response.ok) {

      console.log(
        "Telegram error:",
        response.status
      );

    }

  } catch (error) {

    console.log(
      "Telegram request error:",
      error.message
    );

  }
}

/*
=========================================================
TELEGRAM ALERT
=========================================================
*/

function createAlertMessage(
  result
) {

  const icon =
    result.signal === "BUY"
      ? "🟢"
      : "🔴";

  return (
`${icon} STRONG ${result.signal}: ${result.symbol}

📍 Entry: ${result.entry}
🛑 Stop Loss: ${result.stopLoss}
🎯 Take Profit: ${result.takeProfit}

⭐ Score: ${result.score}/5

📊 1H: ${result.h1}
📊 15M: ${result.m15}
📊 5M: ${result.m5}

RSI: ${result.rsi ?? "N/A"}

💥 Burst Fader: ${result.burstSignal}
🔥 Burst Heat: ${result.burstHeat}
📈 Burst Stack: ${result.burstStack}

🔎 Liquidity: ${result.liquiditySweep}

🏗 M15 Structure: ${result.m15Structure}
🏗 M5 Structure: ${result.m5Structure}

${
  result.m5BOS !== "NONE"
    ? `💥 ${result.m5BOS}`
    : ""
}

${
  result.m5CHoCH !== "NONE"
    ? `🔄 ${result.m5CHoCH}`
    : ""
}

⚖️ Risk/Reward: 1:2

🤖 Deriv Volatility Burst Fader`
  );

}

/*
=========================================================
ALERT COOLDOWN
=========================================================
*/

async function maybeSendAlert(
  result
) {

  if (
    result.signal !== "BUY" &&
    result.signal !== "SELL"
  ) {

    return;

  }

  if (
    result.score < 3
  ) {

    return;

  }

  const key =
    `${result.symbol}_${result.signal}`;

  const now =
    Date.now();

  const lastAlert =
    alertHistory.get(key) ||
    0;

  if (
    now - lastAlert <
    ALERT_COOLDOWN_MS
  ) {

    return;

  }

  alertHistory.set(
    key,
    now
  );

  const message =
    createAlertMessage(
      result
    );

  console.log(
    "\n" +
    message +
    "\n"
  );

  await sendTelegram(
    message
  );
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

    /*
    H1
    */

    const h1Candles =
      await getCandles(
        symbol,
        H1_GRANULARITY
      );

    await sleep(250);

    /*
    15M
    */

    const m15Candles =
      await getCandles(
        symbol,
        M15_GRANULARITY
      );

    await sleep(250);

    /*
    5M
    */

    const m5Candles =
      await getCandles(
        symbol,
        M5_GRANULARITY
      );

    if (
      h1Candles.length < 40 ||
      m15Candles.length < 40 ||
      m5Candles.length < 40
    ) {

      console.log(
        `Not enough candles for ${symbol}`
      );

      return null;

    }

    const result =
      generateSignal(
        symbol,
        h1Candles,
        m15Candles,
        m5Candles
      );

    result.displayName =
      item.name;

    result.market =
      item.market;

    result.symbolType =
      item.type;

    scanResults[symbol] =
      result;

    await maybeSendAlert(
      result
    );

    return result;

  } catch (error) {

    console.log(
      `Scan error for ${symbol}:`,
      error.message
    );

    scanResults[symbol] = {

      symbol,

      displayName:
        item.name,

      signal:
        "WAIT",

      score:
        0,

      error:
        error.message,

      timestamp:
        new Date().toISOString()

    };

    return null;
  }
}

/*
=========================================================
SCAN ALL
=========================================================
*/

async function scanAll() {

  if (
    scannerRunning
  ) {

    console.log(
      "Previous scan still running. Skipping."
    );

    return;

  }

  scannerRunning =
    true;

  try {

    if (
      volatilitySymbols.length === 0
    ) {

      await getActiveSymbols();

    }

    lastScan =
      new Date().toISOString();

    console.log(
      `\nScanning ${volatilitySymbols.length} Volatility symbols...`
    );

    for (
      const item of
      volatilitySymbols
    ) {

      await scanSymbol(
        item
      );

      await sleep(250);

    }

    lastScan =
      new Date().toISOString();

    console.log(
      "Scan completed:",
      lastScan
    );

  } catch (error) {

    console.log(
      "Scanner error:",
      error.message
    );

  } finally {

    scannerRunning =
      false;

  }
}

/*
=========================================================
START SERVER
=========================================================
*/

app.listen(
  PORT,
  "0.0.0.0",
  async () => {

    console.log(
      `Deriv Volatility Burst Fader running on port ${PORT}`
    );

    console.log(
      "Dashboard server is ready."
    );

    try {

      await connectDeriv();

      await getActiveSymbols();

      await scanAll();

      setInterval(
        scanAll,
        SCAN_INTERVAL_MS
      );

    } catch (error) {

      console.log(
        "Startup market-data error:",
        error.message
      );

      setTimeout(
        () => {

          connectDeriv()

            .then(
              getActiveSymbols
            )

            .then(
              scanAll
            )

            .catch(err => {

              console.log(
                "Retry error:",
                err.message
              );

            });

        },
        10000
      );

      setInterval(
        scanAll,
        SCAN_INTERVAL_MS
      );

    }

  }
);
