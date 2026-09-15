const express = require("express");
const path = require("path");
const WebSocket = require("ws");

const app = express();

const PORT = process.env.PORT || 3000;

const TELEGRAM_BOT_TOKEN = process.env.TELEGRAM_BOT_TOKEN;
const TELEGRAM_CHAT_ID = process.env.TELEGRAM_CHAT_ID;

const DERIV_WS_URL = "wss://ws.binaryws.com/websockets/v3";

const H1 = 3600;
const M15 = 900;
const M5 = 300;

const SCAN_INTERVAL = 30000;
const ALERT_COOLDOWN = 15 * 60 * 1000;

let derivWs = null;
let requestId = 1;
let pendingRequests = new Map();

let symbols = [];
let scans = [];
let recentAlerts = [];
let lastScan = null;
let connected = false;
let scanning = false;

const lastAlertTime = {};

/* =========================================================
   EXPRESS
========================================================= */

app.use(express.json());

app.use(express.static(path.join(__dirname, "public")));

/* IMPORTANT: SERVE DASHBOARD AT ROOT */
app.get("/", (req, res) => {
  res.sendFile(path.join(__dirname, "public", "index.html"));
});

/* =========================================================
   HEALTH
========================================================= */

app.get("/health", (req, res) => {
  res.json({
    ok: true,
    online: true,
    connected,
    symbols: symbols.length,
    lastScan
  });
});

/* =========================================================
   STATUS API
========================================================= */

app.get("/api/status", (req, res) => {
  res.json({
    ok: true,
    online: true,
    connected,
    derivConnected: connected,
    symbolCount: symbols.length,
    symbols: scans,
    scans,
    alerts: recentAlerts,
    recentAlerts,
    lastScan
  });
});

/* =========================================================
   DERIV CONNECTION
========================================================= */

function connectDeriv() {
  if (derivWs) {
    try {
      derivWs.close();
    } catch (e) {}
  }

  derivWs = new WebSocket(DERIV_WS_URL);

  derivWs.on("open", () => {
    connected = true;

    console.log("Connected to Deriv.");

    sendRequest({
      active_symbols: "brief",
      product_type: "basic"
    })
      .then((data) => {
        if (
          data &&
          Array.isArray(data.active_symbols)
        ) {
          symbols = data.active_symbols
            .filter(isVolatilitySymbol)
            .map((item) => ({
              symbol: item.symbol,
              displayName:
                item.display_name ||
                item.symbol
            }));

          console.log(
            `Found ${symbols.length} Volatility indices.`
          );
        }

        startScanner();
      })
      .catch((error) => {
        console.error(
          "Active symbols error:",
          error.message
        );

        startScanner();
      });
  });

  derivWs.on("message", (raw) => {
    try {
      const data = JSON.parse(raw.toString());

      if (data.req_id && pendingRequests.has(data.req_id)) {
        const request = pendingRequests.get(data.req_id);

        pendingRequests.delete(data.req_id);

        if (data.error) {
          request.reject(
            new Error(data.error.message || "Deriv API error")
          );
        } else {
          request.resolve(data);
        }
      }
    } catch (error) {
      console.error(
        "Deriv message error:",
        error.message
      );
    }
  });

  derivWs.on("close", () => {
    connected = false;

    console.log(
      "Deriv connection closed. Reconnecting..."
    );

    setTimeout(connectDeriv, 5000);
  });

  derivWs.on("error", (error) => {
    connected = false;

    console.error(
      "Deriv WebSocket error:",
      error.message
    );
  });
}

/* =========================================================
   DERIV REQUEST
========================================================= */

function sendRequest(payload) {
  return new Promise((resolve, reject) => {
    if (
      !derivWs ||
      derivWs.readyState !== WebSocket.OPEN
    ) {
      reject(
        new Error("Deriv WebSocket not connected")
      );

      return;
    }

    const req_id = requestId++;

    pendingRequests.set(req_id, {
      resolve,
      reject
    });

    derivWs.send(
      JSON.stringify({
        ...payload,
        req_id
      })
    );

    setTimeout(() => {
      if (pendingRequests.has(req_id)) {
        pendingRequests.delete(req_id);

        reject(
          new Error("Deriv request timeout")
        );
      }
    }, 15000);
  });
}

/* =========================================================
   VOLATILITY SYMBOL FILTER
========================================================= */

function isVolatilitySymbol(item) {
  const display =
    String(item.display_name || "").toLowerCase();

  const symbol =
    String(item.symbol || "").toUpperCase();

  return (
    display.includes("volatility") ||
    /^(R_|1HZ|HZ)/.test(symbol)
  );
}

/* =========================================================
   CANDLE DATA
========================================================= */

async function getCandles(symbol, granularity) {
  const data = await sendRequest({
    ticks_history: symbol,
    style: "candles",
    granularity,
    count: 250,
    end: "latest",
    adjust_start_time: 1,
    subscribe: 0
  });

  if (
    !data ||
    !Array.isArray(data.candles)
  ) {
    throw new Error(
      `No candle data for ${symbol}`
    );
  }

  return data.candles.map((c) => ({
    time: Number(c.epoch),
    open: Number(c.open),
    high: Number(c.high),
    low: Number(c.low),
    close: Number(c.close)
  }));
}

/* =========================================================
   EMA
========================================================= */

function ema(values, period) {
  if (!values.length) return null;

  if (values.length < period) {
    return values[values.length - 1];
  }

  const multiplier =
    2 / (period + 1);

  let result = values
    .slice(0, period)
    .reduce((a, b) => a + b, 0) / period;

  for (
    let i = period;
    i < values.length;
    i++
  ) {
    result =
      (values[i] - result) *
        multiplier +
      result;
  }

  return result;
}

/* =========================================================
   ATR
========================================================= */

function atr(candles, period = 14) {
  if (candles.length < 2) return null;

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
    return trs[trs.length - 1] || 0;
  }

  let value =
    trs
      .slice(0, period)
      .reduce((a, b) => a + b, 0) /
    period;

  for (
    let i = period;
    i < trs.length;
    i++
  ) {
    value =
      ((value * (period - 1)) +
        trs[i]) /
      period;
  }

  return value;
}

/* =========================================================
   RSI
========================================================= */

function rsi(candles, period = 14) {
  if (candles.length < period + 1) {
    return 50;
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

  let avgGain = gains / period;
  let avgLoss = losses / period;

  for (
    let i = period + 1;
    i < candles.length;
    i++
  ) {
    const change =
      candles[i].close -
      candles[i - 1].close;

    const gain =
      change > 0 ? change : 0;

    const loss =
      change < 0 ? Math.abs(change) : 0;

    avgGain =
      ((avgGain * (period - 1)) +
        gain) /
      period;

    avgLoss =
      ((avgLoss * (period - 1)) +
        loss) /
      period;
  }

  if (avgLoss === 0) return 100;

  const rs = avgGain / avgLoss;

  return 100 - 100 / (1 + rs);
}

/* =========================================================
   STRUCTURE
========================================================= */

function structure(candles) {
  if (candles.length < 10) {
    return "NEUTRAL";
  }

  const last =
    candles[candles.length - 1];

  const previous =
    candles[candles.length - 2];

  const lookback =
    candles.slice(
      Math.max(0, candles.length - 12),
      candles.length - 2
    );

  const previousHigh =
    Math.max(
      ...lookback.map((c) => c.high)
    );

  const previousLow =
    Math.min(
      ...lookback.map((c) => c.low)
    );

  if (last.close > previousHigh) {
    return "BULLISH BOS";
  }

  if (last.close < previousLow) {
    return "BEARISH BOS";
  }

  if (
    last.close > previous.close &&
    last.close >
      ema(
        candles.map((c) => c.close),
        20
      )
  ) {
    return "BULLISH";
  }

  if (
    last.close < previous.close &&
    last.close <
      ema(
        candles.map((c) => c.close),
        20
      )
  ) {
    return "BEARISH";
  }

  return "NEUTRAL";
}

/* =========================================================
   STRONG BURST FADER
========================================================= */

function burstFader(candles) {
  if (candles.length < 25) {
    return {
      direction: "NONE",
      stack: 0,
      label: "NO BURST"
    };
  }

  const completed =
    candles.slice(
      0,
      candles.length - 1
    );

  const closes =
    completed.map(
      (c) => c.close
    );

  const basis =
    ema(closes, 20);

  const atrValue =
    atr(completed, 14);

  if (!basis || !atrValue) {
    return {
      direction: "NONE",
      stack: 0,
      label: "NO BURST"
    };
  }

  const last =
    completed[
      completed.length - 1
    ];

  const upper =
    basis + 2 * atrValue;

  const lower =
    basis - 2 * atrValue;

  let direction = "NONE";
  let extension = 0;

  if (last.high > upper) {
    direction = "UP";

    extension =
      (last.high - upper) /
      atrValue;
  }

  if (last.low < lower) {
    const downExtension =
      (lower - last.low) /
      atrValue;

    if (
      direction === "NONE" ||
      downExtension > extension
    ) {
      direction = "DOWN";
      extension = downExtension;
    }
  }

  const stack = Math.min(
    6,
    Math.max(
      0,
      Math.floor(
        extension / 0.5
      ) + 2
    )
  );

  let label = "NO BURST";

  if (direction === "UP") {
    label =
      stack >= 5
        ? "EXTREME UP BURST"
        : stack >= 3
        ? "STRONG UP BURST"
        : "UP BURST";
  }

  if (direction === "DOWN") {
    label =
      stack >= 5
        ? "EXTREME DOWN BURST"
        : stack >= 3
        ? "STRONG DOWN BURST"
        : "DOWN BURST";
  }

  return {
    direction,
    stack,
    label
  };
}

/* =========================================================
   SIGNAL ANALYSIS
========================================================= */

function analyze(
  symbol,
  displayName,
  h1Candles,
  m15Candles,
  m5Candles
) {
  const h1Structure =
    structure(h1Candles);

  const m15Structure =
    structure(m15Candles);

  const m5Structure =
    structure(m5Candles);

  const h1Closes =
    h1Candles.map(
      (c) => c.close
    );

  const h1EMA =
    ema(h1Closes, 20);

  const h1Last =
    h1Candles[
      h1Candles.length - 1
    ];

  let h1Direction =
    "NEUTRAL";

  if (
    h1Last.close > h1EMA
  ) {
    h1Direction = "BULLISH";
  }

  if (
    h1Last.close < h1EMA
  ) {
    h1Direction = "BEARISH";
  }

  let m15Direction =
    "NEUTRAL";

  const m15EMA =
    ema(
      m15Candles.map(
        (c) => c.close
      ),
      20
    );

  const m15Last =
    m15Candles[
      m15Candles.length - 1
    ];

  if (
    m15Last.close > m15EMA
  ) {
    m15Direction = "BULLISH";
  }

  if (
    m15Last.close < m15EMA
  ) {
    m15Direction = "BEARISH";
  }

  let m5Direction =
    "NEUTRAL";

  const m5EMA =
    ema(
      m5Candles.map(
        (c) => c.close
      ),
      20
    );

  const m5Last =
    m5Candles[
      m5Candles.length - 1
    ];

  if (
    m5Last.close > m5EMA
  ) {
    m5Direction = "BULLISH";
  }

  if (
    m5Last.close < m5EMA
  ) {
    m5Direction = "BEARISH";
  }

  const burst =
    burstFader(m5Candles);

  const rsiValue =
    rsi(m5Candles);

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
      m15Structure.includes("BULLISH")
    ) ||
    (
      h1Direction === "BEARISH" &&
      m15Structure.includes("BEARISH")
    )
  ) {
    score++;
  }

  if (
    (
      h1Direction === "BULLISH" &&
      m5Structure.includes("BULLISH")
    ) ||
    (
      h1Direction === "BEARISH" &&
      m5Structure.includes("BEARISH")
    )
  ) {
    score++;
  }

  if (burst.stack >= 3) {
    score++;
  }

  let signal = "WAIT";

  /*
    Burst Fader is SUPPORTING evidence.
    It is NOT a mandatory gate.
  */

  const bullishSetup =
    h1Direction === "BULLISH" &&
    m15Direction === "BULLISH" &&
    (
      m5Structure.includes("BULLISH") ||
      burst.direction === "DOWN"
    ) &&
    score >= 3;

  const bearishSetup =
    h1Direction === "BEARISH" &&
    m15Direction === "BEARISH" &&
    (
      m5Structure.includes("BEARISH") ||
      burst.direction === "UP"
    ) &&
    score >= 3;

  if (bullishSetup) {
    signal = "BUY";
  }

  if (bearishSetup) {
    signal = "SELL";
  }

  const entry =
    m5Last.close;

  const m5ATR =
    atr(m5Candles, 14) ||
    Math.abs(
      m5Last.high -
        m5Last.low
    );

  let sl;
  let tp;

  if (signal === "BUY") {
    const recentLow =
      Math.min(
        ...m5Candles
          .slice(-8)
          .map((c) => c.low)
      );

    sl =
      recentLow -
      m5ATR * 0.2;

    tp =
      entry +
      (entry - sl) * 2;
  }

  if (signal === "SELL") {
    const recentHigh =
      Math.max(
        ...m5Candles
          .slice(-8)
          .map((c) => c.high)
      );

    sl =
      recentHigh +
      m5ATR * 0.2;

    tp =
      entry -
      (sl - entry) * 2;
  }

  return {
    symbol,
    displayName,

    price: entry,

    signal,

    score,

    h1: h1Direction,
    h1Direction,

    m15: m15Direction,
    m15Direction,

    m5: m5Direction,
    m5Direction,

    h1Structure,
    m15Structure,
    m5Structure,

    burstFader:
      burst.label,

    burst:
      burst.label,

    burstDirection:
      burst.direction,

    burstStack:
      burst.stack,

    rsi:
      Number(rsiValue.toFixed(1)),

    entry:
      signal === "WAIT"
        ? null
        : entry,

    sl:
      signal === "WAIT"
        ? null
        : sl,

    tp:
      signal === "WAIT"
        ? null
        : tp,

    updated:
      new Date().toISOString()
  };
}

/* =========================================================
   TELEGRAM
========================================================= */

async function sendTelegram(message) {
  if (
    !TELEGRAM_BOT_TOKEN ||
    !TELEGRAM_CHAT_ID
  ) {
    return;
  }

  try {
    const url =
      `https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage`;

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
  } catch (error) {
    console.error(
      "Telegram error:",
      error.message
    );
  }
}

/* =========================================================
   FORMAT ALERT
========================================================= */

function formatAlert(result) {
  const digits =
    result.price >= 1000
      ? 2
      : result.price >= 100
      ? 3
      : 4;

  return (
    `${result.signal === "BUY" ? "🟢" : "🔴"} ` +
    `STRONG ${result.signal}: ${result.displayName}\n\n` +

    `📍 Entry: ${Number(result.entry).toFixed(digits)}\n` +

    `🛑 Stop Loss: ${Number(result.sl).toFixed(digits)}\n` +

    `🎯 Take Profit: ${Number(result.tp).toFixed(digits)}\n\n` +

    `⭐ Score: ${result.score}/5\n` +

    `📊 1H: ${result.h1}\n` +
    `📊 15M: ${result.m15}\n` +
    `📊 5M: ${result.m5}\n\n` +

    `⚡ Burst Fader: ${result.burstFader}\n` +
    `RSI: ${result.rsi}\n\n` +

    `🔎 15M Structure: ${result.m15Structure}\n` +
    `🔎 5M Structure: ${result.m5Structure}`
  );
}

/* =========================================================
   SCAN ONE SYMBOL
========================================================= */

async function scanSymbol(item) {
  try {
    const h1Candles =
      await getCandles(
        item.symbol,
        H1
      );

    const m15Candles =
      await getCandles(
        item.symbol,
        M15
      );

    const m5Candles =
      await getCandles(
        item.symbol,
        M5
      );

    const result =
      analyze(
        item.symbol,
        item.displayName,
        h1Candles,
        m15Candles,
        m5Candles
      );

    return result;
  } catch (error) {
    console.error(
      `Scan error ${item.symbol}:`,
      error.message
    );

    return {
      symbol: item.symbol,
      displayName:
        item.displayName,

      price: null,

      signal: "WAIT",

      score: 0,

      h1: "NEUTRAL",
      m15: "NEUTRAL",
      m5: "NEUTRAL",

      burstFader:
        "DATA UNAVAILABLE",

      entry: null,
      sl: null,
      tp: null,

      error:
        error.message,

      updated:
        new Date().toISOString()
    };
  }
}

/* =========================================================
   SCANNER
========================================================= */

async function runScanner() {
  if (scanning) {
    return;
  }

  if (
    !connected ||
    symbols.length === 0
  ) {
    return;
  }

  scanning = true;

  try {
    const results = [];

    /*
      Sequential requests are intentional.
      This reduces pressure on the Deriv connection.
    */

    for (const item of symbols) {
      const result =
        await scanSymbol(item);

      results.push(result);

      await sleep(250);
    }

    scans = results;

    lastScan =
      new Date().toISOString();

    /*
      Telegram alerts
    */

    for (const result of results) {
      if (
        result.signal === "BUY" ||
        result.signal === "SELL"
      ) {
        const lastTime =
          lastAlertTime[
            result.symbol
          ] || 0;

        const now =
          Date.now();

        if (
          now - lastTime >=
          ALERT_COOLDOWN
        ) {
          lastAlertTime[
            result.symbol
          ] = now;

          const message =
            formatAlert(result);

          recentAlerts.unshift({
            message,
            time:
              new Date().toISOString(),
            symbol:
              result.symbol
          });

          recentAlerts =
            recentAlerts.slice(0, 20);

          await sendTelegram(
            message
          );
        }
      }
    }

    console.log(
      `Scan complete: ${results.length} Volatility indices`
    );
  } catch (error) {
    console.error(
      "Scanner error:",
      error.message
    );
  } finally {
    scanning = false;
  }
}

/* =========================================================
   START SCANNER
========================================================= */

function startScanner() {
  setTimeout(
    runScanner,
    2000
  );

  setInterval(
    runScanner,
    SCAN_INTERVAL
  );
}

/* =========================================================
   SLEEP
========================================================= */

function sleep(ms) {
  return new Promise(
    (resolve) =>
      setTimeout(resolve, ms)
  );
}

/* =========================================================
   START SERVER
========================================================= */

app.listen(PORT, "0.0.0.0", () => {
  console.log(
    `Deriv Volatility Burst Fader running on port ${PORT}`
  );

  connectDeriv();
});
