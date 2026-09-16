const express = require("express");
const path = require("path");
const WebSocket = require("ws");

const app = express();

const PORT = process.env.PORT || 3000;

/* =========================================================
   SERVER
========================================================= */

app.use(express.json());
app.use(express.static(path.join(__dirname, "public")));

app.get("/", (req, res) => {
  res.sendFile(path.join(__dirname, "public", "index.html"));
});

app.get("/health", (req, res) => {
  res.json({
    ok: true,
    online: true,
    derivConnected: state.derivConnected
  });
});

app.get("/api/status", (req, res) => {
  res.json(state);
});

/* =========================================================
   DERIV SETTINGS
========================================================= */

const DERIV_WS_URL =
  "wss://api.derivws.com/trading/v1/options/ws/public";

/*
  IMPORTANT:

  We DO NOT request 3600-second candles from Deriv.

  We request:
  - 15M candles
  - 5M candles

  Then we build the 1H candles locally from the 15M candles.

  This avoids the 1HZ15V + granularity 3600 validation problem.
*/

const M15 = 900;
const M5 = 300;

const CANDLE_COUNT = 120;

const SCAN_INTERVAL_MS = 60000;
const REQUEST_TIMEOUT_MS = 20000;

const REQUEST_DELAY_MS = 700;

const ACTIVE_SYMBOL_CACHE_MS = 10 * 60 * 1000;

const ALERT_COOLDOWN_MS = 15 * 60 * 1000;

/* =========================================================
   TELEGRAM
========================================================= */

const TELEGRAM_BOT_TOKEN =
  process.env.TELEGRAM_BOT_TOKEN;

const TELEGRAM_CHAT_ID =
  process.env.TELEGRAM_CHAT_ID;

/* =========================================================
   STATE
========================================================= */

const state = {
  online: true,
  derivConnected: false,
  markets: [],
  lastScan: null,
  error: null,
  symbolCount: 0,
  scanRunning: false
};

/* =========================================================
   DERIV CONNECTION STATE
========================================================= */

let ws = null;
let wsReady = false;
let connectPromise = null;

let requestId = 1;

const pendingRequests = new Map();

let requestQueue = Promise.resolve();

let cachedSymbols = [];
let cachedSymbolsAt = 0;

/* =========================================================
   ALERT STATE
========================================================= */

const alertState = new Map();
const burstWarningState = new Map();

/* =========================================================
   HELPERS
========================================================= */

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function safeNumber(value) {
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

function roundPrice(value, digits = 3) {
  if (!Number.isFinite(value)) return null;
  return Number(value.toFixed(digits));
}

function getDigits(symbol) {
  const s = String(symbol).toUpperCase();

  if (
    s.includes("XAU") ||
    s.includes("GOLD")
  ) {
    return 2;
  }

  return 3;
}

/* =========================================================
   DERIV CONNECTION
========================================================= */

function connectDeriv() {

  if (
    ws &&
    ws.readyState === WebSocket.OPEN &&
    wsReady
  ) {
    return Promise.resolve();
  }

  if (connectPromise) {
    return connectPromise;
  }

  connectPromise = new Promise((resolve, reject) => {

    console.log("Connecting to Deriv...");

    let settled = false;

    const socket = new WebSocket(DERIV_WS_URL);

    ws = socket;
    wsReady = false;

    const connectionTimeout = setTimeout(() => {

      if (settled) return;

      settled = true;

      try {
        socket.close();
      } catch {}

      reject(
        new Error("Deriv WebSocket connection timeout")
      );

    }, 15000);

    socket.on("open", () => {

      if (settled) return;

      clearTimeout(connectionTimeout);

      wsReady = true;
      state.derivConnected = true;
      state.error = null;

      console.log("Deriv WebSocket connected");

      settled = true;

      resolve();
    });

    socket.on("message", raw => {

      let data;

      try {
        data = JSON.parse(raw.toString());
      } catch {
        console.log(
          "DERIV MESSAGE PARSE ERROR"
        );
        return;
      }

      console.log(
        "DERIV RECV:",
        data.msg_type || "unknown",
        data.req_id || ""
      );

      if (data.error) {

        console.log(
          "DERIV API ERROR:",
          data.error.code,
          data.error.message
        );

        const reqId =
          data.req_id;

        if (
          reqId &&
          pendingRequests.has(reqId)
        ) {

          const pending =
            pendingRequests.get(reqId);

          pendingRequests.delete(reqId);

          pending.reject(
            new Error(
              `${data.error.code || "DerivError"}: ${
                data.error.message || "Unknown Deriv error"
              }`
            )
          );

          return;
        }

        /*
          New Deriv API does not guarantee echo_req.
          If there is only one pending request,
          route the error to it.
        */

        if (pendingRequests.size === 1) {

          const [onlyId, pending] =
            [...pendingRequests.entries()][0];

          pendingRequests.delete(onlyId);

          pending.reject(
            new Error(
              `${data.error.code || "DerivError"}: ${
                data.error.message || "Unknown Deriv error"
              }`
            )
          );
        }

        return;
      }

      const reqId =
        data.req_id;

      if (
        reqId &&
        pendingRequests.has(reqId)
      ) {

        const pending =
          pendingRequests.get(reqId);

        pendingRequests.delete(reqId);

        pending.resolve(data);

        return;
      }

      /*
        Fallback for responses without
        usable req_id.
      */

      if (pendingRequests.size === 1) {

        const [onlyId, pending] =
          [...pendingRequests.entries()][0];

        pendingRequests.delete(onlyId);

        pending.resolve(data);
      }
    });

    socket.on("error", error => {

      console.log(
        "Deriv WebSocket error:",
        error.message
      );

      state.derivConnected = false;
      state.error = error.message;

      if (!settled) {

        clearTimeout(connectionTimeout);

        settled = true;

        reject(error);
      }
    });

    socket.on("close", () => {

      console.log(
        "Deriv WebSocket disconnected"
      );

      wsReady = false;
      state.derivConnected = false;

      for (
        const [id, pending]
        of pendingRequests.entries()
      ) {

        pending.reject(
          new Error(
            "Deriv WebSocket disconnected"
          )
        );

        pendingRequests.delete(id);
      }

      /*
        Reconnect automatically.
      */

      setTimeout(() => {

        if (!wsReady) {
          connectDeriv().catch(() => {});
        }

      }, 5000);
    });
  });

  connectPromise =
    connectPromise.finally(() => {
      connectPromise = null;
    });

  return connectPromise;
}

/* =========================================================
   DERIV REQUEST QUEUE
========================================================= */

function derivRequest(payload) {

  /*
    IMPORTANT FIX:

    A failed promise must NOT poison
    the entire request queue.
  */

  const job =
    requestQueue
      .catch(() => {})
      .then(async () => {

        await sleep(
          REQUEST_DELAY_MS
        );

        return performDerivRequest(
          payload
        );
      });

  requestQueue =
    job.catch(() => {});

  return job;
}

/* =========================================================
   PERFORM DERIV REQUEST
========================================================= */

async function performDerivRequest(payload) {

  let attempts = 0;

  while (attempts < 3) {

    attempts++;

    try {

      await connectDeriv();

      const reqId =
        requestId++;

      const request = {
        ...payload,
        req_id: reqId
      };

      console.log(
        "DERIV SEND:",
        JSON.stringify(request)
      );

      const result =
        await new Promise(
          (resolve, reject) => {

            const timeout =
              setTimeout(() => {

                pendingRequests.delete(
                  reqId
                );

                reject(
                  new Error(
                    "Deriv request timeout"
                  )
                );

              }, REQUEST_TIMEOUT_MS);

            pendingRequests.set(
              reqId,
              {
                resolve: data => {

                  clearTimeout(timeout);

                  resolve(data);
                },

                reject: error => {

                  clearTimeout(timeout);

                  reject(error);
                }
              }
            );

            if (
              !ws ||
              ws.readyState !== WebSocket.OPEN
            ) {

              clearTimeout(timeout);

              pendingRequests.delete(
                reqId
              );

              reject(
                new Error(
                  "Deriv WebSocket not ready"
                )
              );

              return;
            }

            ws.send(
              JSON.stringify(request),
              error => {

                if (error) {

                  clearTimeout(timeout);

                  pendingRequests.delete(
                    reqId
                  );

                  reject(error);
                }
              }
            );
          }
        );

      return result;

    } catch (error) {

      console.log(
        `Deriv request attempt ${attempts} failed:`,
        error.message
      );

      if (attempts < 3) {

        await sleep(
          2000 * attempts
        );

      } else {

        throw error;
      }
    }
  }

  throw new Error(
    "Deriv request failed"
  );
}

/* =========================================================
   GET VOLATILITY SYMBOLS
========================================================= */

async function getVolatilitySymbols() {

  const now = Date.now();

  if (
    cachedSymbols.length &&
    now - cachedSymbolsAt <
      ACTIVE_SYMBOL_CACHE_MS
  ) {

    return cachedSymbols;
  }

  const data =
    await derivRequest({
      active_symbols: "brief"
    });

  const symbols =
    Array.isArray(
      data.active_symbols
    )
      ? data.active_symbols
      : [];

  const result = [];

  for (const item of symbols) {

    const symbol =
      item.symbol ||
      item.underlying_symbol ||
      item.name;

    const display =
      item.display_name ||
      item.underlying_symbol_name ||
      item.name ||
      "";

    if (!symbol) continue;

    const s =
      String(symbol).toUpperCase();

    const d =
      String(display).toUpperCase();

    const isVolatility =
      /VOLATILITY/i.test(d) ||
      /^1HZ\d+V$/i.test(s) ||
      /^V\d+$/i.test(s) ||
      /^R_(10|25|50|75|100)$/i.test(s);

    if (isVolatility) {

      result.push({
        symbol,
        display_name:
          display || symbol
      });
    }
  }

  cachedSymbols =
    result;

  cachedSymbolsAt =
    now;

  state.symbolCount =
    result.length;

  console.log(
    "Volatility symbols found:",
    result.length
  );

  return result;
}

/* =========================================================
   REQUEST 15M / 5M CANDLES
========================================================= */

async function requestCandles(
  symbol,
  granularity
) {

  /*
    IMPORTANT:

    We request only 900 and 300 seconds.

    We do NOT request 3600.
  */

  const data = await derivRequest({
  ticks_history: symbol,
  end: "latest",
  count: CANDLE_COUNT,
  style: "candles",
  granularity,
  adjust_start_time: 1
});

  if (
    data &&
    Array.isArray(data.candles)
  ) {

    return cleanCandles(
      data.candles
    );
  }

  if (
    data &&
    data.history &&
    Array.isArray(
      data.history.candles
    )
  ) {

    return cleanCandles(
      data.history.candles
    );
  }

  /*
    Fallback if Deriv returns
    history prices/times.
  */

  if (
    data &&
    data.history &&
    Array.isArray(
      data.history.prices
    ) &&
    Array.isArray(
      data.history.times
    )
  ) {

    const prices =
      data.history.prices;

    const times =
      data.history.times;

    const candles = [];

    for (
      let i = 0;
      i < Math.min(
        prices.length,
        times.length
      );
      i++
    ) {

      const price =
        safeNumber(prices[i]);

      const epoch =
        Number(times[i]);

      if (
        price === null ||
        !Number.isFinite(epoch)
      ) {
        continue;
      }

      candles.push({
        epoch,
        open: price,
        high: price,
        low: price,
        close: price
      });
    }

    return cleanCandles(
      candles
    );
  }

  throw new Error(
    `No candle data returned for ${symbol} (${granularity})`
  );
}

/* =========================================================
   CLEAN CANDLES
========================================================= */

function cleanCandles(candles) {

  const map =
    new Map();

  for (const c of candles) {

    const epoch =
      Number(c.epoch);

    const open =
      safeNumber(c.open);

    const high =
      safeNumber(c.high);

    const low =
      safeNumber(c.low);

    const close =
      safeNumber(c.close);

    if (
      !Number.isFinite(epoch) ||
      open === null ||
      high === null ||
      low === null ||
      close === null
    ) {
      continue;
    }

    map.set(
      epoch,
      {
        epoch,
        open,
        high,
        low,
        close
      }
    );
  }

  return [...map.values()]
    .sort(
      (a, b) =>
        a.epoch - b.epoch
    )
    .slice(-CANDLE_COUNT);
}

/* =========================================================
   BUILD 1H FROM 15M
========================================================= */

function buildHourlyCandles(
  m15Candles
) {

  /*
    Four 15M candles = one 1H candle.

    We group using UTC hour boundaries.
  */

  const groups =
    new Map();

  for (
    const candle of m15Candles
  ) {

    const date =
      new Date(
        candle.epoch * 1000
      );

    const year =
      date.getUTCFullYear();

    const month =
      date.getUTCMonth();

    const day =
      date.getUTCDate();

    const hour =
      date.getUTCHours();

    const bucket =
      Date.UTC(
        year,
        month,
        day,
        hour,
        0,
        0
      ) / 1000;

    if (!groups.has(bucket)) {

      groups.set(
        bucket,
        []
      );
    }

    groups
      .get(bucket)
      .push(candle);
  }

  const hourly = [];

  for (
    const [
      epoch,
      group
    ]
    of groups.entries()
  ) {

    group.sort(
      (a, b) =>
        a.epoch - b.epoch
    );

    /*
      We want a complete 1H candle.

      A complete hour should contain
      at least 4 x 15M candles.
    */

    if (group.length < 4) {
      continue;
    }

    const first =
      group[0];

    const last =
      group[group.length - 1];

    hourly.push({

      epoch,

      open:
        first.open,

      high:
        Math.max(
          ...group.map(
            c => c.high
          )
        ),

      low:
        Math.min(
          ...group.map(
            c => c.low
          )
        ),

      close:
        last.close
    });
  }

  return hourly
    .sort(
      (a, b) =>
        a.epoch - b.epoch
    )
    .slice(-60);
}

/* =========================================================
   EMA
========================================================= */

function ema(
  values,
  period
) {

  if (
    values.length < period
  ) {
    return null;
  }

  const multiplier =
    2 /
    (period + 1);

  let result = 0;

  for (
    let i = 0;
    i < period;
    i++
  ) {

    result +=
      values[i];
  }

  result /= period;

  for (
    let i = period;
    i < values.length;
    i++
  ) {

    result =
      (
        values[i] -
        result
      ) *
        multiplier +
      result;
  }

  return result;
}

/* =========================================================
   ATR
========================================================= */

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

  const trs = [];

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

    trs.push(tr);
  }

  if (
    trs.length < period
  ) {
    return null;
  }

  let value = 0;

  for (
    let i = 0;
    i < period;
    i++
  ) {

    value +=
      trs[i];
  }

  value /= period;

  for (
    let i = period;
    i < trs.length;
    i++
  ) {

    value =
      (
        value *
          (period - 1) +
        trs[i]
      ) / period;
  }

  return value;
}

/* =========================================================
   RSI
========================================================= */

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
      losses -= change;
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
      Math.max(
        change,
        0
      );

    const loss =
      Math.max(
        -change,
        0
      );

    avgGain =
      (
        avgGain *
          (period - 1) +
        gain
      ) / period;

    avgLoss =
      (
        avgLoss *
          (period - 1) +
        loss
      ) / period;
  }

  if (avgLoss === 0) {
    return 100;
  }

  const rs =
    avgGain /
    avgLoss;

  return 100 -
    100 /
      (1 + rs);
}

/* =========================================================
   DIRECTION
========================================================= */

function getDirection(
  candles
) {

  if (
    candles.length < 25
  ) {
    return "NEUTRAL";
  }

  const closes =
    candles.map(
      c => c.close
    );

  const last =
    closes[
      closes.length - 1
    ];

  const previous =
    closes[
      closes.length - 6
    ];

  const e =
    ema(
      closes,
      20
    );

  if (
    e === null
  ) {
    return "NEUTRAL";
  }

  if (
    last > e &&
    last > previous
  ) {
    return "BULLISH";
  }

  if (
    last < e &&
    last < previous
  ) {
    return "BEARISH";
  }

  return "NEUTRAL";
}

/* =========================================================
   STRUCTURE
========================================================= */

function getStructure(
  candles
) {

  if (
    candles.length < 15
  ) {
    return "NEUTRAL";
  }

  const last =
    candles[
      candles.length - 1
    ];

  const previous =
    candles.slice(
      candles.length - 11,
      candles.length - 1
    );

  const previousHigh =
    Math.max(
      ...previous.map(
        c => c.high
      )
    );

  const previousLow =
    Math.min(
      ...previous.map(
        c => c.low
      )
    );

  if (
    last.close >
    previousHigh
  ) {

    return "BULLISH BOS";
  }

  if (
    last.close <
    previousLow
  ) {

    return "BEARISH BOS";
  }

  const recent =
    candles.slice(-3);

  const bullish =
    recent.every(
      c =>
        c.close >
        c.open
    );

  const bearish =
    recent.every(
      c =>
        c.close <
        c.open
    );

  if (bullish) {
    return "BULLISH CHOCH";
  }

  if (bearish) {
    return "BEARISH CHOCH";
  }

  return "NEUTRAL";
}

/* =========================================================
   LIQUIDITY SWEEP
========================================================= */

function getLiquiditySweep(
  candles
) {

  if (
    candles.length < 12
  ) {
    return "NONE";
  }

  const last =
    candles[
      candles.length - 1
    ];

  const previous =
    candles.slice(
      candles.length - 11,
      candles.length - 1
    );

  const previousLow =
    Math.min(
      ...previous.map(
        c => c.low
      )
    );

  const previousHigh =
    Math.max(
      ...previous.map(
        c => c.high
      )
    );

  /*
    Bullish liquidity sweep:
    price takes previous low
    but closes back above it.
  */

  if (
    last.low <
      previousLow &&
    last.close >
      previousLow
  ) {

    return "BULLISH";
  }

  /*
    Bearish liquidity sweep:
    price takes previous high
    but closes back below it.
  */

  if (
    last.high >
      previousHigh &&
    last.close <
      previousHigh
  ) {

    return "BEARISH";
  }

  return "NONE";
}

/* =========================================================
   STRONG BURST FADER
========================================================= */

function getStrongBurstFader(
  candles
) {

  if (
    candles.length < 25
  ) {

    return {
      burst: "NONE",
      stack: 0,
      heat: "COOL",
      fadeBias: "NONE"
    };
  }

  const closes =
    candles.map(
      c => c.close
    );

  const basis =
    ema(
      closes,
      20
    );

  const atrValue =
    atr(
      candles,
      14
    );

  if (
    basis === null ||
    atrValue === null ||
    atrValue <= 0
  ) {

    return {
      burst: "NONE",
      stack: 0,
      heat: "COOL",
      fadeBias: "NONE"
    };
  }

  /*
    Use completed candle.
  */

  const last =
    candles[
      candles.length - 1
    ];

  const upper =
    basis +
    2 * atrValue;

  const lower =
    basis -
    2 * atrValue;

  let burst =
    "NONE";

  let stack = 0;

  if (
    last.high >
    upper
  ) {

    burst =
      "UPSIDE";

    stack =
      Math.ceil(
        (
          last.high -
          upper
        ) /
          (
            0.5 *
            atrValue
          )
      );

  } else if (
    last.low <
    lower
  ) {

    burst =
      "DOWNSIDE";

    stack =
      Math.ceil(
        (
          lower -
          last.low
        ) /
          (
            0.5 *
            atrValue
          )
      );
  }

  stack =
    Math.max(
      0,
      Math.min(
        6,
        stack
      )
    );

  let heat =
    "COOL";

  if (stack === 1) {
    heat = "WARM";
  }

  if (
    stack >= 2 &&
    stack <= 4
  ) {
    heat = "HOT";
  }

  if (
    stack >= 5
  ) {
    heat = "EXTREME";
  }

  let fadeBias =
    "NONE";

  if (
    burst === "UPSIDE"
  ) {
    fadeBias =
      "BEARISH";
  }

  if (
    burst === "DOWNSIDE"
  ) {
    fadeBias =
      "BULLISH";
  }

  return {
    burst,
    stack,
    heat,
    fadeBias,
    basis: roundPrice(
      basis,
      5
    ),
    upper: roundPrice(
      upper,
      5
    ),
    lower: roundPrice(
      lower,
      5
    )
  };
}

/* =========================================================
   BURST SUPPORT
========================================================= */

function getBurstSupport(
  burst,
  m5Structure,
  liquidity
) {

  let supportsBuy =
    false;

  let supportsSell =
    false;

  /*
    Downside burst can become
    bullish reversal support.
  */

  if (
    burst.burst ===
      "DOWNSIDE" &&
    (
      liquidity ===
        "BULLISH" ||
      m5Structure.startsWith(
        "BULLISH"
      )
    )
  ) {

    supportsBuy = true;
  }

  /*
    Upside burst can become
    bearish reversal support.
  */

  if (
    burst.burst ===
      "UPSIDE" &&
    (
      liquidity ===
        "BEARISH" ||
      m5Structure.startsWith(
        "BEARISH"
      )
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
   TELEGRAM
========================================================= */

async function sendTelegram(
  message
) {

  if (
    !TELEGRAM_BOT_TOKEN ||
    !TELEGRAM_CHAT_ID
  ) {

    console.log(
      "Telegram variables not configured."
    );

    return false;
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

          body: JSON.stringify({
            chat_id:
              TELEGRAM_CHAT_ID,

            text:
              message
          })
        }
      );

    return response.ok;

  } catch (error) {

    console.log(
      "Telegram error:",
      error.message
    );

    return false;
  }
}

/* =========================================================
   BURST WARNING
========================================================= */

async function sendBurstWarning(
  symbol,
  price,
  burst
) {

  if (
    !burst ||
    burst.stack < 2
  ) {

    burstWarningState.delete(
      symbol
    );

    return;
  }

  const stage =
    burst.stack >= 3
      ? "STRONG"
      : "EARLY";

  const previous =
    burstWarningState.get(
      symbol
    );

  const direction =
    burst.burst ===
      "UPSIDE"
      ? "UPSIDE BURST"
      : "DOWNSIDE BURST";

  const shouldSend =
    !previous ||
    previous.stage !== stage ||
    previous.direction !==
      direction;

  if (!shouldSend) {
    return;
  }

  burstWarningState.set(
    symbol,
    {
      stage,
      direction
    }
  );

  const message =

`⚠️ ${stage} BURST WARNING

📊 ${symbol}

💥 ${direction}
🔥 Heat: ${burst.heat}
📶 Stack: ${burst.stack}

Possible reversal developing.

💰 Price: ${price}

⏳ WAIT FOR CONFIRMATION

This is a warning, NOT an entry signal.`;

  await sendTelegram(
    message
  );
}

/* =========================================================
   TRADE ALERT
========================================================= */

async function sendTradeAlert(
  result
) {

  if (
    result.signal ===
      "WAIT"
  ) {
    return;
  }

  const now =
    Date.now();

  const previous =
    alertState.get(
      result.symbol
    );

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
    result.signal ===
      "BUY"
      ? "🟢"
      : "🔴";

  const message =

`${emoji} STRONG ${result.signal}: ${result.symbol}

📍 Entry: ${result.entry}
🛑 Stop Loss: ${result.sl}
🎯 Take Profit: ${result.tp}

⭐ Score: ${result.score}/5

📊 1H: ${result.h1Direction}
📊 15M: ${result.m15Direction}
📊 5M: ${result.m5Direction}

RSI: ${result.rsi}

🔎 SMC: ${result.m15Structure}
📈 Structure: ${result.m5Structure}

💧 Liquidity: ${result.liquidity}

💥 Burst: ${result.burst.burst}
🔥 Heat: ${result.burst.heat}
📶 Stack: ${result.burst.stack}

⚠️ Scanner alert only — no automatic trade execution.`;

  await sendTelegram(
    message
  );
}

/* =========================================================
   ANALYZE ONE SYMBOL
========================================================= */

async function analyzeSymbol(
  symbol,
  displayName
) {

  console.log(
    `Analyzing ${symbol}...`
  );

  /*
    ONLY TWO REQUESTS:

    1. 15M
    2. 5M

    1H is built locally.
  */

  const m15Candles =
    await requestCandles(
      symbol,
      M15
    );

  /*
    Small delay between
    requests to Deriv.
  */

  await sleep(500);

  const m5Candles =
    await requestCandles(
      symbol,
      M5
    );

  /*
    Build H1 locally.
  */

  const h1Candles =
    buildHourlyCandles(
      m15Candles
    );

  if (
    h1Candles.length < 25
  ) {

    throw new Error(
      `Not enough complete H1 candles for ${symbol}`
    );
  }

  if (
    m15Candles.length < 25
  ) {

    throw new Error(
      `Not enough 15M candles for ${symbol}`
    );
  }

  if (
    m5Candles.length < 25
  ) {

    throw new Error(
      `Not enough 5M candles for ${symbol}`
    );
  }

  /* =======================================================
     ANALYSIS
  ======================================================= */

  const h1Direction =
    getDirection(
      h1Candles
    );

  const m15Direction =
    getDirection(
      m15Candles
    );

  const m5Direction =
    getDirection(
      m5Candles
    );

  const m15Structure =
    getStructure(
      m15Candles
    );

  const m5Structure =
    getStructure(
      m5Candles
    );

  const liquidity =
    getLiquiditySweep(
      m5Candles
    );

  const burst =
    getStrongBurstFader(
      m5Candles
    );

  const burstSupport =
    getBurstSupport(
      burst,
      m5Structure,
      liquidity
    );

  const rsiValue =
    rsi(
      m5Candles,
      14
    );

  /* =======================================================
     SCORE

     Maximum displayed score = 5.

     Burst Fader is SUPPORTING evidence,
     not a mandatory gate.
  ======================================================= */

  let score = 0;

  if (
    h1Direction !==
    "NEUTRAL"
  ) {

    score++;
  }

  if (
    m15Direction ===
    h1Direction
  ) {

    score++;
  }

  if (
    h1Direction ===
      "BULLISH" &&
    m15Structure.startsWith(
      "BULLISH"
    )
  ) {

    score++;
  }

  if (
    h1Direction ===
      "BEARISH" &&
    m15Structure.startsWith(
      "BEARISH"
    )
  ) {

    score++;
  }

  if (
    h1Direction ===
      "BULLISH" &&
    m5Structure.startsWith(
      "BULLISH"
    )
  ) {

    score++;
  }

  if (
    h1Direction ===
      "BEARISH" &&
    m5Structure.startsWith(
      "BEARISH"
    )
  ) {

    score++;
  }

  if (
    liquidity !==
    "NONE"
  ) {

    score++;
  }

  if (
    burstSupport.supportsBuy ||
    burstSupport.supportsSell
  ) {

    score++;
  }

  /*
    Keep dashboard score at 5 maximum.
  */

  score =
    Math.min(
      5,
      score
    );

  /* =======================================================
     SIGNAL
  ======================================================= */

  let signal =
    "WAIT";

  const bullishSetup =
    h1Direction ===
      "BULLISH" &&

    m15Direction ===
      "BULLISH" &&

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

  const bearishSetup =
    h1Direction ===
      "BEARISH" &&

    m15Direction ===
      "BEARISH" &&

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

  if (
    bullishSetup
  ) {

    signal =
      "BUY";
  }

  if (
    bearishSetup
  ) {

    signal =
      "SELL";
  }

  /* =======================================================
     PRICE / SL / TP
  ======================================================= */

  const lastM5 =
    m5Candles[
      m5Candles.length - 1
    ];

  const entry =
    lastM5.close;

  const atrValue =
    atr(
      m5Candles,
      14
    ) || 0;

  const recent =
    m5Candles.slice(-8);

  let sl = null;
  let tp = null;

  if (
    signal ===
    "BUY"
  ) {

    const lowest =
      Math.min(
        ...recent.map(
          c => c.low
        )
      );

    sl =
      lowest -
      atrValue * 0.2;

    const risk =
      entry - sl;

    tp =
      entry +
      risk * 2;
  }

  if (
    signal ===
    "SELL"
  ) {

    const highest =
      Math.max(
        ...recent.map(
          c => c.high
        )
      );

    sl =
      highest +
      atrValue * 0.2;

    const risk =
      sl - entry;

    tp =
      entry -
      risk * 2;
  }

  const digits =
    getDigits(symbol);

  const result = {

    symbol,

    displayName,

    signal,

    score,

    price:
      roundPrice(
        entry,
        digits
      ),

    entry:
      roundPrice(
        entry,
        digits
      ),

    sl:
      sl === null
        ? null
        : roundPrice(
            sl,
            digits
          ),

    tp:
      tp === null
        ? null
        : roundPrice(
            tp,
            digits
          ),

    h1Direction,

    m15Direction,

    m5Direction,

    m15Structure,

    m5Structure,

    liquidity,

    rsi:
      rsiValue === null
        ? null
        : Number(
            rsiValue.toFixed(1)
          ),

    burst,

    burstSupport,

    candles: {
      h1:
        h1Candles.length,

      m15:
        m15Candles.length,

      m5:
        m5Candles.length
    },

    updatedAt:
      new Date().toISOString()
  };

  /*
    Burst warning is independent
    from normal trade signals.
  */

  await sendBurstWarning(
    symbol,
    result.price,
    burst
  );

  /*
    Trade alert.
  */

  await sendTradeAlert(
    result
  );

  return result;
}

/* =========================================================
   SCAN ALL MARKETS
========================================================= */

async function scanMarkets() {

  if (
    state.scanRunning
  ) {

    console.log(
      "Scan already running. Skipping."
    );

    return;
  }

  state.scanRunning = true;

  console.log(
    "=============================="
  );

  console.log(
    "SCAN START"
  );

  try {

    const symbols =
      await getVolatilitySymbols();

    const results = [];

    /*
      Sequential scanning keeps API
      usage controlled.
    */

    for (
      const item of symbols
    ) {

      try {

        const result =
          await analyzeSymbol(
            item.symbol,
            item.display_name
          );

        results.push(
          result
        );

        console.log(
          `${item.symbol}: ${result.signal} ${result.score}/5`
        );

      } catch (error) {

        console.log(
          `SCAN ERROR ${item.symbol}:`,
          error.message
        );

        results.push({

          symbol:
            item.symbol,

          displayName:
            item.display_name,

          signal:
            "WAIT",

          score:
            0,

          price:
            null,

          entry:
            null,

          sl:
            null,

          tp:
            null,

          h1Direction:
            "NEUTRAL",

          m15Direction:
            "NEUTRAL",

          m5Direction:
            "NEUTRAL",

          m15Structure:
            "NEUTRAL",

          m5Structure:
            "NEUTRAL",

          liquidity:
            "NONE",

          rsi:
            null,

          burst: {
            burst:
              "NONE",

            stack:
              0,

            heat:
              "COOL",

            fadeBias:
              "NONE"
          },

          error:
            error.message,

          updatedAt:
            new Date().toISOString()
        });
      }

      /*
        Extra pacing between symbols.
      */

      await sleep(500);
    }

    state.markets =
      results;

    state.lastScan =
      new Date().toISOString();

    state.error =
      null;

    const buys =
      results.filter(
        r =>
          r.signal ===
          "BUY"
      ).length;

    const sells =
      results.filter(
        r =>
          r.signal ===
          "SELL"
      ).length;

    const waits =
      results.filter(
        r =>
          r.signal ===
          "WAIT"
      ).length;

    console.log(
      `SCAN COMPLETE | BUY: ${buys} | SELL: ${sells} | WAIT: ${waits}`
    );

  } catch (error) {

    state.error =
      error.message;

    console.log(
      "SCAN ERROR:",
      error.message
    );

  } finally {

    state.scanRunning =
      false;

    console.log(
      "=============================="
    );
  }
}

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
      "Dashboard:",
      "/"
    );

    console.log(
      "Health:",
      "/health"
    );

    console.log(
      "API:",
      "/api/status"
    );

    console.log(
      "Strategy:",
      "1H → 15M → 5M → Strong Burst Fader"
    );

    console.log(
      "1H candles are built locally from 15M candles."
    );

    /*
      Connect and scan.
    */

    connectDeriv()
      .then(() => {

        scanMarkets()
          .catch(error => {

            console.log(
              "Initial scan error:",
              error.message
            );
          });

      })
      .catch(error => {

        console.log(
          "Initial Deriv connection failed:",
          error.message
        );
      });

    /*
      Continue scanning every minute.
    */

    setInterval(
      () => {

        scanMarkets()
          .catch(error => {

            console.log(
              "Scheduled scan error:",
              error.message
            );
          });

      },
      SCAN_INTERVAL_MS
    );
  }
);
