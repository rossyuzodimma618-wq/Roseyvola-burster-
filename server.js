const express = require("express");
const path = require("path");
const WebSocket = require("ws");

const app = express();
const PORT = process.env.PORT || 3000;

app.use(express.json());
app.use(express.static(path.join(__dirname, "public")));

const DERIV_WS_URL =
  "wss://api.derivws.com/trading/v1/options/ws/public";

const H1 = 3600;
const M15 = 900;
const M5 = 300;

const CANDLE_COUNT = 120;

const SCAN_INTERVAL_MS = 60000;
const REQUEST_TIMEOUT_MS = 20000;
const REQUEST_DELAY_MS = 700;

const ACTIVE_SYMBOL_CACHE_MS = 10 * 60 * 1000;
const ALERT_COOLDOWN_MS = 15 * 60 * 1000;

const TELEGRAM_BOT_TOKEN =
  process.env.TELEGRAM_BOT_TOKEN;

const TELEGRAM_CHAT_ID =
  process.env.TELEGRAM_CHAT_ID;


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
  error: null,
  symbolCount: 0,
  scanRunning: false
};

const alertState = new Map();
const burstWarningState = new Map();

let ws = null;
let wsReady = false;
let connectPromise = null;

let requestId = 1;
let requestQueue = Promise.resolve();

let cachedSymbols = [];
let cachedSymbolsAt = 0;

const pendingRequests = new Map();


/*
=========================================================
HELPERS
=========================================================
*/

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function safeNumber(value) {
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

function round(value, decimals = 5) {
  const n = Number(value);

  if (!Number.isFinite(n)) {
    return null;
  }

  return Number(n.toFixed(decimals));
}

function getDecimals(symbol) {
  if (/1HZ|R_/i.test(symbol)) {
    return 2;
  }

  return 2;
}

function normalizeSymbol(item) {
  return (
    item?.underlying_symbol ||
    item?.symbol ||
    item?.underlying ||
    ""
  );
}

function normalizeName(item) {
  return (
    item?.underlying_symbol_name ||
    item?.display_name ||
    item?.name ||
    ""
  );
}


/*
=========================================================
DERIV CONNECTION
=========================================================
*/

function closeDeriv() {
  wsReady = false;
  state.derivConnected = false;

  if (ws) {
    try {
      ws.removeAllListeners();
      ws.close();
    } catch (_) {}
  }

  ws = null;
}

function rejectAllPending(error) {
  for (const [reqId, pending] of pendingRequests.entries()) {
    clearTimeout(pending.timeout);

    pending.reject(error);

    pendingRequests.delete(reqId);
  }
}


/*
New Deriv API does not guarantee echo_req.

Because this bot sends only one request at a time,
if there is no req_id we can safely use the only
pending request.
*/

function findPending(reqId) {

  if (
    reqId != null &&
    pendingRequests.has(Number(reqId))
  ) {
    return [
      Number(reqId),
      pendingRequests.get(Number(reqId))
    ];
  }

  if (pendingRequests.size === 1) {

    const first =
      pendingRequests.entries().next().value;

    if (first) {
      return first;
    }
  }

  return null;
}


function connectDeriv() {

  if (
    wsReady &&
    ws &&
    ws.readyState === WebSocket.OPEN
  ) {
    return Promise.resolve();
  }

  if (connectPromise) {
    return connectPromise;
  }

  connectPromise = new Promise((resolve, reject) => {

    let settled = false;

    console.log("Connecting to Deriv...");

    const socket =
      new WebSocket(DERIV_WS_URL);

    ws = socket;


    const connectionTimeout =
      setTimeout(() => {

        if (settled) {
          return;
        }

        settled = true;

        try {
          socket.close();
        } catch (_) {}

        wsReady = false;
        state.derivConnected = false;
        connectPromise = null;

        reject(
          new Error(
            "Deriv WebSocket connection timeout"
          )
        );

      }, 15000);


    socket.on("open", () => {

      clearTimeout(connectionTimeout);

      wsReady = true;
      state.derivConnected = true;
      state.error = null;

      console.log(
        "Deriv WebSocket connected"
      );

      if (!settled) {

        settled = true;
        connectPromise = null;

        resolve();
      }
    });


    socket.on("message", raw => {

      let data;

      try {
        data = JSON.parse(
          raw.toString()
        );
      } catch (error) {

        console.log(
          "Deriv returned invalid JSON"
        );

        return;
      }


      console.log(
        "DERIV RECV:",
        data.msg_type || "unknown",
        data.req_id ?? "-"
      );


      /*
      API ERROR
      */

      if (data.error) {

        console.log(
          "DERIV API ERROR:",
          data.error.code ||
            "Error",
          data.error.message ||
            "Unknown error"
        );


        const matched =
          findPending(data.req_id);


        if (matched) {

          const [
            reqId,
            pending
          ] = matched;

          clearTimeout(
            pending.timeout
          );

          pendingRequests.delete(
            reqId
          );


          pending.reject(
            new Error(
              `${
                data.error.code ||
                "DerivError"
              }: ${
                data.error.message ||
                "Deriv API error"
              }`
            )
          );
        }

        return;
      }


      /*
      NORMAL RESPONSE
      */

      const matched =
        findPending(data.req_id);


      if (matched) {

        const [
          reqId,
          pending
        ] = matched;

        clearTimeout(
          pending.timeout
        );

        pendingRequests.delete(
          reqId
        );

        pending.resolve(data);
      }

    });


    socket.on("error", error => {

      console.log(
        "Deriv WebSocket error:",
        error.message
      );

      state.derivConnected = false;
      wsReady = false;

      state.error =
        error.message;


      if (!settled) {

        clearTimeout(
          connectionTimeout
        );

        settled = true;
        connectPromise = null;

        reject(error);
      }
    });


    socket.on("close", (code, reason) => {

      clearTimeout(
        connectionTimeout
      );

      wsReady = false;
      state.derivConnected = false;

      console.log(
        "Deriv WebSocket disconnected:",
        code,
        reason?.toString() || ""
      );


      rejectAllPending(
        new Error(
          "Deriv WebSocket disconnected"
        )
      );


      if (ws === socket) {
        ws = null;
      }

      connectPromise = null;
    });

  });

  return connectPromise;
}


/*
=========================================================
DERIV REQUEST QUEUE
=========================================================
*/

function derivRequest(payload) {

  /*
  Important:
  A failed request must NOT poison the
  entire request queue.
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


async function performDerivRequest(payload) {

  let lastError = null;


  for (
    let attempt = 1;
    attempt <= 3;
    attempt++
  ) {

    try {

      await connectDeriv();


      if (
        !ws ||
        ws.readyState !==
          WebSocket.OPEN
      ) {

        throw new Error(
          "Deriv WebSocket not ready"
        );
      }


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


      const response =
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
                timeout,
                resolve,
                reject
              }
            );


            try {

              ws.send(
                JSON.stringify(request),
                error => {

                  if (error) {

                    clearTimeout(
                      timeout
                    );

                    pendingRequests.delete(
                      reqId
                    );

                    reject(error);
                  }

                }
              );

            } catch (error) {

              clearTimeout(
                timeout
              );

              pendingRequests.delete(
                reqId
              );

              reject(error);
            }

          }
        );


      return response;

    } catch (error) {

      lastError = error;

      console.log(
        `Deriv request attempt ${attempt} failed:`,
        error.message
      );


      if (attempt < 3) {

        await sleep(
          1500 * attempt
        );


        if (
          !wsReady ||
          !ws ||
          ws.readyState !==
            WebSocket.OPEN
        ) {

          closeDeriv();
        }
      }
    }
  }


  throw (
    lastError ||
    new Error(
      "Deriv request failed"
    )
  );
}


/*
=========================================================
GET VOLATILITY SYMBOLS
=========================================================
*/

async function getVolatilitySymbols() {

  const now = Date.now();


  if (
    cachedSymbols.length > 0 &&
    now - cachedSymbolsAt <
      ACTIVE_SYMBOL_CACHE_MS
  ) {

    return cachedSymbols;
  }


  const data =
    await derivRequest({
      active_symbols: "brief"
    });


  const list =
    Array.isArray(
      data.active_symbols
    )
      ? data.active_symbols
      : [];


  const symbols =
    list
      .map(item => ({
        symbol:
          normalizeSymbol(item),

        name:
          normalizeName(item),

        pipSize:
          safeNumber(
            item?.pip_size ??
            item?.pip
          )
      }))

      .filter(
        item => item.symbol
      )

      .filter(item => {

        const text =
          `${item.symbol} ${item.name}`
            .toLowerCase();


        return (
          text.includes(
            "volatility"
          ) ||

          /^1hz\d+v$/i.test(
            item.symbol
          ) ||

          /^v\d+$/i.test(
            item.symbol
          ) ||

          /^r_(10|25|50|75|100)$/i.test(
            item.symbol
          )
        );

      });


  /*
  Remove duplicates.
  */

  const unique = [];
  const seen = new Set();


  for (const item of symbols) {

    if (
      seen.has(item.symbol)
    ) {
      continue;
    }

    seen.add(
      item.symbol
    );

    unique.push(item);
  }


  cachedSymbols =
    unique;

  cachedSymbolsAt =
    now;


  console.log(
    `Volatility symbols found: ${cachedSymbols.length}`
  );


  console.log(
    cachedSymbols
      .map(x => x.symbol)
      .join(", ")
  );


  return cachedSymbols;
}


/*
=========================================================
CANDLE DATA
=========================================================
*/

async function requestCandles(
  symbol,
  granularity
) {

  const data =
    await derivRequest({

      ticks_history:
        symbol,

      end:
        "latest",

      count:
        CANDLE_COUNT,

      style:
        "candles",

      granularity,

      subscribe:
        0
    });


  let candles = [];


  if (
    Array.isArray(
      data.candles
    )
  ) {

    candles =
      data.candles;

  } else if (
    data.history &&
    Array.isArray(
      data.history.candles
    )
  ) {

    candles =
      data.history.candles;
  }


  if (
    candles.length > 20
  ) {

    return cleanCandles(
      candles
    );
  }


  /*
  Fallback:
  If Deriv returns tick history,
  convert it into simple candles.

  */

  if (
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


    const pseudo =
      prices.map(
        (price, index) => ({

          epoch:
            Number(
              times[index]
            ),

          open:
            Number(price),

          high:
            Number(price),

          low:
            Number(price),

          close:
            Number(price)

        })
      );


    if (
      pseudo.length > 20
    ) {

      return cleanCandles(
        pseudo
      );
    }
  }


  throw new Error(
    `No candle data for ${symbol} (${granularity})`
  );
}


function cleanCandles(candles) {

  const cleaned =
    candles

      .map(c => ({

        epoch:
          Number(
            c.epoch ??
            c.time
          ),

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
        Number.isFinite(
          c.epoch
        ) &&

        Number.isFinite(
          c.open
        ) &&

        Number.isFinite(
          c.high
        ) &&

        Number.isFinite(
          c.low
        ) &&

        Number.isFinite(
          c.close
        )
      )

      .sort(
        (a, b) =>
          a.epoch -
          b.epoch
      );


  const unique = [];
  const seen = new Set();


  for (const candle of cleaned) {

    if (
      seen.has(
        candle.epoch
      )
    ) {
      continue;
    }

    seen.add(
      candle.epoch
    );

    unique.push(
      candle
    );
  }


  return unique.slice(
    -CANDLE_COUNT
  );
}


/*
=========================================================
EMA
=========================================================
*/

function ema(
  values,
  period = 20
) {

  if (
    values.length < period
  ) {
    return null;
  }


  const k =
    2 /
    (period + 1);


  let result =
    values
      .slice(
        0,
        period
      )
      .reduce(
        (a, b) =>
          a + b,
        0
      ) /
    period;


  for (
    let i = period;
    i < values.length;
    i++
  ) {

    result =
      values[i] * k +
      result * (1 - k);
  }


  return result;
}


/*
=========================================================
ATR
=========================================================
*/

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


  let result =
    trs
      .slice(
        0,
        period
      )
      .reduce(
        (a, b) =>
          a + b,
        0
      ) /
    period;


  for (
    let i = period;
    i < trs.length;
    i++
  ) {

    result =
      (
        result *
          (period - 1) +
        trs[i]
      ) /
      period;
  }


  return result;
}


/*
=========================================================
RSI
=========================================================
*/

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


    if (
      change >= 0
    ) {

      gains += change;

    } else {

      losses +=
        Math.abs(change);
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
      ) /
      period;


    avgLoss =
      (
        avgLoss *
          (period - 1) +
        loss
      ) /
      period;
  }


  if (
    avgLoss === 0
  ) {
    return 100;
  }


  const rs =
    avgGain /
    avgLoss;


  return (
    100 -
    100 /
      (1 + rs)
  );
}


/*
=========================================================
DIRECTION
=========================================================
*/

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


  const e20 =
    ema(
      closes,
      20
    );


  const old =
    closes[
      closes.length - 6
    ];


  if (
    !Number.isFinite(e20)
  ) {
    return "NEUTRAL";
  }


  if (
    last > e20 &&
    last > old
  ) {

    return "BULLISH";
  }


  if (
    last < e20 &&
    last < old
  ) {

    return "BEARISH";
  }


  return "NEUTRAL";
}


/*
=========================================================
STRUCTURE
=========================================================
*/

function getStructure(
  candles
) {

  if (
    candles.length < 20
  ) {
    return "NEUTRAL";
  }


  const last =
    candles[
      candles.length - 1
    ];


  const previous =
    candles.slice(
      -11,
      -1
    );


  const highest =
    Math.max(
      ...previous.map(
        c => c.high
      )
    );


  const lowest =
    Math.min(
      ...previous.map(
        c => c.low
      )
    );


  if (
    last.close >
    highest
  ) {

    return "BULLISH BOS";
  }


  if (
    last.close <
    lowest
  ) {

    return "BEARISH BOS";
  }


  const last3 =
    candles.slice(-3);


  const bullish3 =
    last3.every(
      c =>
        c.close >
        c.open
    );


  const bearish3 =
    last3.every(
      c =>
        c.close <
        c.open
    );


  if (bullish3) {
    return "BULLISH CHOCH";
  }


  if (bearish3) {
    return "BEARISH CHOCH";
  }


  return "NEUTRAL";
}


/*
=========================================================
LIQUIDITY SWEEP
=========================================================
*/

function getLiquiditySweep(
  candles
) {

  if (
    candles.length < 15
  ) {
    return "NONE";
  }


  const last =
    candles[
      candles.length - 1
    ];


  const previous =
    candles.slice(
      -11,
      -1
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


  if (
    last.low <
      previousLow &&
    last.close >
      previousLow
  ) {

    return "BULLISH LIQUIDITY SWEEP";
  }


  if (
    last.high >
      previousHigh &&
    last.close <
      previousHigh
  ) {

    return "BEARISH LIQUIDITY SWEEP";
  }


  return "NONE";
}


/*
=========================================================
STRONG BURST FADER
=========================================================
*/

function getBurstFader(
  candles
) {

  if (
    candles.length < 25
  ) {

    return {
      direction: "NONE",
      stack: 0,
      heat: "COOL",
      fadeBias: "NONE"
    };
  }


  const completed =
    candles[
      candles.length - 1
    ];


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
    !Number.isFinite(
      basis
    ) ||
    !Number.isFinite(
      atrValue
    ) ||
    atrValue <= 0
  ) {

    return {
      direction: "NONE",
      stack: 0,
      heat: "COOL",
      fadeBias: "NONE"
    };
  }


  const upper =
    basis +
    2 * atrValue;


  const lower =
    basis -
    2 * atrValue;


  let direction =
    "NONE";

  let stack = 0;


  if (
    completed.high >
    upper
  ) {

    direction =
      "UP";


    stack =
      Math.ceil(
        (
          completed.high -
          upper
        ) /
        (
          0.5 *
          atrValue
        )
      );
  }


  if (
    completed.low <
    lower
  ) {

    const downStack =
      Math.ceil(
        (
          lower -
          completed.low
        ) /
        (
          0.5 *
          atrValue
        )
      );


    if (
      direction === "NONE" ||
      downStack > stack
    ) {

      direction =
        "DOWN";

      stack =
        downStack;
    }
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


  if (
    stack === 1
  ) {
    heat = "WARM";
  }


  if (
    stack >= 2 &&
    stack <= 3
  ) {
    heat = "HOT";
  }


  if (
    stack >= 4
  ) {
    heat = "EXTREME";
  }


  let fadeBias =
    "NONE";


  if (
    direction === "UP" &&
    stack >= 2
  ) {

    fadeBias =
      "BEARISH";
  }


  if (
    direction === "DOWN" &&
    stack >= 2
  ) {

    fadeBias =
      "BULLISH";
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

  const supportsBuy =
    burst.direction ===
      "DOWN" &&
    (
      liquidity ===
        "BULLISH LIQUIDITY SWEEP" ||

      m5Structure.startsWith(
        "BULLISH"
      )
    );


  const supportsSell =
    burst.direction ===
      "UP" &&
    (
      liquidity ===
        "BEARISH LIQUIDITY SWEEP" ||

      m5Structure.startsWith(
        "BEARISH"
      )
    );


  return {
    supportsBuy,
    supportsSell
  };
}


/*
=========================================================
TELEGRAM
=========================================================
*/

async function sendTelegram(
  text
) {

  if (
    !TELEGRAM_BOT_TOKEN ||
    !TELEGRAM_CHAT_ID
  ) {

    return false;
  }


  try {

    const response =
      await fetch(
        `https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage`,
        {
          method:
            "POST",

          headers: {
            "Content-Type":
              "application/json"
          },

          body:
            JSON.stringify({
              chat_id:
                TELEGRAM_CHAT_ID,

              text
            })
        }
      );


    if (
      !response.ok
    ) {

      const body =
        await response.text();

      console.log(
        "Telegram error:",
        response.status,
        body
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
BURST WARNING
=========================================================
*/

async function sendBurstWarning(
  symbol,
  burst,
  price
) {

  if (
    !TELEGRAM_BOT_TOKEN ||
    !TELEGRAM_CHAT_ID
  ) {
    return;
  }


  if (
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


  if (
    previous &&
    previous.stage === stage &&
    previous.direction ===
      burst.direction
  ) {

    return;
  }


  burstWarningState.set(
    symbol,
    {
      stage,
      direction:
        burst.direction
    }
  );


  const text =
`⚠️ ${stage} BURST WARNING

📊 ${symbol}

💥 ${
  burst.direction === "UP"
    ? "UPSIDE BURST"
    : "DOWNSIDE BURST"
}

🔥 Heat: ${burst.heat}
📶 Stack: ${burst.stack}/6

Possible reversal detected.

💰 Price: ${price}

⏳ WAIT FOR CONFIRMATION

5M liquidity sweep
→ BOS/CHoCH
→ retracement
→ confirmation

⚠️ This is a warning,
NOT an entry signal.`;


  await sendTelegram(
    text
  );
}


/*
=========================================================
TRADE ALERT
=========================================================
*/

async function sendTradeAlert(
  symbol,
  analysis
) {

  if (
    !TELEGRAM_BOT_TOKEN ||
    !TELEGRAM_CHAT_ID
  ) {

    return;
  }


  const now =
    Date.now();


  const lastAlert =
    alertState.get(
      symbol
    ) || 0;


  if (
    now - lastAlert <
    ALERT_COOLDOWN_MS
  ) {

    return;
  }


  alertState.set(
    symbol,
    now
  );


  const decimals =
    getDecimals(symbol);


  const text =
`${analysis.signal === "BUY" ? "🟢" : "🔴"} STRONG ${analysis.signal}: ${symbol}

📍 Entry: ${analysis.entry.toFixed(decimals)}
🛑 Stop Loss: ${analysis.stopLoss.toFixed(decimals)}
🎯 Take Profit: ${analysis.takeProfit.toFixed(decimals)}

⭐ Score: ${analysis.score}/5

📊 1H: ${analysis.h1Direction}
📊 15M: ${analysis.m15Direction}
📊 5M: ${analysis.m5Direction}

🔎 15M Structure: ${analysis.m15Structure}
🔎 5M Structure: ${analysis.m5Structure}

💧 Liquidity: ${analysis.liquidity}

💥 Burst: ${analysis.burst.direction}
🔥 Heat: ${analysis.burst.heat}
📶 Burst Stack: ${analysis.burst.stack}/6

RSI: ${
  analysis.rsi !== null
    ? analysis.rsi.toFixed(1)
    : "N/A"
}

⚠️ Scanner/alert only.
No automatic trading.`;


  await sendTelegram(
    text
  );
}


/*
=========================================================
FULL MARKET ANALYSIS
=========================================================
*/

async function analyzeSymbol(
  symbolInfo
) {

  const symbol =
    symbolInfo.symbol;


  console.log(
    `Analyzing ${symbol}...`
  );


  /*
  1H
  */

  const h1 =
    await requestCandles(
      symbol,
      H1
    );


  await sleep(250);


  /*
  15M
  */

  const m15 =
    await requestCandles(
      symbol,
      M15
    );


  await sleep(250);


  /*
  5M
  */

  const m5 =
    await requestCandles(
      symbol,
      M5
    );


  if (
    h1.length < 30 ||
    m15.length < 30 ||
    m5.length < 30
  ) {

    throw new Error(
      `Not enough candles for ${symbol}`
    );
  }


  /*
  DIRECTIONS
  */

  const h1Direction =
    getDirection(h1);

  const m15Direction =
    getDirection(m15);

  const m5Direction =
    getDirection(m5);


  /*
  STRUCTURE
  */

  const m15Structure =
    getStructure(m15);

  const m5Structure =
    getStructure(m5);


  /*
  LIQUIDITY
  */

  const liquidity =
    getLiquiditySweep(m5);


  /*
  BURST FADER
  */

  const burst =
    getBurstFader(m5);


  const burstSupport =
    getBurstSupport(
      burst,
      liquidity,
      m5Structure
    );


  /*
  SCORE
  */

  let score = 0;


  if (
    h1Direction !==
    "NEUTRAL"
  ) {
    score++;
  }


  if (
    m15Direction !==
      "NEUTRAL" &&
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
    liquidity !== "NONE"
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
  Keep the original 5/5 scoring format.
  */

  score =
    Math.min(
      5,
      score
    );


  /*
  BUY
  */

  const buy =
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


  /*
  SELL
  */

  const sell =
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


  let signal =
    "WAIT";


  if (buy) {
    signal =
      "BUY";
  }


  if (sell) {
    signal =
      "SELL";
  }


  /*
  PRICE
  */

  const lastM5 =
    m5[
      m5.length - 1
    ];


  const currentPrice =
    lastM5.close;


  const decimals =
    getDecimals(symbol);


  const atrValue =
    atr(
      m5,
      14
    ) || 0;


  let entry =
    currentPrice;

  let stopLoss =
    currentPrice;

  let takeProfit =
    currentPrice;


  /*
  BUY SL / TP
  */

  if (
    signal === "BUY"
  ) {

    const recentLow =
      Math.min(
        ...m5
          .slice(-8)
          .map(
            c => c.low
          )
      );


    entry =
      currentPrice;


    stopLoss =
      recentLow -
      atrValue * 0.2;


    const risk =
      entry -
      stopLoss;


    if (risk > 0) {

      takeProfit =
        entry +
        risk * 2;
    }
  }


  /*
  SELL SL / TP
  */

  if (
    signal === "SELL"
  ) {

    const recentHigh =
      Math.max(
        ...m5
          .slice(-8)
          .map(
            c => c.high
          )
      );


    entry =
      currentPrice;


    stopLoss =
      recentHigh +
      atrValue * 0.2;


    const risk =
      stopLoss -
      entry;


    if (risk > 0) {

      takeProfit =
        entry -
        risk * 2;
    }
  }


  const rsiValue =
    rsi(
      m5,
      14
    );


  const result = {

    symbol,

    price:
      round(
        currentPrice,
        decimals
      ),

    signal,

    score,


    h1Direction,

    m15Direction,

    m5Direction,


    m15Structure,

    m5Structure,


    liquidity,


    burst: {

      direction:
        burst.direction,

      stack:
        burst.stack,

      heat:
        burst.heat,

      fadeBias:
        burst.fadeBias
    },


    rsi:
      rsiValue === null
        ? null
        : round(
            rsiValue,
            1
          ),


    entry:
      round(
        entry,
        decimals
      ),

    stopLoss:
      round(
        stopLoss,
        decimals
      ),

    takeProfit:
      round(
        takeProfit,
        decimals
      ),


    updatedAt:
      new Date()
        .toISOString()
  };


  /*
  BURST WARNING
  */

  await sendBurstWarning(
    symbol,
    burst,
    result.price
  );


  /*
  TRADE ALERT
  */

  if (
    signal === "BUY" ||
    signal === "SELL"
  ) {

    await sendTradeAlert(
      symbol,
      result
    );
  }


  console.log(
    `${symbol}: ${signal} | score ${score}/5 | H1 ${h1Direction} | 15M ${m15Direction} | 5M ${m5Direction} | burst ${burst.direction} ${burst.stack}/6`
  );


  return result;
}


/*
=========================================================
SCANNER
=========================================================
*/

let scanInProgress =
  false;


async function scanMarkets() {

  if (
    scanInProgress
  ) {

    console.log(
      "SCAN SKIPPED: previous scan still running"
    );

    return;
  }


  scanInProgress =
    true;

  state.scanRunning =
    true;

  state.error =
    null;


  console.log(
    "======================================"
  );

  console.log(
    "SCAN START"
  );

  console.log(
    "======================================"
  );


  try {

    const symbols =
      await getVolatilitySymbols();


    state.symbolCount =
      symbols.length;


    const results = [];


    for (
      const symbolInfo of symbols
    ) {

      try {

        const result =
          await analyzeSymbol(
            symbolInfo
          );


        results.push(
          result
        );

      } catch (error) {

        console.log(
          `Analysis failed for ${symbolInfo.symbol}:`,
          error.message
        );


        results.push({

          symbol:
            symbolInfo.symbol,

          price:
            null,

          signal:
            "WAIT",

          score:
            0,

          h1Direction:
            "ERROR",

          m15Direction:
            "ERROR",

          m5Direction:
            "ERROR",

          m15Structure:
            "ERROR",

          m5Structure:
            "ERROR",

          liquidity:
            "ERROR",

          burst: {

            direction:
              "NONE",

            stack:
              0,

            heat:
              "COOL",

            fadeBias:
              "NONE"
          },

          rsi:
            null,

          entry:
            null,

          stopLoss:
            null,

          takeProfit:
            null,

          error:
            error.message,

          updatedAt:
            new Date()
              .toISOString()
        });
      }


      /*
      Pace requests so the scanner
      stays comfortably below
      Deriv's WebSocket request limit.
      */

      await sleep(500);
    }


    state.markets =
      results;


    state.lastScan =
      new Date()
        .toISOString();


    const buys =
      results.filter(
        x =>
          x.signal ===
          "BUY"
      ).length;


    const sells =
      results.filter(
        x =>
          x.signal ===
          "SELL"
      ).length;


    const waits =
      results.filter(
        x =>
          x.signal ===
          "WAIT"
      ).length;


    console.log(
      `SCAN COMPLETE | BUY ${buys} | SELL ${sells} | WAIT ${waits}`
    );


  } catch (error) {

    state.error =
      error.message;


    console.log(
      "SCAN ERROR:",
      error.message
    );


  } finally {

    scanInProgress =
      false;

    state.scanRunning =
      false;
  }
}


/*
=========================================================
DASHBOARD
=========================================================
*/

app.get(
  "/",
  (req, res) => {

    res.sendFile(
      path.join(
        __dirname,
        "public",
        "index.html"
      )
    );
  }
);


app.get(
  "/health",
  (req, res) => {

    res.json({

      ok:
        true,

      online:
        state.online,

      derivConnected:
        state.derivConnected,

      scanRunning:
        state.scanRunning,

      lastScan:
        state.lastScan,

      error:
        state.error
    });
  }
);


app.get(
  "/api/status",
  (req, res) => {

    res.json({

      ok:
        true,

      online:
        state.online,

      derivConnected:
        state.derivConnected,

      symbolCount:
        state.symbolCount,

      lastScan:
        state.lastScan,

      scanRunning:
        state.scanRunning,

      error:
        state.error,

      markets:
        state.markets
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
  "0.0.0.0",
  async () => {

    console.log(
      `Deriv Volatility Burst Fader running on port ${PORT}`
    );


    try {

      await connectDeriv();

      await scanMarkets();

    } catch (error) {

      state.error =
        error.message;

      console.log(
        "Startup error:",
        error.message
      );
    }


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
