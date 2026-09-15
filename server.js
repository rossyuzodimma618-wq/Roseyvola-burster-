const express = require("express");
const path = require("path");
const WebSocket = require("ws");

const app = express();

const PORT = process.env.PORT || 3000;

const DERIV_WS_URL =
  "wss://api.derivws.com/trading/v1/options/ws/public";

const TELEGRAM_BOT_TOKEN =
  process.env.TELEGRAM_BOT_TOKEN;

const TELEGRAM_CHAT_ID =
  process.env.TELEGRAM_CHAT_ID;


// =====================================================
// SETTINGS
// =====================================================

const H1_GRANULARITY = 3600;
const M15_GRANULARITY = 900;
const M5_GRANULARITY = 300;

const CANDLE_COUNT = 250;

const SCAN_INTERVAL_MS = 30000;

const REQUEST_TIMEOUT_MS = 15000;

const ALERT_COOLDOWN_MS =
  15 * 60 * 1000;


// =====================================================
// EXPRESS
// =====================================================

app.use(express.json());

app.use(
  express.static(
    path.join(__dirname, "public")
  )
);

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
    service:
      "Deriv Volatility Burst Fader"
  });
});


// =====================================================
// STATE
// =====================================================

const state = {

  online: false,

  connected: false,

  lastScan: null,

  symbols: [],

  markets: [],

  error: null

};


const alertHistory = new Map();

let ws = null;

let requestId = 100;

const pendingRequests =
  new Map();


// =====================================================
// WEBSOCKET CONNECTION
// =====================================================

function connectDeriv() {

  return new Promise(
    (resolve, reject) => {

      if (
        ws &&
        ws.readyState ===
          WebSocket.OPEN
      ) {
        state.connected = true;
        return resolve();
      }

      ws = new WebSocket(
        DERIV_WS_URL
      );

      let settled = false;

      const timer =
        setTimeout(() => {

          if (!settled) {

            settled = true;

            try {
              ws.close();
            } catch {}

            reject(
              new Error(
                "Deriv WebSocket connection timeout"
              )
            );
          }

        }, REQUEST_TIMEOUT_MS);


      ws.on(
        "open",
        () => {

          clearTimeout(timer);

          state.connected = true;
          state.online = true;
          state.error = null;

          console.log(
            "Deriv WebSocket connected"
          );

          if (!settled) {

            settled = true;

            resolve();
          }

        }
      );


      ws.on(
        "message",
        raw => {

          let data;

          try {

            data =
              JSON.parse(
                raw.toString()
              );

          } catch {

            return;

          }


          if (
            data.req_id !==
            undefined
          ) {

            const pending =
              pendingRequests.get(
                data.req_id
              );

            if (pending) {

              pendingRequests.delete(
                data.req_id
              );

              clearTimeout(
                pending.timer
              );


              if (data.error) {

                pending.reject(
                  new Error(
                    data.error.message ||
                    JSON.stringify(
                      data.error
                    )
                  )
                );

              } else {

                pending.resolve(
                  data
                );

              }

            }

          }

        }
      );


      ws.on(
        "close",
        () => {

          state.connected =
            false;

          state.online =
            false;

          console.log(
            "Deriv WebSocket closed"
          );

        }
      );


      ws.on(
        "error",
        error => {

          state.connected =
            false;

          state.error =
            error.message;

          console.log(
            "Deriv WebSocket error:",
            error.message
          );

          if (!settled) {

            clearTimeout(timer);

            settled = true;

            reject(error);

          }

        }
      );

    }
  );
}


// =====================================================
// DERIV REQUEST
// =====================================================

async function derivRequest(
  payload
) {

  await connectDeriv();

  const req_id =
    ++requestId;

  const requestPayload = {

    ...payload,

    req_id

  };


  console.log(
    "Deriv request:",
    JSON.stringify(
      requestPayload
    )
  );


  return new Promise(
    (resolve, reject) => {

      const timer =
        setTimeout(() => {

          pendingRequests.delete(
            req_id
          );

          reject(
            new Error(
              "Deriv request timeout"
            )
          );

        }, REQUEST_TIMEOUT_MS);


      pendingRequests.set(
        req_id,
        {
          resolve,
          reject,
          timer
        }
      );


      try {

        ws.send(
          JSON.stringify(
            requestPayload
          )
        );

      } catch (error) {

        clearTimeout(timer);

        pendingRequests.delete(
          req_id
        );

        reject(error);

      }

    }
  );
}


// =====================================================
// ACTIVE SYMBOLS
// =====================================================

async function getActiveSymbols() {

  const data =
    await derivRequest({

      active_symbols:
        "brief"

    });


  if (
    !Array.isArray(
      data.active_symbols
    )
  ) {

    throw new Error(
      "No active symbols returned by Deriv"
    );

  }


  const found = [];


  for (
    const item of
    data.active_symbols
  ) {

    const symbol =
      item.underlying_symbol ||
      item.symbol ||
      "";


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


    const combined =
      `${symbol} ${name} ${type} ${market}`
        .toLowerCase();


    const isVolatility =
      combined.includes(
        "volatility"
      ) ||
      /^1hz\d+v/i.test(
        symbol
      ) ||
      /^r_\d+/i.test(
        symbol
      );


    if (
      isVolatility &&
      symbol
    ) {

      found.push({

        symbol,

        name,

        market

      });

    }

  }


  const unique =
    Array.from(
      new Map(
        found.map(
          x => [
            x.symbol,
            x
          ]
        )
      ).values()
    );


  state.symbols =
    unique.map(
      x => x.symbol
    );


  return unique;

}


// =====================================================
// CANDLE REQUEST
// =====================================================
//
// Deriv's current public endpoint can validate
// the "subscribe" field differently.
//
// We first try without subscribe.
// If Deriv specifically complains about subscribe,
// we retry with subscribe:false.
// If necessary, we retry with subscribe:0.
//
// Both false and 0 mean:
// DO NOT keep a live subscription.
// =====================================================

async function requestCandles(
  symbol,
  granularity,
  subscribeValue
) {

  const payload = {

    ticks_history:
      symbol,

    end:
      "latest",

    count:
      CANDLE_COUNT,

    style:
      "candles",

    granularity

  };


  if (
    subscribeValue !==
    undefined
  ) {

    payload.subscribe =
      subscribeValue;

  }


  const data =
    await derivRequest(
      payload
    );


  if (
    !Array.isArray(
      data.candles
    )
  ) {

    throw new Error(
      `No candles returned for ${symbol} ${granularity}`
    );

  }


  return data;

}


async function getCandles(
  symbol,
  granularity
) {

  try {

    return await requestCandles(
      symbol,
      granularity
    );

  } catch (firstError) {

    const message =
      String(
        firstError.message ||
        ""
      );


    if (
      !/subscribe/i.test(
        message
      )
    ) {

      throw firstError;

    }


    console.log(
      `Retrying ${symbol} ${granularity} with subscribe:false`
    );


    try {

      return await requestCandles(
        symbol,
        granularity,
        false
      );

    } catch (secondError) {

      const message2 =
        String(
          secondError.message ||
          ""
        );


      if (
        !/subscribe/i.test(
          message2
        )
      ) {

        throw secondError;

      }


      console.log(
        `Retrying ${symbol} ${granularity} with subscribe:0`
      );


      return await requestCandles(
        symbol,
        granularity,
        0
      );

    }

  }

}


// =====================================================
// CLEAN CANDLES
// =====================================================

function cleanCandles(
  data
) {

  const candles =
    data.candles
      .map(
        c => ({

          time:
            Number(
              c.epoch
            ),

          open:
            Number(
              c.open
            ),

          high:
            Number(
              c.high
            ),

          low:
            Number(
              c.low
            ),

          close:
            Number(
              c.close
            )

        })
      )
      .filter(
        c =>

          Number.isFinite(
            c.time
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
          a.time - b.time
      );


  // Remove the currently forming candle.
  if (
    candles.length > 1
  ) {

    candles.pop();

  }


  return candles;

}


// =====================================================
// EMA
// =====================================================

function ema(
  values,
  period
) {

  if (
    values.length <
    period
  ) {

    return null;

  }


  const multiplier =
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
      (
        values[i] -
        result
      ) *
        multiplier +
      result;

  }


  return result;

}


// =====================================================
// ATR
// =====================================================

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
    trs.length <
    period
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


// =====================================================
// RSI
// =====================================================

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
      change > 0
        ? change
        : 0;


    const loss =
      change < 0
        ? -change
        : 0;


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


// =====================================================
// DIRECTION
// =====================================================

function getDirection(
  candles
) {

  if (
    candles.length < 30
  ) {

    return "NEUTRAL";

  }


  const closes =
    candles.map(
      c => c.close
    );


  const ema20 =
    ema(
      closes,
      20
    );


  if (
    ema20 === null
  ) {

    return "NEUTRAL";

  }


  const last =
    candles[
      candles.length - 1
    ];


  const previous =
    candles[
      candles.length - 6
    ];


  if (
    last.close >
      ema20 &&
    last.close >
      previous.close
  ) {

    return "BULLISH";

  }


  if (
    last.close <
      ema20 &&
    last.close <
      previous.close
  ) {

    return "BEARISH";

  }


  return "NEUTRAL";

}


// =====================================================
// BOS / CHoCH STRUCTURE
// =====================================================

function getStructure(
  candles
) {

  if (
    candles.length < 20
  ) {

    return "NEUTRAL";

  }


  const recent =
    candles.slice(
      -12
    );


  let highest =
    -Infinity;

  let lowest =
    Infinity;


  for (
    let i = 0;
    i < recent.length - 2;
    i++
  ) {

    highest =
      Math.max(
        highest,
        recent[i].high
      );

    lowest =
      Math.min(
        lowest,
        recent[i].low
      );

  }


  const last =
    recent[
      recent.length - 1
    ];


  const previous =
    recent[
      recent.length - 2
    ];


  if (
    last.close >
    highest
  ) {

    return "BULLISH";

  }


  if (
    last.close <
    lowest
  ) {

    return "BEARISH";

  }


  if (
    last.close >
      previous.close &&
    last.low >=
      previous.low
  ) {

    return "BULLISH";

  }


  if (
    last.close <
      previous.close &&
    last.high <=
      previous.high
  ) {

    return "BEARISH";

  }


  return "NEUTRAL";

}


// =====================================================
// LIQUIDITY SWEEP
// =====================================================

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


  const previousCandles =
    candles.slice(
      -11,
      -1
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


  // Price swept below liquidity
  // and closed back above it.
  if (
    last.low <
      previousLow &&
    last.close >
      previousLow
  ) {

    return "BULLISH LIQUIDITY SWEEP";

  }


  // Price swept above liquidity
  // and closed back below it.
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


// =====================================================
// STRONG BURST FADER
// =====================================================
//
// This is the important part.
//
// EMA20
// ATR14
// Upper = EMA20 + 2 ATR
// Lower = EMA20 - 2 ATR
//
// Burst stack measures how far price extends
// beyond the normal ATR band.
//
// The FADER interprets:
//
// DOWN BURST -> possible bullish reversal
// UP BURST   -> possible bearish reversal
//
// It is a SUPPORTING confirmation.
// It does NOT have to be active for every trade.
// =====================================================

function strongBurstFader(
  candles
) {

  if (
    candles.length < 30
  ) {

    return {

      signal: "NEUTRAL",

      heat: "COOL",

      stack: 0,

      fadeBias: "NEUTRAL",

      strength: 0,

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

      signal: "NEUTRAL",

      heat: "COOL",

      stack: 0,

      fadeBias: "NEUTRAL",

      strength: 0,

      basis: null,

      upper: null,

      lower: null

    };

  }


  const upper =
    basis +
    2 * atrValue;


  const lower =
    basis -
    2 * atrValue;


  const last =
    candles[
      candles.length - 1
    ];


  let signal =
    "NEUTRAL";


  let stack = 0;


  // ===================================================
  // UP BURST
  // ===================================================

  if (
    last.high >
    upper
  ) {

    signal =
      "UP BURST";


    const extension =
      last.high -
      upper;


    stack =
      Math.min(
        6,
        Math.max(
          1,
          Math.ceil(
            extension /
              (0.5 * atrValue)
          )
        )
      );

  }


  // ===================================================
  // DOWN BURST
  // ===================================================

  else if (
    last.low <
    lower
  ) {

    signal =
      "DOWN BURST";


    const extension =
      lower -
      last.low;


    stack =
      Math.min(
        6,
        Math.max(
          1,
          Math.ceil(
            extension /
              (0.5 * atrValue)
          )
        )
      );

  }


  let heat =
    "COOL";


  if (
    stack >= 5
  ) {

    heat =
      "EXTREME";

  }

  else if (
    stack >= 3
  ) {

    heat =
      "HOT";

  }

  else if (
    stack >= 1
  ) {

    heat =
      "WARM";

  }


  // ===================================================
  // FADER LOGIC
  // ===================================================

  let fadeBias =
    "NEUTRAL";


  if (
    signal ===
      "DOWN BURST" &&
    stack >= 1
  ) {

    fadeBias =
      "BULLISH";

  }


  if (
    signal ===
      "UP BURST" &&
    stack >= 1
  ) {

    fadeBias =
      "BEARISH";

  }


  return {

    signal,

    heat,

    stack,

    fadeBias,

    strength:
      stack,

    basis,

    upper,

    lower

  };

}


// =====================================================
// BURST + LIQUIDITY CONFIRMATION
// =====================================================

function getBurstConfirmation(
  burst,
  liquidity,
  structure
) {

  let bullish = false;
  let bearish = false;


  // Strong downside burst
  // followed by bullish liquidity/structure
  // = strong fader BUY support.
  if (
    burst.signal ===
      "DOWN BURST"
  ) {

    if (
      liquidity ===
        "BULLISH LIQUIDITY SWEEP" ||
      structure ===
        "BULLISH"
    ) {

      bullish = true;

    }

  }


  // Strong upside burst
  // followed by bearish liquidity/structure
  // = strong fader SELL support.
  if (
    burst.signal ===
      "UP BURST"
  ) {

    if (
      liquidity ===
        "BEARISH LIQUIDITY SWEEP" ||
      structure ===
        "BEARISH"
    ) {

      bearish = true;

    }

  }


  return {

    bullish,

    bearish

  };

}


// =====================================================
// PRICE DECIMALS
// =====================================================

function getDecimals(
  symbol
) {

  if (
    /XAU|GOLD/i.test(
      symbol
    )
  ) {

    return 2;

  }


  if (
    /JPY/i.test(
      symbol
    )
  ) {

    return 3;

  }


  return 5;

}


function roundPrice(
  value,
  decimals
) {

  return Number(
    Number(value).toFixed(
      decimals
    )
  );

}


// =====================================================
// ANALYZE SYMBOL
// =====================================================

async function analyzeSymbol(
  symbol
) {

  // -----------------------------------------------
  // H1
  // -----------------------------------------------

  const h1Data =
    await getCandles(
      symbol,
      H1_GRANULARITY
    );


  const h1 =
    cleanCandles(
      h1Data
    );


  // -----------------------------------------------
  // M15
  // -----------------------------------------------

  const m15Data =
    await getCandles(
      symbol,
      M15_GRANULARITY
    );


  const m15 =
    cleanCandles(
      m15Data
    );


  // -----------------------------------------------
  // M5
  // -----------------------------------------------

  const m5Data =
    await getCandles(
      symbol,
      M5_GRANULARITY
    );


  const m5 =
    cleanCandles(
      m5Data
    );


  if (
    h1.length < 30 ||
    m15.length < 30 ||
    m5.length < 30
  ) {

    throw new Error(
      `Insufficient candle data for ${symbol}`
    );

  }


  // -----------------------------------------------
  // ANALYSIS
  // -----------------------------------------------

  const h1Direction =
    getDirection(
      h1
    );


  const m15Direction =
    getDirection(
      m15
    );


  const m5Direction =
    getDirection(
      m5
    );


  const m15Structure =
    getStructure(
      m15
    );


  const m5Structure =
    getStructure(
      m5
    );


  const liquidity =
    getLiquiditySweep(
      m5
    );


  const burst =
    strongBurstFader(
      m5
    );


  const burstConfirmation =
    getBurstConfirmation(
      burst,
      liquidity,
      m5Structure
    );


  const rsiValue =
    rsi(
      m5,
      14
    );


  const atrValue =
    atr(
      m5,
      14
    );


  const last =
    m5[
      m5.length - 1
    ];


  // =================================================
  // DETERMINE PRIMARY DIRECTION
  // =================================================

  let direction =
    "WAIT";


  if (
    h1Direction ===
      "BULLISH" &&
    m15Direction ===
      "BULLISH"
  ) {

    direction =
      "BUY";

  }


  if (
    h1Direction ===
      "BEARISH" &&
    m15Direction ===
      "BEARISH"
  ) {

    direction =
      "SELL";

  }


  // =================================================
  // SCORE
  // =================================================

  let buyScore = 0;
  let sellScore = 0;


  // H1
  if (
    h1Direction ===
    "BULLISH"
  ) {

    buyScore++;

  }


  if (
    h1Direction ===
    "BEARISH"
  ) {

    sellScore++;

  }


  // M15 direction
  if (
    m15Direction ===
    "BULLISH"
  ) {

    buyScore++;

  }


  if (
    m15Direction ===
    "BEARISH"
  ) {

    sellScore++;

  }


  // M15 structure
  if (
    m15Structure ===
    "BULLISH"
  ) {

    buyScore++;

  }


  if (
    m15Structure ===
    "BEARISH"
  ) {

    sellScore++;

  }


  // M5 structure
  if (
    m5Structure ===
    "BULLISH"
  ) {

    buyScore++;

  }


  if (
    m5Structure ===
    "BEARISH"
  ) {

    sellScore++;

  }


  // =================================================
  // STRONG BURST FADER
  // =================================================
  //
  // IMPORTANT:
  // This is NOT a mandatory condition.
  //
  // It adds a point when it supports the setup.
  // =================================================

  if (
    burstConfirmation.bullish
  ) {

    buyScore++;

  }


  if (
    burstConfirmation.bearish
  ) {

    sellScore++;

  }


  // =================================================
  // LIQUIDITY BONUS
  // =================================================

  if (
    liquidity ===
      "BULLISH LIQUIDITY SWEEP"
  ) {

    buyScore++;

  }


  if (
    liquidity ===
      "BEARISH LIQUIDITY SWEEP"
  ) {

    sellScore++;

  }


  // =================================================
  // CAP SCORE AT 5
  // =================================================

  buyScore =
    Math.min(
      5,
      buyScore
    );


  sellScore =
    Math.min(
      5,
      sellScore
    );


  // =================================================
  // FINAL SIGNAL
  // =================================================

  let signal =
    "WAIT";


  let score = 0;


  // -----------------------------------------------
  // BUY
  // -----------------------------------------------

  const buyStructureOK =
    m15Structure ===
      "BULLISH" ||
    m5Structure ===
      "BULLISH" ||
    burstConfirmation.bullish;


  const sellStructureOK =
    m15Structure ===
      "BEARISH" ||
    m5Structure ===
      "BEARISH" ||
    burstConfirmation.bearish;


  if (
    h1Direction ===
      "BULLISH" &&
    m15Direction ===
      "BULLISH" &&
    buyStructureOK &&
    buyScore >= 3
  ) {

    signal =
      "BUY";

    score =
      buyScore;

  }


  // -----------------------------------------------
  // SELL
  // -----------------------------------------------

  if (
    h1Direction ===
      "BEARISH" &&
    m15Direction ===
      "BEARISH" &&
    sellStructureOK &&
    sellScore >= 3
  ) {

    signal =
      "SELL";

    score =
      sellScore;

  }


  // =================================================
  // ENTRY / SL / TP
  // =================================================

  const decimals =
    getDecimals(
      symbol
    );


  const entry =
    roundPrice(
      last.close,
      decimals
    );


  let stopLoss;
  let takeProfit;


  const recent =
    m5.slice(
      -8
    );


  const recentHigh =
    Math.max(
      ...recent.map(
        c => c.high
      )
    );


  const recentLow =
    Math.min(
      ...recent.map(
        c => c.low
      )
    );


  const buffer =
    atrValue
      ? atrValue *
        0.20
      : entry *
        0.001;


  if (
    signal ===
    "BUY"
  ) {

    stopLoss =
      recentLow -
      buffer;


    const risk =
      entry -
      stopLoss;


    takeProfit =
      entry +
      risk * 2;

  }


  else if (
    signal ===
    "SELL"
  ) {

    stopLoss =
      recentHigh +
      buffer;


    const risk =
      stopLoss -
      entry;


    takeProfit =
      entry -
      risk * 2;

  }


  else {

    stopLoss = null;

    takeProfit = null;

  }


  if (
    stopLoss !== null
  ) {

    stopLoss =
      roundPrice(
        stopLoss,
        decimals
      );

  }


  if (
    takeProfit !== null
  ) {

    takeProfit =
      roundPrice(
        takeProfit,
        decimals
      );

  }


  // =================================================
  // RETURN
  // =================================================

  return {

    symbol,

    price:
      entry,

    signal,

    score,

    maxScore:
      5,

    entry,

    stopLoss,

    takeProfit,

    h1:
      h1Direction,

    h1Direction,

    m15:
      m15Direction,

    m15Direction,

    m5:
      m5Direction,

    m5Direction,

    m15Structure,

    m5Structure,

    liquidity,

    liquiditySweep:
      liquidity,

    burstFader:
      burst.signal,

    burstSignal:
      burst.signal,

    burstHeat:
      burst.heat,

    burstStack:
      burst.stack,

    burstFadeBias:
      burst.fadeBias,

    burstStrength:
      burst.strength,

    burstSupportsBuy:
      burstConfirmation.bullish,

    burstSupportsSell:
      burstConfirmation.bearish,

    rsi:
      rsiValue === null
        ? null
        : Number(
            rsiValue.toFixed(
              1
            )
          ),

    atr:
      atrValue,

    timestamp:
      new Date().toISOString()

  };

}


// =====================================================
// TELEGRAM
// =====================================================

async function sendTelegram(
  result
) {

  if (
    !TELEGRAM_BOT_TOKEN ||
    !TELEGRAM_CHAT_ID
  ) {

    return;

  }


  if (
    result.signal !==
      "BUY" &&
    result.signal !==
      "SELL"
  ) {

    return;

  }


  const now =
    Date.now();


  const previous =
    alertHistory.get(
      result.symbol
    ) || 0;


  if (
    now - previous <
    ALERT_COOLDOWN_MS
  ) {

    return;

  }


  alertHistory.set(
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
🛑 Stop Loss: ${result.stopLoss}
🎯 Take Profit: ${result.takeProfit}

⭐ Score: ${result.score}/5

📊 H1: ${result.h1Direction}
📊 M15: ${result.m15Direction}
📊 M5: ${result.m5Direction}

🏗️ M15 Structure: ${result.m15Structure}
🏗️ M5 Structure: ${result.m5Structure}

🔎 Liquidity: ${result.liquidity}

⚡ Burst Fader: ${result.burstSignal}
🔥 Burst Heat: ${result.burstHeat}
📈 Burst Stack: ${result.burstStack}
🧭 Burst Fade Bias: ${result.burstFadeBias}

RSI: ${result.rsi ?? "N/A"}

⚠️ Scanner alert only — confirm before trading.`;


  const url =
    `https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage`;


  try {

    await fetch(
      url,
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

            text:
              message

          })

      }
    );

  } catch (error) {

    console.log(
      "Telegram error:",
      error.message
    );

  }

}


// =====================================================
// SCANNER
// =====================================================

let scanning = false;


async function scanMarkets() {

  if (
    scanning
  ) {

    return;

  }


  scanning = true;


  try {

    const symbols =
      await getActiveSymbols();


    const results = [];


    console.log(
      `Found ${symbols.length} Volatility symbols`
    );


    for (
      const item of symbols
    ) {

      try {

        const result =
          await analyzeSymbol(
            item.symbol
          );


        results.push(
          result
        );


        console.log(
          `${item.symbol} | ` +
          `H1 ${result.h1Direction} | ` +
          `M15 ${result.m15Structure} | ` +
          `M5 ${result.m5Structure} | ` +
          `Burst ${result.burstSignal} | ` +
          `Heat ${result.burstHeat} | ` +
          `Stack ${result.burstStack} | ` +
          `Liquidity ${result.liquidity} | ` +
          `${result.signal} ${result.score}/5`
        );


        await sendTelegram(
          result
        );


        // Small delay between symbols.
        await new Promise(
          resolve =>
            setTimeout(
              resolve,
              250
            )
        );


      } catch (error) {

        console.log(
          `Scan error for ${item.symbol}:`,
          error.message
        );

      }

    }


    state.markets =
      results;


    state.lastScan =
      new Date().toISOString();


    state.error =
      null;


  } catch (error) {

    state.error =
      error.message;


    console.log(
      "Scanner error:",
      error.message
    );

  } finally {

    scanning =
      false;

  }

}


// =====================================================
// STATUS API
// =====================================================

app.get(
  "/api/status",
  (req, res) => {

    res.json({

      ok:
        true,

      online:
        state.online,

      connected:
        state.connected,

      lastScan:
        state.lastScan,

      symbols:
        state.symbols,

      volatilityIndices:
        state.symbols.length,

      markets:
        state.markets,

      error:
        state.error

    });

  }
);


// =====================================================
// START SERVER
// =====================================================

app.listen(
  PORT,
  "0.0.0.0",
  () => {

    console.log(
      `Deriv Volatility Burst Fader running on port ${PORT}`
    );


    setTimeout(
      () => {

        scanMarkets();

      },
      2000
    );


    setInterval(
      () => {

        scanMarkets();

      },
      SCAN_INTERVAL_MS
    );

  }
);
