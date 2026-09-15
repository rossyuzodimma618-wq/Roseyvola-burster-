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
SETTINGS
=========================================================
*/

const TIMEFRAMES = {
  H1: 3600,
  M15: 900,
  M5: 300
};

const CANDLE_COUNT = 120;

const SCAN_INTERVAL_MS = 60000;

const REQUEST_TIMEOUT_MS = 15000;

const REQUEST_DELAY_MS = 400;

const ALERT_COOLDOWN_MS = 15 * 60 * 1000;

const ACTIVE_SYMBOL_CACHE_MS = 10 * 60 * 1000;

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

let activeSymbolsCache = [];

let activeSymbolsCacheTime = 0;

/*
=========================================================
DERIV CONNECTION
=========================================================
*/

let derivSocket = null;

let derivConnecting = false;

let derivRequestId = 1;

const pendingRequests = new Map();

let requestQueue = Promise.resolve();

let lastRequestTime = 0;

/*
=========================================================
UTILITY
=========================================================
*/

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function now() {
  return Date.now();
}

function roundPrice(value, digits = 2) {
  const n = Number(value);

  if (!Number.isFinite(n)) {
    return null;
  }

  return Number(n.toFixed(digits));
}

function symbolDigits(symbol) {
  const s = String(symbol || "").toUpperCase();

  if (s.includes("JPY")) {
    return 3;
  }

  return 2;
}

/*
=========================================================
DERIV SOCKET
=========================================================
*/

function connectDeriv() {
  return new Promise((resolve, reject) => {
    if (
      derivSocket &&
      derivSocket.readyState === WebSocket.OPEN
    ) {
      resolve();

      return;
    }

    if (derivConnecting) {
      const wait = setInterval(() => {
        if (
          derivSocket &&
          derivSocket.readyState === WebSocket.OPEN
        ) {
          clearInterval(wait);

          resolve();
        }

        if (!derivConnecting) {
          clearInterval(wait);

          if (
            !derivSocket ||
            derivSocket.readyState !== WebSocket.OPEN
          ) {
            reject(
              new Error("Deriv connection failed")
            );
          }
        }
      }, 100);

      return;
    }

    derivConnecting = true;

    const ws = new WebSocket(DERIV_WS_URL);

    derivSocket = ws;

    const timeout = setTimeout(() => {
      try {
        ws.close();
      } catch {}

      derivConnecting = false;

      reject(
        new Error("Deriv WebSocket connection timeout")
      );
    }, REQUEST_TIMEOUT_MS);

    ws.on("open", () => {
      clearTimeout(timeout);

      derivConnecting = false;

      state.derivConnected = true;

      console.log(
        "Deriv WebSocket connected"
      );

      resolve();
    });

    ws.on("message", raw => {
      let data;

      try {
        data = JSON.parse(
          raw.toString()
        );
      } catch {
        return;
      }

      const requestId =
        data.req_id;

      if (
        requestId &&
        pendingRequests.has(requestId)
      ) {
        const pending =
          pendingRequests.get(requestId);

        pendingRequests.delete(requestId);

        clearTimeout(
          pending.timeout
        );

        if (data.error) {
          pending.reject(
            new Error(
              data.error.message ||
              JSON.stringify(data.error)
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

      state.error =
        err.message;

      state.derivConnected = false;

      if (derivConnecting) {
        clearTimeout(timeout);

        derivConnecting = false;

        reject(err);
      }
    });

    ws.on("close", () => {
      state.derivConnected = false;

      derivSocket = null;

      derivConnecting = false;

      console.log(
        "Deriv WebSocket closed"
      );

      for (
        const [
          id,
          pending
        ] of pendingRequests
      ) {
        clearTimeout(
          pending.timeout
        );

        pending.reject(
          new Error(
            "Deriv connection closed"
          )
        );

        pendingRequests.delete(id);
      }
    });
  });
}

/*
=========================================================
DERIV REQUEST
=========================================================
*/

function derivRequest(payload) {
  requestQueue =
    requestQueue.then(
      async () => {
        const elapsed =
          now() - lastRequestTime;

        if (
          elapsed <
          REQUEST_DELAY_MS
        ) {
          await sleep(
            REQUEST_DELAY_MS -
            elapsed
          );
        }

        lastRequestTime = now();

        let attempt = 0;

        while (attempt < 3) {
          attempt++;

          try {
            await connectDeriv();

            if (
              !derivSocket ||
              derivSocket.readyState !==
                WebSocket.OPEN
            ) {
              throw new Error(
                "Deriv socket is not open"
              );
            }

            const reqId =
              derivRequestId++;

            const message = {
              ...payload,
              req_id: reqId
            };

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
                      resolve,
                      reject,
                      timeout
                    }
                  );

                  derivSocket.send(
                    JSON.stringify(message)
                  );
                }
              );

            state.error = null;

            return result;
          } catch (error) {
            console.log(
              `Deriv request attempt ${attempt}:`,
              error.message
            );

            if (
              attempt >= 3
            ) {
              throw error;
            }

            await sleep(
              1000 * attempt
            );
          }
        }
      }
    );

  return requestQueue;
}

/*
=========================================================
ACTIVE SYMBOLS
=========================================================
*/

async function getVolatilitySymbols() {
  if (
    activeSymbolsCache.length > 0 &&
    now() -
      activeSymbolsCacheTime <
      ACTIVE_SYMBOL_CACHE_MS
  ) {
    return activeSymbolsCache;
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

  const volatility =
    symbols.filter(item => {
      const symbol =
        String(
          item.symbol ||
          item.underlying_symbol ||
          ""
        );

      const display =
        String(
          item.display_name ||
          item.underlying_symbol_name ||
          ""
        );

      return (
        /volatility/i.test(
          display
        ) ||
        /volatility/i.test(
          symbol
        ) ||
        /V\d+/i.test(
          symbol
        )
      );
    });

  activeSymbolsCache =
    volatility.map(item => ({
      symbol:
        item.symbol ||
        item.underlying_symbol,

      display_name:
        item.display_name ||
        item.underlying_symbol_name ||
        item.symbol ||
        item.underlying_symbol,

      pip:
        item.pip ||
        item.pip_size ||
        null
    }));

  activeSymbolsCacheTime =
    now();

  return activeSymbolsCache;
}

/*
=========================================================
CANDLE DATA
IMPORTANT:
NO "subscribe" FIELD
=========================================================
*/

async function requestCandles(
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
    data &&
    data.history &&
    Array.isArray(
      data.history.candles
    )
  ) {
    return data.history.candles;
  }

  if (
    data &&
    Array.isArray(
      data.candles
    )
  ) {
    return data.candles;
  }

  throw new Error(
    `No candles returned for ${symbol}`
  );
}

/*
=========================================================
CANDLE CLEANING
=========================================================
*/

function cleanCandles(raw) {
  if (!Array.isArray(raw)) {
    return [];
  }

  const candles =
    raw
      .map(c => ({
        time: Number(
          c.epoch ||
          c.time
        ),

        open: Number(
          c.open
        ),

        high: Number(
          c.high
        ),

        low: Number(
          c.low
        ),

        close: Number(
          c.close
        )
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
  Remove the currently forming candle.
  */

  if (candles.length > 2) {
    candles.pop();
  }

  return candles;
}

/*
=========================================================
EMA
=========================================================
*/

function ema(values, period) {
  if (
    values.length <
    period
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

/*
=========================================================
ATR
=========================================================
*/

function atr(candles, period = 14) {
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

    const tr = Math.max(
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

  let value = 0;

  for (
    let i = 0;
    i < period;
    i++
  ) {
    value += trs[i];
  }

  value /= period;

  for (
    let i = period;
    i < trs.length;
    i++
  ) {
    value =
      (value *
        (period - 1) +
        trs[i]) /
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
  if (
    candles.length <=
    period
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
      change > 0
        ? change
        : 0;

    const loss =
      change < 0
        ? Math.abs(change)
        : 0;

    avgGain =
      (avgGain *
        (period - 1) +
        gain) /
      period;

    avgLoss =
      (avgLoss *
        (period - 1) +
        loss) /
      period;
  }

  if (
    avgLoss === 0
  ) {
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
DIRECTION
=========================================================
*/

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

  const e =
    ema(closes, 20);

  const last =
    closes[closes.length - 1];

  const old =
    closes[
      closes.length - 6
    ];

  if (
    last > e &&
    last > old
  ) {
    return "BULLISH";
  }

  if (
    last < e &&
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
      candles.length - 12,
      candles.length - 2
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
    last.close > highest
  ) {
    return "BULLISH BOS";
  }

  if (
    last.close < lowest
  ) {
    return "BEARISH BOS";
  }

  const prev =
    candles[
      candles.length - 2
    ];

  if (
    last.close > last.open &&
    prev.close > prev.open
  ) {
    return "BULLISH CONTINUATION";
  }

  if (
    last.close < last.open &&
    prev.close < prev.open
  ) {
    return "BEARISH CONTINUATION";
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
    candles.length < 30
  ) {
    return {
      active: false,
      direction: "NONE",
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
    ema(closes, 20);

  const atrValue =
    atr(candles, 14);

  if (
    basis === null ||
    atrValue === null ||
    atrValue <= 0
  ) {
    return {
      active: false,
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

  const last =
    candles[
      candles.length - 1
    ];

  let direction =
    "NONE";

  let stack = 0;

  if (
    last.high > upper
  ) {
    direction = "UP";

    stack = Math.min(
      6,
      Math.max(
        1,
        Math.ceil(
          (last.high -
            upper) /
            (0.5 *
              atrValue)
        )
      )
    );
  } else if (
    last.low < lower
  ) {
    direction = "DOWN";

    stack = Math.min(
      6,
      Math.max(
        1,
        Math.ceil(
          (lower -
            last.low) /
            (0.5 *
              atrValue)
        )
      )
    );
  }

  let heat = "COOL";

  if (
    stack >= 5
  ) {
    heat = "EXTREME";
  } else if (
    stack >= 3
  ) {
    heat = "HOT";
  } else if (
    stack >= 1
  ) {
    heat = "WARM";
  }

  let fadeBias =
    "NONE";

  if (
    direction === "UP"
  ) {
    fadeBias = "BEARISH";
  }

  if (
    direction === "DOWN"
  ) {
    fadeBias = "BULLISH";
  }

  return {
    active:
      direction !== "NONE",

    direction,

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
BURST WARNING
=========================================================
*/

function getBurstWarningStage(
  burst
) {
  if (
    !burst.active ||
    burst.stack < 1
  ) {
    return null;
  }

  if (
    burst.stack >= 3
  ) {
    return "STRONG";
  }

  if (
    burst.stack === 2
  ) {
    return "EARLY";
  }

  return "EARLY";
}

function shouldSendBurstWarning(
  symbol,
  burst
) {
  const stage =
    getBurstWarningStage(
      burst
    );

  const previous =
    burstWarningState.get(
      symbol
    );

  if (!stage) {
    burstWarningState.delete(
      symbol
    );

    return false;
  }

  if (!previous) {
    burstWarningState.set(
      symbol,
      {
        direction:
          burst.direction,
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
        direction:
          burst.direction,
        stage
      }
    );

    return true;
  }

  if (
    previous.stage !==
      "STRONG" &&
    stage === "STRONG"
  ) {
    burstWarningState.set(
      symbol,
      {
        direction:
          burst.direction,
        stage
      }
    );

    return true;
  }

  burstWarningState.set(
    symbol,
    {
      direction:
        burst.direction,
      stage
    }
  );

  return false;
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

  const url =
    `https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage`;

  try {
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
    console.log(
      "Telegram error:",
      error.message
    );
  }
}

/*
=========================================================
BURST WARNING TELEGRAM
=========================================================
*/

async function sendBurstWarning(
  market
) {
  const direction =
    market.burst.direction ===
    "UP"
      ? "UPSIDE BURST"
      : "DOWNSIDE BURST";

  const message =
`⚠️ STRONG BURST WARNING

📊 ${market.displayName}

💥 ${direction}
🔥 Heat: ${market.burst.heat}
📈 Stack: ${market.burst.stack}/6

Possible reversal area detected.

WAIT FOR CONFIRMATION.

5M → Liquidity Sweep
→ BOS/CHoCH
→ Retracement
→ Confirmation

⚠️ This is a warning,
NOT an entry signal.`;

  await sendTelegram(
    message
  );
}

/*
=========================================================
TRADE ALERT
=========================================================
*/

async function sendTradeAlert(
  market
) {
  const lastAlert =
    alertState.get(
      market.symbol
    );

  if (
    lastAlert &&
    now() - lastAlert <
      ALERT_COOLDOWN_MS
  ) {
    return;
  }

  alertState.set(
    market.symbol,
    now()
  );

  const digits =
    symbolDigits(
      market.symbol
    );

  const message =
`${market.signal === "BUY"
    ? "🟢"
    : "🔴"} STRONG ${market.signal}

📊 ${market.displayName}

📍 Entry: ${market.entry.toFixed(
    digits
  )}

🛑 Stop Loss: ${market.sl.toFixed(
    digits
  )}

🎯 Take Profit: ${market.tp.toFixed(
    digits
  )}

⭐ Score: ${market.score}/5

📊 1H: ${market.h1Direction}
📊 15M: ${market.m15Direction}
📊 5M: ${market.m5Direction}

🔎 Structure:
${market.m15Structure}

💧 Liquidity:
${market.liquidity}

💥 Burst Fader:
${market.burst.direction} / ${market.burst.heat}

📈 RSI: ${
    market.rsi !== null
      ? market.rsi.toFixed(1)
      : "N/A"
  }

⚖️ Risk/Reward: 1:2`;

  await sendTelegram(
    message
  );
}

/*
=========================================================
ANALYZE MARKET
=========================================================
*/

async function analyzeMarket(
  symbolInfo
) {
  const symbol =
    symbolInfo.symbol;

  const displayName =
    symbolInfo.display_name ||
    symbol;

  /*
  H1
  */

  const h1Raw =
    await requestCandles(
      symbol,
      TIMEFRAMES.H1
    );

  await sleep(
    REQUEST_DELAY_MS
  );

  /*
  M15
  */

  const m15Raw =
    await requestCandles(
      symbol,
      TIMEFRAMES.M15
    );

  await sleep(
    REQUEST_DELAY_MS
  );

  /*
  M5
  */

  const m5Raw =
    await requestCandles(
      symbol,
      TIMEFRAMES.M5
    );

  const h1 =
    cleanCandles(
      h1Raw
    );

  const m15 =
    cleanCandles(
      m15Raw
    );

  const m5 =
    cleanCandles(
      m5Raw
    );

  if (
    h1.length < 30 ||
    m15.length < 30 ||
    m5.length < 30
  ) {
    throw new Error(
      `Not enough candle data for ${symbol}`
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
  BURST
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
  RSI
  */

  const rsiValue =
    rsi(m5, 14);

  /*
  SCORE
  */

  let score = 0;

  if (
    h1Direction ===
    "BULLISH"
  ) {
    score++;
  }

  if (
    h1Direction ===
    "BEARISH"
  ) {
    score++;
  }

  if (
    m15Direction ===
      h1Direction &&
    h1Direction !==
      "NEUTRAL"
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
    (
      m5Structure.startsWith(
        "BULLISH"
      ) ||
      burstSupport.supportsBuy
    )
  ) {
    score++;
  }

  if (
    h1Direction ===
      "BEARISH" &&
    (
      m5Structure.startsWith(
        "BEARISH"
      ) ||
      burstSupport.supportsSell
    )
  ) {
    score++;
  }

  if (
    h1Direction ===
      "BULLISH" &&
    liquidity ===
      "BULLISH LIQUIDITY SWEEP"
  ) {
    score++;
  }

  if (
    h1Direction ===
      "BEARISH" &&
    liquidity ===
      "BEARISH LIQUIDITY SWEEP"
  ) {
    score++;
  }

  score =
    Math.min(
      5,
      score
    );

  /*
  SIGNAL
  */

  let signal =
    "WAIT";

  if (
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
    score >= 3
  ) {
    signal = "BUY";
  }

  if (
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
    score >= 3
  ) {
    signal = "SELL";
  }

  /*
  ENTRY
  */

  const entry =
    m5[
      m5.length - 1
    ].close;

  /*
  ATR
  */

  const atrValue =
    atr(m5, 14) ||
    Math.abs(
      m5[
        m5.length - 1
      ].high -
      m5[
        m5.length - 1
      ].low
    );

  /*
  STOP LOSS
  */

  const recentM5 =
    m5.slice(
      Math.max(
        0,
        m5.length - 8
      )
    );

  let sl;

  let tp;

  if (
    signal === "BUY"
  ) {
    const low =
      Math.min(
        ...recentM5.map(
          c => c.low
        )
      );

    sl =
      low -
      0.2 * atrValue;

    const risk =
      entry - sl;

    tp =
      entry +
      2 * risk;
  } else if (
    signal === "SELL"
  ) {
    const high =
      Math.max(
        ...recentM5.map(
          c => c.high
        )
      );

    sl =
      high +
      0.2 * atrValue;

    const risk =
      sl - entry;

    tp =
      entry -
      2 * risk;
  } else {
    sl = null;

    tp = null;
  }

  return {
    symbol,

    displayName,

    price: entry,

    signal,

    score,

    entry,

    sl,

    tp,

    h1Direction,

    m15Direction,

    m5Direction,

    m15Structure,

    m5Structure,

    liquidity,

    rsi: rsiValue,

    burst,

    burstSupport,

    updatedAt:
      new Date().toISOString()
  };
}

/*
=========================================================
SCAN ALL MARKETS
=========================================================
*/

async function scanMarkets() {
  try {
    state.error = null;

    const symbols =
      await getVolatilitySymbols();

    console.log(
      `Scanning ${symbols.length} Volatility indices...`
    );

    const results = [];

    for (
      const symbolInfo of symbols
    ) {
      try {
        const market =
          await analyzeMarket(
            symbolInfo
          );

        results.push(
          market
        );

        /*
        BURST WARNING
        */

        if (
          market.burst.active &&
          shouldSendBurstWarning(
            market.symbol,
            market.burst
          )
        ) {
          await sendBurstWarning(
            market
          );
        }

        /*
        NORMAL TRADE ALERT
        */

        if (
          market.signal ===
            "BUY" ||
          market.signal ===
            "SELL"
        ) {
          await sendTradeAlert(
            market
          );
        }

        await sleep(
          REQUEST_DELAY_MS
        );
      } catch (error) {
        console.log(
          `Error scanning ${symbolInfo.symbol}:`,
          error.message
        );
      }
    }

    state.markets =
      results;

    state.lastScan =
      new Date().toISOString();

    state.online = true;

    console.log(
      `Scan complete: ${results.length} markets`
    );
  } catch (error) {
    state.error =
      error.message;

    state.online = false;

    console.log(
      "Scanner error:",
      error.message
    );
  }
}

/*
=========================================================
API STATUS
=========================================================
*/

app.get(
  "/api/status",
  (req, res) => {
    res.json({
      ok: true,

      online:
        state.online,

      derivConnected:
        state.derivConnected,

      lastScan:
        state.lastScan,

      error:
        state.error,

      marketCount:
        state.markets.length,

      markets:
        state.markets
    });
  }
);

/*
=========================================================
HEALTH
=========================================================
*/

app.get(
  "/health",
  (req, res) => {
    res.json({
      ok: true,
      online: true,
      derivConnected:
        state.derivConnected
    });
  }
);

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

/*
=========================================================
START SERVER
=========================================================
*/

app.listen(
  PORT,
  "0.0.0.0",
  () => {
    console.log(
      `Deriv Volatility Burst Fader running on port ${PORT}`
    );

    /*
    Initial scan
    */

    setTimeout(
      () => {
        scanMarkets();
      },
      3000
    );

    /*
    Repeat scan
    */

    setInterval(
      () => {
        scanMarkets();
      },
      SCAN_INTERVAL_MS
    );
  }
);
