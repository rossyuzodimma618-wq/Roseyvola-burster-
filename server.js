const express = require("express");
const path = require("path");
const WebSocket = require("ws");

const app = express();

app.use(express.json());
app.use(express.static(path.join(__dirname, "public")));

const PORT = process.env.PORT || 3000;

/*
=========================================================
DERIV
=========================================================
*/

const DERIV_WS_URL =
  "wss://api.derivws.com/trading/v1/options/ws/public";

/*
=========================================================
TELEGRAM
=========================================================
*/

const TELEGRAM_BOT_TOKEN = process.env.TELEGRAM_BOT_TOKEN;
const TELEGRAM_CHAT_ID = process.env.TELEGRAM_CHAT_ID;

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

/*
=========================================================
BURST WARNING STATE
=========================================================

The warning system is NOT a trade filter.

It simply warns:

Stack 2 = EARLY BURST WARNING
Stack 3+ = STRONG BURST WARNING

It does not stop normal BUY/SELL signals.
=========================================================
*/

const burstWarningState = new Map();

/*
=========================================================
UTILITY
=========================================================
*/

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function round(value, decimals = 3) {
  if (!Number.isFinite(value)) return null;

  const factor = Math.pow(10, decimals);

  return Math.round(value * factor) / factor;
}

function formatPrice(value, decimals = 3) {
  if (!Number.isFinite(value)) return "N/A";

  return Number(value).toFixed(decimals);
}

/*
=========================================================
SYMBOL DECIMALS
=========================================================
*/

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

/*
=========================================================
DERIV REQUEST
=========================================================
*/

function derivRequest(requestPayload) {
  return new Promise((resolve, reject) => {
    let settled = false;

    const ws = new WebSocket(DERIV_WS_URL);

    const timeout = setTimeout(() => {
      if (settled) return;

      settled = true;

      try {
        ws.close();
      } catch (_) {}

      reject(
        new Error(
          `Deriv request timeout: ${JSON.stringify(requestPayload)}`
        )
      );
    }, REQUEST_TIMEOUT_MS);

    function finishError(error) {
      if (settled) return;

      settled = true;
      clearTimeout(timeout);

      try {
        ws.close();
      } catch (_) {}

      reject(error);
    }

    function finishSuccess(data) {
      if (settled) return;

      settled = true;
      clearTimeout(timeout);

      try {
        ws.close();
      } catch (_) {}

      resolve(data);
    }

    ws.on("open", () => {
      state.derivConnected = true;

      console.log(
        "Deriv request:",
        JSON.stringify(requestPayload)
      );

      try {
        ws.send(JSON.stringify(requestPayload));
      } catch (error) {
        finishError(error);
      }
    });

    ws.on("message", raw => {
      let data;

      try {
        data = JSON.parse(raw.toString());
      } catch (error) {
        return finishError(
          new Error("Invalid JSON received from Deriv")
        );
      }

      if (data.error) {
        return finishError(
          new Error(
            data.error.message ||
            data.error.code ||
            "Deriv API error"
          )
        );
      }

      finishSuccess(data);
    });

    ws.on("error", error => {
      state.derivConnected = false;

      finishError(
        new Error(
          `Deriv WebSocket error: ${error.message}`
        )
      );
    });

    ws.on("close", () => {
      if (!settled) {
        state.derivConnected = false;
      }
    });
  });
}

/*
=========================================================
ACTIVE VOLATILITY SYMBOLS
=========================================================
*/

async function getActiveSymbols() {
  const data = await derivRequest({
    active_symbols: "brief"
  });

  const symbols = Array.isArray(data.active_symbols)
    ? data.active_symbols
    : [];

  const result = [];

  for (const item of symbols) {
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
      `${symbol} ${name} ${type} ${market}`.toLowerCase();

    const isVolatility =
      combined.includes("volatility") ||
      /^1hz\d+v/i.test(symbol) ||
      /^r_\d+/i.test(symbol);

    if (!isVolatility) continue;

    result.push({
      symbol,
      name
    });
  }

  const unique = [];
  const seen = new Set();

  for (const item of result) {
    if (!item.symbol) continue;

    const key = item.symbol.toUpperCase();

    if (seen.has(key)) continue;

    seen.add(key);
    unique.push(item);
  }

  return unique;
}

/*
=========================================================
CANDLE REQUEST
=========================================================
*/

async function requestCandles(
  symbol,
  granularity,
  subscribeValue
) {
  const payload = {
    ticks_history: symbol,
    end: "latest",
    count: CANDLE_COUNT,
    style: "candles",
    granularity
  };

  if (subscribeValue !== undefined) {
    payload.subscribe = subscribeValue;
  }

  const data = await derivRequest(payload);

  if (!Array.isArray(data.candles)) {
    throw new Error(
      `No candles returned for ${symbol} ${granularity}`
    );
  }

  return data;
}

/*
=========================================================
GET CANDLES
=========================================================
*/

async function getCandles(symbol, granularity) {
  try {
    return await requestCandles(
      symbol,
      granularity
    );
  } catch (firstError) {
    const message = String(
      firstError.message || ""
    );

    if (!/subscribe/i.test(message)) {
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
      const message2 = String(
        secondError.message || ""
      );

      if (!/subscribe/i.test(message2)) {
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

/*
=========================================================
CLEAN CANDLES
=========================================================
*/

function cleanCandles(data) {
  const candles = Array.isArray(data.candles)
    ? data.candles
        .map(c => ({
          epoch: Number(c.epoch),
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
        .sort((a, b) => a.epoch - b.epoch)
    : [];

  /*
    Remove the currently forming candle.

    We only analyse completed candles.
  */

  if (candles.length > 1) {
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
  if (!candles || candles.length < period + 1) {
    return null;
  }

  const trs = [];

  for (let i = 1; i < candles.length; i++) {
    const current = candles[i];
    const previous = candles[i - 1];

    const tr = Math.max(
      current.high - current.low,
      Math.abs(current.high - previous.close),
      Math.abs(current.low - previous.close)
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
  if (!candles || candles.length < period + 1) {
    return null;
  }

  let gains = 0;
  let losses = 0;

  for (let i = 1; i <= period; i++) {
    const change =
      candles[i].close -
      candles[i - 1].close;

    if (change >= 0) {
      gains += change;
    } else {
      losses += Math.abs(change);
    }
  }

  let averageGain = gains / period;
  let averageLoss = losses / period;

  for (
    let i = period + 1;
    i < candles.length;
    i++
  ) {
    const change =
      candles[i].close -
      candles[i - 1].close;

    const gain = Math.max(change, 0);
    const loss = Math.max(-change, 0);

    averageGain =
      ((averageGain * (period - 1)) + gain) /
      period;

    averageLoss =
      ((averageLoss * (period - 1)) + loss) /
      period;
  }

  if (averageLoss === 0) {
    return 100;
  }

  const rs = averageGain / averageLoss;

  return 100 - (100 / (1 + rs));
}

/*
=========================================================
DIRECTION
=========================================================
*/

function getDirection(candles) {
  if (!candles || candles.length < 25) {
    return "NEUTRAL";
  }

  const closes = candles.map(c => c.close);

  const basis = ema(closes, 20);

  const last = candles[candles.length - 1];

  const lookback =
    candles[candles.length - 6];

  if (!Number.isFinite(basis)) {
    return "NEUTRAL";
  }

  if (
    last.close > basis &&
    last.close > lookback.close
  ) {
    return "BULLISH";
  }

  if (
    last.close < basis &&
    last.close < lookback.close
  ) {
    return "BEARISH";
  }

  return "NEUTRAL";
}

/*
=========================================================
STRUCTURE / BOS / CHoCH
=========================================================
*/

function getStructure(candles) {
  if (!candles || candles.length < 15) {
    return "NEUTRAL";
  }

  const last = candles[candles.length - 1];

  const previous = candles.slice(
    Math.max(0, candles.length - 12),
    candles.length - 2
  );

  if (!previous.length) {
    return "NEUTRAL";
  }

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

  const before = candles[
    candles.length - 2
  ];

  if (
    last.close > before.close &&
    last.high >= before.high
  ) {
    return "BULLISH CONTINUATION";
  }

  if (
    last.close < before.close &&
    last.low <= before.low
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

function getLiquiditySweep(candles) {
  if (!candles || candles.length < 15) {
    return "NONE";
  }

  const last = candles[candles.length - 1];

  const previous = candles.slice(
    Math.max(0, candles.length - 11),
    candles.length - 1
  );

  if (!previous.length) {
    return "NONE";
  }

  const previousLow = Math.min(
    ...previous.map(c => c.low)
  );

  const previousHigh = Math.max(
    ...previous.map(c => c.high)
  );

  /*
    Bullish liquidity sweep:
    price takes previous low and closes back above it.
  */

  if (
    last.low < previousLow &&
    last.close > previousLow
  ) {
    return "BULLISH LIQUIDITY SWEEP";
  }

  /*
    Bearish liquidity sweep:
    price takes previous high and closes back below it.
  */

  if (
    last.high > previousHigh &&
    last.close < previousHigh
  ) {
    return "BEARISH LIQUIDITY SWEEP";
  }

  return "NONE";
}

/*
=========================================================
STRONG BURST FADER
=========================================================

This is the server-side version of the Pine idea:

EMA20
ATR14
Upper = EMA20 + 2 ATR
Lower = EMA20 - 2 ATR

Stack:
1 = first burst
2 = EARLY WARNING
3 = STRONG WARNING
4-6 = EXTREME area

The Fader assumes:
UP burst -> possible SELL reversal
DOWN burst -> possible BUY reversal

IMPORTANT:
It is SUPPORTING information.
It does NOT force or block a trade.
=========================================================
*/

function getBurstFader(candles) {
  if (!candles || candles.length < 30) {
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

  /*
    Last candle is already completed because
    cleanCandles() removes the live candle.
  */

  const last = candles[candles.length - 1];

  const closes = candles.map(c => c.close);

  const basis = ema(closes, 20);

  const atrValue = atr(candles, 14);

  if (
    !Number.isFinite(basis) ||
    !Number.isFinite(atrValue) ||
    atrValue <= 0
  ) {
    return {
      signal: "NEUTRAL",
      heat: "COOL",
      stack: 0,
      fadeBias: "NEUTRAL",
      strength: 0,
      basis,
      upper: null,
      lower: null
    };
  }

  const upper =
    basis + (2 * atrValue);

  const lower =
    basis - (2 * atrValue);

  let signal = "NEUTRAL";
  let stack = 0;
  let strength = 0;

  /*
    UP BURST
  */

  if (last.high > upper) {
    signal = "UP BURST";

    const extension =
      last.high - upper;

    stack = Math.ceil(
      extension / (0.5 * atrValue)
    );

    stack = Math.max(
      1,
      Math.min(6, stack)
    );

    strength = stack;
  }

  /*
    DOWN BURST
  */

  else if (last.low < lower) {
    signal = "DOWN BURST";

    const extension =
      lower - last.low;

    stack = Math.ceil(
      extension / (0.5 * atrValue)
    );

    stack = Math.max(
      1,
      Math.min(6, stack)
    );

    strength = stack;
  }

  let heat = "COOL";

  if (stack >= 5) {
    heat = "EXTREME";
  } else if (stack >= 3) {
    heat = "HOT";
  } else if (stack >= 1) {
    heat = "WARM";
  }

  let fadeBias = "NEUTRAL";

  if (signal === "UP BURST") {
    fadeBias = "BEARISH";
  }

  if (signal === "DOWN BURST") {
    fadeBias = "BULLISH";
  }

  return {
    signal,
    heat,
    stack,
    fadeBias,
    strength,
    basis,
    upper,
    lower
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
  structure
) {
  let supportsBuy = false;
  let supportsSell = false;

  if (
    burst.signal === "DOWN BURST" &&
    (
      liquidity === "BULLISH LIQUIDITY SWEEP" ||
      structure.startsWith("BULLISH")
    )
  ) {
    supportsBuy = true;
  }

  if (
    burst.signal === "UP BURST" &&
    (
      liquidity === "BEARISH LIQUIDITY SWEEP" ||
      structure.startsWith("BEARISH")
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
BURST WARNING STAGE
=========================================================
*/

function getBurstWarningStage(result) {
  if (
    !result ||
    !result.burstSignal ||
    result.burstSignal === "NEUTRAL"
  ) {
    return null;
  }

  if (!Number.isFinite(result.burstStack)) {
    return null;
  }

  /*
    Stack 1:
    normal burst information only.

    Stack 2:
    EARLY WARNING.

    Stack 3+:
    STRONG WARNING.
  */

  if (result.burstStack >= 3) {
    return "STRONG";
  }

  if (result.burstStack >= 2) {
    return "EARLY";
  }

  return null;
}

/*
=========================================================
SHOULD SEND BURST WARNING
=========================================================

Important anti-spam logic:

- First stack 2 -> warning
- Stack 3 -> strong warning
- Stack 4/5/6 -> no repeated warnings
- Stack drops from 3 to 2 -> no downgrade warning
- Burst disappears -> state resets
- New burst later -> warning can fire again
- Burst changes direction -> new warning
=========================================================
*/

function shouldSendBurstWarning(result) {
  const stage =
    getBurstWarningStage(result);

  /*
    No meaningful warning.
    Reset so the next fresh burst can warn.
  */

  if (!stage) {
    burstWarningState.delete(result.symbol);
    return false;
  }

  const previous =
    burstWarningState.get(result.symbol);

  /*
    First warning for this burst.
  */

  if (!previous) {
    burstWarningState.set(
      result.symbol,
      {
        signal: result.burstSignal,
        stage,
        stack: result.burstStack,
        time: Date.now()
      }
    );

    return true;
  }

  /*
    Burst direction changed.
  */

  if (
    previous.signal !==
    result.burstSignal
  ) {
    burstWarningState.set(
      result.symbol,
      {
        signal: result.burstSignal,
        stage,
        stack: result.burstStack,
        time: Date.now()
      }
    );

    return true;
  }

  /*
    Upgrade:
    EARLY -> STRONG
  */

  if (
    previous.stage === "EARLY" &&
    stage === "STRONG"
  ) {
    burstWarningState.set(
      result.symbol,
      {
        signal: result.burstSignal,
        stage,
        stack: result.burstStack,
        time: Date.now()
      }
    );

    return true;
  }

  /*
    No new warning.
  */

  return false;
}

/*
=========================================================
TELEGRAM REQUEST
=========================================================
*/

async function sendTelegramMessage(message) {
  if (
    !TELEGRAM_BOT_TOKEN ||
    !TELEGRAM_CHAT_ID
  ) {
    console.log(
      "Telegram credentials not configured."
    );

    return false;
  }

  const url =
    `https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage`;

  try {
    const response = await fetch(url, {
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

    const data = await response.json();

    if (!response.ok || !data.ok) {
      console.error(
        "Telegram error:",
        data
      );

      return false;
    }

    return true;
  } catch (error) {
    console.error(
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

async function sendBurstWarning(result) {
  if (
    !TELEGRAM_BOT_TOKEN ||
    !TELEGRAM_CHAT_ID
  ) {
    return false;
  }

  const stage =
    getBurstWarningStage(result);

  if (!stage) {
    return false;
  }

  if (!shouldSendBurstWarning(result)) {
    return false;
  }

  const isUp =
    result.burstSignal === "UP BURST";

  const possibleDirection =
    isUp ? "SELL" : "BUY";

  const emoji =
    stage === "STRONG"
      ? "🚨"
      : "⚠️";

  const title =
    stage === "STRONG"
      ? "STRONG BURST WARNING"
      : "EARLY BURST WARNING";

  const message =
`${emoji} ${title}

📌 ${result.symbol}

⚡ Burst: ${result.burstSignal}
🔥 Heat: ${result.burstHeat}
📈 Stack: ${result.burstStack}/6
🧭 Possible reversal: ${possibleDirection}

📊 H1: ${result.h1Direction}
📊 M15: ${result.m15Direction}
📊 M5: ${result.m5Direction}

🏗️ M15 Structure:
${result.m15Structure}

🏗️ M5 Structure:
${result.m5Structure}

🔎 Liquidity:
${result.liquidity}

💡 Burst Fader:
${result.burstFadeBias}

⏳ WAIT FOR CONFIRMATION

This is a warning, NOT an entry signal.

The bot will still wait for its normal H1 → M15 → M5 confirmation before sending a BUY/SELL signal.`;

  return sendTelegramMessage(message);
}

/*
=========================================================
NORMAL ALERT
=========================================================
*/

async function sendTelegramAlert(result) {
  if (
    !TELEGRAM_BOT_TOKEN ||
    !TELEGRAM_CHAT_ID
  ) {
    return false;
  }

  if (
    result.signal !== "BUY" &&
    result.signal !== "SELL"
  ) {
    return false;
  }

  const now = Date.now();

  const previous =
    alertState.get(result.symbol) || 0;

  /*
    Normal trade alert cooldown.
  */

  if (
    now - previous <
    ALERT_COOLDOWN_MS
  ) {
    return false;
  }

  alertState.set(
    result.symbol,
    now
  );

  const decimals =
    getDecimals(result.symbol);

  const emoji =
    result.signal === "BUY"
      ? "🟢"
      : "🔴";

  const message =
`${emoji} STRONG ${result.signal}: ${result.symbol}

📍 Entry: ${formatPrice(result.entry, decimals)}
🛑 Stop Loss: ${formatPrice(result.stopLoss, decimals)}
🎯 Take Profit: ${formatPrice(result.takeProfit, decimals)}

⭐ Score: ${result.score}/${result.maxScore}

📊 H1: ${result.h1Direction}
📊 M15: ${result.m15Direction}
📊 M5: ${result.m5Direction}

🏗️ M15 Structure:
${result.m15Structure}

🏗️ M5 Structure:
${result.m5Structure}

🔎 Liquidity:
${result.liquidity}

⚡ Burst Fader:
${result.burstSignal}

🔥 Burst Heat:
${result.burstHeat}

📈 Burst Stack:
${result.burstStack}/6

🧭 Burst Fade Bias:
${result.burstFadeBias}

RSI: ${Number.isFinite(result.rsi) ? result.rsi.toFixed(1) : "N/A"}

⏱️ 1:2 Risk/Reward`;

  return sendTelegramMessage(message);
}

/*
=========================================================
ANALYSE ONE SYMBOL
=========================================================
*/

async function analyzeSymbol(symbol) {
  console.log(
    `Analyzing ${symbol}...`
  );

  /*
    H1
  */

  const h1Data =
    await getCandles(
      symbol,
      H1_GRANULARITY
    );

  await sleep(250);

  /*
    M15
  */

  const m15Data =
    await getCandles(
      symbol,
      M15_GRANULARITY
    );

  await sleep(250);

  /*
    M5
  */

  const m5Data =
    await getCandles(
      symbol,
      M5_GRANULARITY
    );

  const h1 =
    cleanCandles(h1Data);

  const m15 =
    cleanCandles(m15Data);

  const m5 =
    cleanCandles(m5Data);

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

  /*
    BURST SUPPORT
  */

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
    ATR
  */

  const atrValue =
    atr(m5, 14);

  /*
    SCORE
  */

  let buyScore = 0;
  let sellScore = 0;

  /*
    H1
  */

  if (h1Direction === "BULLISH") {
    buyScore++;
  }

  if (h1Direction === "BEARISH") {
    sellScore++;
  }

  /*
    M15 direction
  */

  if (m15Direction === "BULLISH") {
    buyScore++;
  }

  if (m15Direction === "BEARISH") {
    sellScore++;
  }

  /*
    M15 structure
  */

  if (
    m15Structure.startsWith("BULLISH")
  ) {
    buyScore++;
  }

  if (
    m15Structure.startsWith("BEARISH")
  ) {
    sellScore++;
  }

  /*
    M5 structure
  */

  if (
    m5Structure.startsWith("BULLISH")
  ) {
    buyScore++;
  }

  if (
    m5Structure.startsWith("BEARISH")
  ) {
    sellScore++;
  }

  /*
    Burst support
  */

  if (burstSupport.supportsBuy) {
    buyScore++;
  }

  if (burstSupport.supportsSell) {
    sellScore++;
  }

  /*
    Liquidity
  */

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

  /*
    Maximum score remains 5.

    This prevents the extra confirmations
    from making the bot artificially stricter.
  */

  buyScore =
    Math.min(5, buyScore);

  sellScore =
    Math.min(5, sellScore);

  /*
    FINAL SIGNAL

    Burst Fader is NOT mandatory.
  */

  let signal = "WAIT";

  if (
    h1Direction === "BULLISH" &&
    m15Direction === "BULLISH" &&
    (
      m15Structure.startsWith("BULLISH") ||
      m5Structure.startsWith("BULLISH") ||
      burstSupport.supportsBuy
    ) &&
    buyScore >= 3
  ) {
    signal = "BUY";
  }

  if (
    h1Direction === "BEARISH" &&
    m15Direction === "BEARISH" &&
    (
      m15Structure.startsWith("BEARISH") ||
      m5Structure.startsWith("BEARISH") ||
      burstSupport.supportsSell
    ) &&
    sellScore >= 3
  ) {
    signal = "SELL";
  }

  /*
    FINAL SCORE
  */

  const score =
    signal === "BUY"
      ? buyScore
      : signal === "SELL"
        ? sellScore
        : Math.max(
            buyScore,
            sellScore
          );

  /*
    ENTRY
  */

  const lastM5 =
    m5[m5.length - 1];

  const entry =
    lastM5.close;

  /*
    STOP LOSS
  */

  const recentM5 =
    m5.slice(
      Math.max(0, m5.length - 8)
    );

  const recentLow =
    Math.min(
      ...recentM5.map(c => c.low)
    );

  const recentHigh =
    Math.max(
      ...recentM5.map(c => c.high)
    );

  let stopLoss = null;
  let takeProfit = null;

  const decimals =
    getDecimals(symbol);

  if (
    signal === "BUY" &&
    Number.isFinite(atrValue)
  ) {
    stopLoss =
      recentLow -
      (atrValue * 0.2);

    const risk =
      entry - stopLoss;

    takeProfit =
      entry +
      (risk * 2);
  }

  if (
    signal === "SELL" &&
    Number.isFinite(atrValue)
  ) {
    stopLoss =
      recentHigh +
      (atrValue * 0.2);

    const risk =
      stopLoss - entry;

    takeProfit =
      entry -
      (risk * 2);
  }

  /*
    BURST WARNING STAGE
  */

  let burstWarningStage =
    "NONE";

  if (burst.stack >= 3) {
    burstWarningStage =
      "STRONG";
  } else if (burst.stack >= 2) {
    burstWarningStage =
      "EARLY";
  }

  /*
    RETURN RESULT
  */

  return {
    symbol,

    price: round(
      entry,
      decimals
    ),

    signal,

    score,

    maxScore: 5,

    entry:
      Number.isFinite(entry)
        ? round(entry, decimals)
        : null,

    stopLoss:
      Number.isFinite(stopLoss)
        ? round(stopLoss, decimals)
        : null,

    takeProfit:
      Number.isFinite(takeProfit)
        ? round(takeProfit, decimals)
        : null,

    h1: h1Direction,
    h1Direction,

    m15: m15Direction,
    m15Direction,

    m5: m5Direction,
    m5Direction,

    m15Structure,
    m5Structure,

    liquidity,

    liquiditySweep: liquidity,

    burstFader: burst.signal,

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
      burstSupport.supportsBuy,

    burstSupportsSell:
      burstSupport.supportsSell,

    burstWarningStage,

    rsi:
      Number.isFinite(rsiValue)
        ? round(rsiValue, 2)
        : null,

    atr:
      Number.isFinite(atrValue)
        ? round(atrValue, decimals)
        : null,

    timestamp:
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
      await getActiveSymbols();

    console.log(
      `Found ${symbols.length} Volatility symbols.`
    );

    const results = [];

    /*
      Sequential scanning helps reduce
      WebSocket/API pressure.
    */

    for (const item of symbols) {
      try {
        const result =
          await analyzeSymbol(
            item.symbol
          );

        results.push(result);

        /*
          BURST WARNING FIRST.

          This means the user can receive:
          ⚠️ Burst warning
          before a later normal signal.

          It does NOT change the signal itself.
        */

        await sendBurstWarning(
          result
        );

        /*
          Normal trade alert.
        */

        await sendTelegramAlert(
          result
        );

        await sleep(250);
      } catch (error) {
        console.error(
          `Analysis failed for ${item.symbol}:`,
          error.message
        );

        results.push({
          symbol: item.symbol,
          signal: "ERROR",
          score: 0,
          maxScore: 5,
          error: error.message,
          timestamp:
            new Date().toISOString()
        });
      }
    }

    state.markets = results;
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

    console.error(
      "Scan error:",
      error.message
    );
  }
}

/*
=========================================================
API STATUS
=========================================================
*/

app.get("/api/status", (req, res) => {
  res.json({
    ok: true,

    online:
      state.online,

    derivConnected:
      state.derivConnected,

    marketOpen: true,

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
HEALTH
=========================================================
*/

app.get("/health", (req, res) => {
  res.json({
    ok: true,
    online: true,
    derivConnected:
      state.derivConnected,
    time:
      new Date().toISOString()
  });
});

/*
=========================================================
ROOT
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
      Start first scan immediately.
    */

    scanMarkets();

    /*
      Continue scanning every 30 seconds.
    */

    setInterval(
      scanMarkets,
      SCAN_INTERVAL_MS
    );
  }
);
