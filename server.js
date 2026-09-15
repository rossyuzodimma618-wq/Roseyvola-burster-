const express = require("express");
const path = require("path");
const WebSocket = require("ws");

const app = express();

const PORT = process.env.PORT || 3000;

const TELEGRAM_BOT_TOKEN =
  process.env.TELEGRAM_BOT_TOKEN;

const TELEGRAM_CHAT_ID =
  process.env.TELEGRAM_CHAT_ID;

/*
=========================================================
DERIV CONNECTION
=========================================================
*/

const DERIV_WS_URL =
  "wss://ws.binaryws.com/websockets/v3?app_id=1089";

/*
=========================================================
TIMEFRAMES
=========================================================
*/

const H1 = 3600;
const M15 = 900;
const M5 = 300;

/*
=========================================================
SCANNER SETTINGS
=========================================================
*/

const SCAN_INTERVAL = 30000;

const ALERT_COOLDOWN =
  15 * 60 * 1000;

const REQUEST_TIMEOUT = 15000;

const RECONNECT_DELAY = 5000;

/*
=========================================================
STATE
=========================================================
*/

let derivWs = null;

let requestId = 1;

const pendingRequests = new Map();

let symbols = [];

let scans = [];

let recentAlerts = [];

let lastScan = null;

let connected = false;

let scanning = false;

let scannerStarted = false;

let reconnectTimer = null;

const lastAlertTime = {};

/*
=========================================================
EXPRESS
=========================================================
*/

app.use(express.json());

app.use(
  express.static(
    path.join(__dirname, "public")
  )
);

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

/*
=========================================================
HEALTH
=========================================================
*/

app.get("/health", (req, res) => {
  res.json({
    ok: true,
    online: true,
    connected,
    derivConnected: connected,
    symbols: symbols.length,
    symbolCount: symbols.length,
    lastScan
  });
});

/*
=========================================================
STATUS API
=========================================================
*/

app.get("/api/status", (req, res) => {
  res.json({
    ok: true,

    online: true,

    connected,

    derivConnected: connected,

    symbolCount:
      symbols.length,

    symbols: scans,

    scans,

    alerts:
      recentAlerts,

    recentAlerts,

    lastScan
  });
});

/*
=========================================================
CONNECT TO DERIV
=========================================================
*/

function connectDeriv() {
  if (reconnectTimer) {
    clearTimeout(reconnectTimer);
    reconnectTimer = null;
  }

  /*
  Close old connection
  */

  if (derivWs) {
    try {
      derivWs.removeAllListeners();
      derivWs.close();
    } catch (error) {}
  }

  connected = false;

  console.log(
    "Connecting to Deriv..."
  );

  derivWs = new WebSocket(
    DERIV_WS_URL,
    {
      handshakeTimeout: 15000
    }
  );

  /*
  ========================================================
  OPEN
  ========================================================
  */

  derivWs.on("open", () => {
    connected = true;

    console.log(
      "Connected to Deriv."
    );

    /*
    Request active symbols.
    */

    sendRequest({
      active_symbols: "brief",
      product_type: "basic"
    })
      .then((data) => {
        processActiveSymbols(data);

        startScanner();

        /*
        Run first scan shortly after
        symbols have been loaded.
        */

        setTimeout(
          runScanner,
          2000
        );
      })
      .catch((error) => {
        console.error(
          "Active symbols error:",
          error.message
        );

        /*
        Keep scanner alive.
        */

        startScanner();
      });
  });

  /*
  ========================================================
  MESSAGE
  ========================================================
  */

  derivWs.on("message", (raw) => {
    try {
      const data =
        JSON.parse(
          raw.toString()
        );

      /*
      Log API errors clearly.
      */

      if (data.error) {
        console.error(
          "Deriv API error:",
          data.error.message ||
            JSON.stringify(
              data.error
            )
        );
      }

      /*
      Match response to request.
      */

      if (
        data.req_id &&
        pendingRequests.has(
          data.req_id
        )
      ) {
        const request =
          pendingRequests.get(
            data.req_id
          );

        pendingRequests.delete(
          data.req_id
        );

        if (data.error) {
          request.reject(
            new Error(
              data.error.message ||
                "Deriv API error"
            )
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

  /*
  ========================================================
  CLOSE
  ========================================================
  */

  derivWs.on("close", (code, reason) => {
    connected = false;

    console.log(
      `Deriv connection closed. Code: ${code}`
    );

    if (reason) {
      console.log(
        `Reason: ${reason.toString()}`
      );
    }

    rejectAllPending(
      "Deriv connection closed"
    );

    scheduleReconnect();
  });

  /*
  ========================================================
  ERROR
  ========================================================
  */

  derivWs.on("error", (error) => {
    connected = false;

    console.error(
      "Deriv WebSocket error:",
      error.message
    );
  });
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

  reconnectTimer =
    setTimeout(() => {
      reconnectTimer = null;

      connectDeriv();
    }, RECONNECT_DELAY);
}

/*
=========================================================
REJECT PENDING REQUESTS
=========================================================
*/

function rejectAllPending(
  message
) {
  for (
    const [
      id,
      request
    ] of pendingRequests
  ) {
    try {
      request.reject(
        new Error(message)
      );
    } catch (error) {}

    pendingRequests.delete(id);
  }
}

/*
=========================================================
DERIV REQUEST
=========================================================
*/

function sendRequest(payload) {
  return new Promise(
    (resolve, reject) => {
      if (
        !derivWs ||
        derivWs.readyState !==
          WebSocket.OPEN
      ) {
        reject(
          new Error(
            "Deriv WebSocket not connected"
          )
        );

        return;
      }

      const req_id =
        requestId++;

      pendingRequests.set(
        req_id,
        {
          resolve,
          reject
        }
      );

      try {
        derivWs.send(
          JSON.stringify({
            ...payload,
            req_id
          })
        );
      } catch (error) {
        pendingRequests.delete(
          req_id
        );

        reject(error);

        return;
      }

      setTimeout(() => {
        if (
          pendingRequests.has(
            req_id
          )
        ) {
          pendingRequests.delete(
            req_id
          );

          reject(
            new Error(
              "Deriv request timeout"
            )
          );
        }
      }, REQUEST_TIMEOUT);
    }
  );
}

/*
=========================================================
PROCESS ACTIVE SYMBOLS
=========================================================
*/

function processActiveSymbols(
  data
) {
  if (
    !data ||
    !Array.isArray(
      data.active_symbols
    )
  ) {
    console.error(
      "Deriv returned no active symbols."
    );

    return;
  }

  const found =
    data.active_symbols
      .filter(
        isVolatilitySymbol
      )
      .map((item) => {
        /*
        Newer field names
        */

        const symbol =
          item.underlying_symbol ||
          item.symbol ||
          "";

        const displayName =
          item.underlying_symbol_name ||
          item.display_name ||
          symbol;

        return {
          symbol,
          displayName
        };
      })
      .filter(
        (item) =>
          item.symbol
      );

  /*
  Remove duplicate symbols.
  */

  const unique =
    new Map();

  for (
    const item of found
  ) {
    unique.set(
      item.symbol,
      item
    );
  }

  symbols =
    Array.from(
      unique.values()
    );

  console.log(
    `Found ${symbols.length} Volatility indices.`
  );

  /*
  Print first few symbols
  so Railway logs are easy to inspect.
  */

  if (symbols.length > 0) {
    console.log(
      "Volatility symbols:",
      symbols
        .slice(0, 15)
        .map(
          (item) =>
            item.symbol
        )
        .join(", ")
    );
  }
}

/*
=========================================================
VOLATILITY SYMBOL FILTER
=========================================================
*/

function isVolatilitySymbol(
  item
) {
  const display =
    String(
      item.underlying_symbol_name ||
        item.display_name ||
        ""
    ).toLowerCase();

  const symbol =
    String(
      item.underlying_symbol ||
        item.symbol ||
        ""
    ).toUpperCase();

  /*
  Standard Volatility names
  */

  if (
    display.includes(
      "volatility"
    )
  ) {
    return true;
  }

  /*
  Modern synthetic symbols
  */

  if (
    /^1HZ[0-9]+V$/.test(symbol)
  ) {
    return true;
  }

  /*
  Older synthetic symbols
  */

  if (
    /^R_[0-9]+$/.test(symbol)
  ) {
    return true;
  }

  if (
    /^HZ[0-9]+V$/.test(symbol)
  ) {
    return true;
  }

  return false;
}

/*
=========================================================
CANDLE DATA
=========================================================
*/

async function getCandles(
  symbol,
  granularity
) {
  const data =
    await sendRequest({
      ticks_history:
        symbol,

      style:
        "candles",

      granularity,

      count: 250,

      end:
        "latest",

      adjust_start_time:
        1,

      subscribe:
        0
    });

  if (
    !data ||
    !Array.isArray(
      data.candles
    )
  ) {
    throw new Error(
      `No candle data for ${symbol}`
    );
  }

  const candles =
    data.candles
      .map((c) => ({
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
      .filter(
        (c) =>
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
      );

  if (
    candles.length < 30
  ) {
    throw new Error(
      `Not enough candles for ${symbol}`
    );
  }

  return candles;
}

/*
=========================================================
EMA
=========================================================
*/

function ema(
  values,
  period
) {
  if (
    !values ||
    values.length === 0
  ) {
    return null;
  }

  if (
    values.length < period
  ) {
    return (
      values[
        values.length - 1
      ]
    );
  }

  const multiplier =
    2 /
    (period + 1);

  let result =
    values
      .slice(0, period)
      .reduce(
        (a, b) =>
          a + b,
        0
      ) / period;

  for (
    let i = period;
    i < values.length;
    i++
  ) {
    result =
      (values[i] -
        result) *
        multiplier +
      result;
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
    !candles ||
    candles.length < 2
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
    return (
      trs[
        trs.length - 1
      ] || 0
    );
  }

  let value =
    trs
      .slice(0, period)
      .reduce(
        (a, b) =>
          a + b,
        0
      ) / period;

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
    !candles ||
    candles.length <
      period + 1
  ) {
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
    let i =
      period + 1;
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

  if (
    avgLoss === 0
  ) {
    return 100;
  }

  const rs =
    avgGain / avgLoss;

  return (
    100 -
    100 /
      (1 + rs)
  );
}

/*
=========================================================
STRUCTURE
=========================================================
*/

function structure(
  candles
) {
  if (
    !candles ||
    candles.length < 10
  ) {
    return "NEUTRAL";
  }

  /*
  Use completed candle.
  */

  const completed =
    candles.slice(
      0,
      candles.length - 1
    );

  if (
    completed.length < 10
  ) {
    return "NEUTRAL";
  }

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
      Math.max(
        0,
        completed.length - 12
      ),
      completed.length - 2
    );

  if (
    lookback.length < 3
  ) {
    return "NEUTRAL";
  }

  const previousHigh =
    Math.max(
      ...lookback.map(
        (c) => c.high
      )
    );

  const previousLow =
    Math.min(
      ...lookback.map(
        (c) => c.low
      )
    );

  /*
  BOS
  */

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

  /*
  EMA direction
  */

  const emaValue =
    ema(
      completed.map(
        (c) => c.close
      ),
      20
    );

  if (
    last.close >
      previous.close &&
    last.close >
      emaValue
  ) {
    return "BULLISH";
  }

  if (
    last.close <
      previous.close &&
    last.close <
      emaValue
  ) {
    return "BEARISH";
  }

  return "NEUTRAL";
}

/*
=========================================================
STRONG BURST FADER
=========================================================
*/

function burstFader(
  candles
) {
  if (
    !candles ||
    candles.length < 25
  ) {
    return {
      direction:
        "NONE",

      stack: 0,

      label:
        "NO BURST"
    };
  }

  /*
  Ignore currently forming candle.
  */

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
    ema(
      closes,
      20
    );

  const atrValue =
    atr(
      completed,
      14
    );

  if (
    !basis ||
    !atrValue ||
    atrValue <= 0
  ) {
    return {
      direction:
        "NONE",

      stack: 0,

      label:
        "NO BURST"
    };
  }

  const last =
    completed[
      completed.length - 1
    ];

  /*
  Strong Burst Fader:
  EMA20 ± 2 ATR
  */

  const upper =
    basis +
    2 * atrValue;

  const lower =
    basis -
    2 * atrValue;

  let direction =
    "NONE";

  let extension = 0;

  /*
  Upside burst
  */

  if (
    last.high >
    upper
  ) {
    direction =
      "UP";

    extension =
      (
        last.high -
        upper
      ) / atrValue;
  }

  /*
  Downside burst
  */

  if (
    last.low <
    lower
  ) {
    const downExtension =
      (
        lower -
        last.low
      ) / atrValue;

    if (
      direction ===
        "NONE" ||
      downExtension >
        extension
    ) {
      direction =
        "DOWN";

      extension =
        downExtension;
    }
  }

  /*
  Stack levels.
  */

  let stack =
    Math.min(
      6,
      Math.max(
        0,
        Math.floor(
          extension /
            0.5
        ) + 2
      )
    );

  /*
  Prevent false stack
  when there is no burst.
  */

  if (
    direction ===
    "NONE"
  ) {
    stack = 0;
  }

  let label =
    "NO BURST";

  if (
    direction ===
    "UP"
  ) {
    label =
      stack >= 5
        ? "EXTREME UP BURST"
        : stack >= 3
        ? "STRONG UP BURST"
        : "UP BURST";
  }

  if (
    direction ===
    "DOWN"
  ) {
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

/*
=========================================================
ANALYSIS
=========================================================
*/

function analyze(
  symbol,
  displayName,
  h1Candles,
  m15Candles,
  m5Candles
) {
  /*
  Structure
  */

  const h1Structure =
    structure(
      h1Candles
    );

  const m15Structure =
    structure(
      m15Candles
    );

  const m5Structure =
    structure(
      m5Candles
    );

  /*
  ========================================================
  1H DIRECTION
  ========================================================
  */

  const h1Completed =
    h1Candles.slice(
      0,
      h1Candles.length - 1
    );

  const h1Closes =
    h1Completed.map(
      (c) => c.close
    );

  const h1EMA =
    ema(
      h1Closes,
      20
    );

  const h1Last =
    h1Completed[
      h1Completed.length - 1
    ];

  let h1Direction =
    "NEUTRAL";

  if (
    h1Last.close >
    h1EMA
  ) {
    h1Direction =
      "BULLISH";
  }

  if (
    h1Last.close <
    h1EMA
  ) {
    h1Direction =
      "BEARISH";
  }

  /*
  ========================================================
  15M DIRECTION
  ========================================================
  */

  const m15Completed =
    m15Candles.slice(
      0,
      m15Candles.length - 1
    );

  const m15EMA =
    ema(
      m15Completed.map(
        (c) => c.close
      ),
      20
    );

  const m15Last =
    m15Completed[
      m15Completed.length - 1
    ];

  let m15Direction =
    "NEUTRAL";

  if (
    m15Last.close >
    m15EMA
  ) {
    m15Direction =
      "BULLISH";
  }

  if (
    m15Last.close <
    m15EMA
  ) {
    m15Direction =
      "BEARISH";
  }

  /*
  ========================================================
  5M DIRECTION
  ========================================================
  */

  const m5Completed =
    m5Candles.slice(
      0,
      m5Candles.length - 1
    );

  const m5EMA =
    ema(
      m5Completed.map(
        (c) => c.close
      ),
      20
    );

  const m5Last =
    m5Completed[
      m5Completed.length - 1
    ];

  let m5Direction =
    "NEUTRAL";

  if (
    m5Last.close >
    m5EMA
  ) {
    m5Direction =
      "BULLISH";
  }

  if (
    m5Last.close <
    m5EMA
  ) {
    m5Direction =
      "BEARISH";
  }

  /*
  ========================================================
  BURST FADER
  ========================================================
  */

  const burst =
    burstFader(
      m5Candles
    );

  /*
  ========================================================
  RSI
  ========================================================
  */

  const rsiValue =
    rsi(
      m5Completed
    );

  /*
  ========================================================
  SCORE
  ========================================================
  */

  let score = 0;

  /*
  1. H1 has direction
  */

  if (
    h1Direction ===
      "BULLISH" ||
    h1Direction ===
      "BEARISH"
  ) {
    score++;
  }

  /*
  2. 15M agrees with H1
  */

  if (
    m15Direction ===
    h1Direction
  ) {
    score++;
  }

  /*
  3. 15M structure
  */

  if (
    (
      h1Direction ===
        "BULLISH" &&
      m15Structure.includes(
        "BULLISH"
      )
    ) ||
    (
      h1Direction ===
        "BEARISH" &&
      m15Structure.includes(
        "BEARISH"
      )
    )
  ) {
    score++;
  }

  /*
  4. 5M structure
  */

  if (
    (
      h1Direction ===
        "BULLISH" &&
      m5Structure.includes(
        "BULLISH"
      )
    ) ||
    (
      h1Direction ===
        "BEARISH" &&
      m5Structure.includes(
        "BEARISH"
      )
    )
  ) {
    score++;
  }

  /*
  5. Strong Burst Fader
  SUPPORTING evidence only.
  */

  if (
    burst.stack >= 3
  ) {
    score++;
  }

  /*
  ========================================================
  SIGNAL
  ========================================================
  */

  let signal =
    "WAIT";

  /*
  IMPORTANT:
  Burst Fader is NOT a mandatory gate.
  */

  const bullishSetup =
    h1Direction ===
      "BULLISH" &&

    m15Direction ===
      "BULLISH" &&

    (
      m5Structure.includes(
        "BULLISH"
      ) ||

      burst.direction ===
        "DOWN"
    ) &&

    score >= 3;

  const bearishSetup =
    h1Direction ===
      "BEARISH" &&

    m15Direction ===
      "BEARISH" &&

    (
      m5Structure.includes(
        "BEARISH"
      ) ||

      burst.direction ===
        "UP"
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

  /*
  ========================================================
  ENTRY / SL / TP
  ========================================================
  */

  const entry =
    m5Last.close;

  const m5ATR =
    atr(
      m5Completed,
      14
    ) ||
    Math.abs(
      m5Last.high -
        m5Last.low
    );

  let sl = null;

  let tp = null;

  if (
    signal ===
    "BUY"
  ) {
    const recentLow =
      Math.min(
        ...m5Completed
          .slice(-8)
          .map(
            (c) =>
              c.low
          )
      );

    sl =
      recentLow -
      m5ATR * 0.2;

    tp =
      entry +
      (
        entry -
        sl
      ) * 2;
  }

  if (
    signal ===
    "SELL"
  ) {
    const recentHigh =
      Math.max(
        ...m5Completed
          .slice(-8)
          .map(
            (c) =>
              c.high
          )
      );

    sl =
      recentHigh +
      m5ATR * 0.2;

    tp =
      entry -
      (
        sl -
        entry
      ) * 2;
  }

  /*
  ========================================================
  RESULT
  ========================================================
  */

  return {
    symbol,

    displayName,

    price:
      entry,

    signal,

    score,

    h1:
      h1Direction,

    h1Direction,

    m15:
      m15Direction,

    m15Direction,

    m5:
      m5Direction,

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
      Number(
        rsiValue.toFixed(1)
      ),

    entry:
      signal ===
        "WAIT"
        ? null
        : entry,

    sl:
      signal ===
        "WAIT"
        ? null
        : sl,

    tp:
      signal ===
        "WAIT"
        ? null
        : tp,

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

    if (
      !response.ok
    ) {
      console.error(
        "Telegram HTTP error:",
        response.status
      );
    }
  } catch (error) {
    console.error(
      "Telegram error:",
      error.message
    );
  }
}

/*
=========================================================
FORMAT TELEGRAM ALERT
=========================================================
*/

function formatAlert(
  result
) {
  const digits =
    result.price >= 1000
      ? 2
      : result.price >= 100
      ? 3
      : 4;

  return (
    `${result.signal === "BUY" ? "🟢" : "🔴"} ` +
    `STRONG ${result.signal}: ${result.displayName}\n\n` +

    `📍 Entry: ${Number(
      result.entry
    ).toFixed(digits)}\n` +

    `🛑 Stop Loss: ${Number(
      result.sl
    ).toFixed(digits)}\n` +

    `🎯 Take Profit: ${Number(
      result.tp
    ).toFixed(digits)}\n\n` +

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

/*
=========================================================
SCAN ONE SYMBOL
=========================================================
*/

async function scanSymbol(
  item
) {
  try {
    /*
    H1
    */

    const h1Candles =
      await getCandles(
        item.symbol,
        H1
      );

    /*
    Small delay between
    requests.
    */

    await sleep(200);

    /*
    15M
    */

    const m15Candles =
      await getCandles(
        item.symbol,
        M15
      );

    await sleep(200);

    /*
    5M
    */

    const m5Candles =
      await getCandles(
        item.symbol,
        M5
      );

    /*
    Analyze
    */

    return analyze(
      item.symbol,
      item.displayName,
      h1Candles,
      m15Candles,
      m5Candles
    );
  } catch (error) {
    console.error(
      `Scan error ${item.symbol}:`,
      error.message
    );

    return {
      symbol:
        item.symbol,

      displayName:
        item.displayName,

      price:
        null,

      signal:
        "WAIT",

      score:
        0,

      h1:
        "NEUTRAL",

      h1Direction:
        "NEUTRAL",

      m15:
        "NEUTRAL",

      m15Direction:
        "NEUTRAL",

      m5:
        "NEUTRAL",

      m5Direction:
        "NEUTRAL",

      h1Structure:
        "DATA UNAVAILABLE",

      m15Structure:
        "DATA UNAVAILABLE",

      m5Structure:
        "DATA UNAVAILABLE",

      burstFader:
        "DATA UNAVAILABLE",

      burst:
        "DATA UNAVAILABLE",

      burstDirection:
        "NONE",

      burstStack:
        0,

      rsi:
        50,

      entry:
        null,

      sl:
        null,

      tp:
        null,

      error:
        error.message,

      updated:
        new Date().toISOString()
    };
  }
}

/*
=========================================================
SCANNER
=========================================================
*/

async function runScanner() {
  if (
    scanning
  ) {
    return;
  }

  if (
    !connected
  ) {
    return;
  }

  if (
    symbols.length === 0
  ) {
    console.log(
      "Scanner waiting for Volatility symbols..."
    );

    return;
  }

  scanning = true;

  try {
    const results = [];

    /*
    Sequential scanning.
    */

    for (
      const item of symbols
    ) {
      const result =
        await scanSymbol(
          item
        );

      results.push(
        result
      );

      /*
      Keep connection
      pressure lower.
      */

      await sleep(250);
    }

    scans =
      results;

    lastScan =
      new Date().toISOString();

    /*
    ======================================================
    TELEGRAM ALERTS
    ======================================================
    */

    for (
      const result of results
    ) {
      if (
        result.signal ===
          "BUY" ||
        result.signal ===
          "SELL"
      ) {
        const lastTime =
          lastAlertTime[
            result.symbol
          ] || 0;

        const now =
          Date.now();

        if (
          now -
            lastTime >=
          ALERT_COOLDOWN
        ) {
          lastAlertTime[
            result.symbol
          ] = now;

          const message =
            formatAlert(
              result
            );

          recentAlerts.unshift(
            {
              message,

              time:
                new Date().toISOString(),

              symbol:
                result.symbol
            }
          );

          recentAlerts =
            recentAlerts.slice(
              0,
              20
            );

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
    scanning =
      false;
  }
}

/*
=========================================================
START SCANNER
=========================================================
*/

function startScanner() {
  if (
    scannerStarted
  ) {
    return;
  }

  scannerStarted =
    true;

  console.log(
    "Scanner started."
  );

  setInterval(
    runScanner,
    SCAN_INTERVAL
  );
}

/*
=========================================================
SLEEP
=========================================================
*/

function sleep(
  ms
) {
  return new Promise(
    (resolve) =>
      setTimeout(
        resolve,
        ms
      )
  );
}

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

    connectDeriv();
  }
);
