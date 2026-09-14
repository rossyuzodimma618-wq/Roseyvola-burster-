# Deriv Volatility Burst Fader

Deriv public WebSocket market data is used for active symbol discovery and candle history.

## Railway
Start command: `npm start`

Optional variables:
- TELEGRAM_BOT_TOKEN
- TELEGRAM_CHAT_ID

No Deriv account token is needed for this read-only scanner.

## Timeframes
H1 = direction
M15 = structure
M5 = entry + Strong Burst Fader

This is an alert/scanner, not automatic trade execution.
