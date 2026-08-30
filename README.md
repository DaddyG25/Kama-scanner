# NIFTY 50 P-KAMA Scanner

This Railway app scans the current NIFTY 50 constituents using Zerodha Kite Connect.

## Confirmed signal

- Latest completed 60-minute candle crosses from below to above P-KAMA.
- 60-minute volume is at least `VOLUME_MULTIPLIER` times the previous 20-bar average.
- Latest completed daily candle is above P-KAMA.
- Daily volume is at least `VOLUME_MULTIPLIER` times the previous 20-day average.

The dashboard scan and the automatic scanner use the same logic. Automatic scanning runs every five minutes during NSE cash-market hours and emails only a new confirmed signal set.

## Railway variables

Set these in the Railway service:

```text
KITE_API_KEY=...
KITE_API_SECRET=...
SMTP_HOST=smtp.gmail.com
SMTP_PORT=587
SMTP_USERNAME=...
SMTP_PASSWORD=...
EMAIL_FROM=...
EMAIL_TO=...
VOLUME_MULTIPLIER=1.5
HOURLY_LOOKBACK_DAYS=60
DAILY_LOOKBACK_DAYS=365
KITE_THROTTLE_SECONDS=0.35
```

Register `https://YOUR-RAILWAY-DOMAIN/callback` as the Kite Connect app redirect URL. The normal Kite access token expires at 6 AM the next day, so reconnect through the dashboard each trading morning. Do not commit API secrets.

The existing `railway.json` starts the app with one Gunicorn worker, which is required because the automatic scheduler runs inside the web process.
