# NIFTY 500 P-KAMA Scanner

This Railway app scans the current NIFTY 500 constituents using the Upstox API.

## Confirmed signal

- Latest completed 60-minute candle crosses from below to above P-KAMA.
- 60-minute volume is at least `VOLUME_MULTIPLIER` times the previous 20-bar average.
- Latest completed daily candle crosses from below to above P-KAMA.
- Daily volume is at least `VOLUME_MULTIPLIER` times the previous 20-day average.

The dashboard scan and the automatic scanner use the same logic. Automatic scanning runs at `09:15`, `10:15`, `11:15`, `12:15`, `13:15`, `14:15`, and `15:15` IST during NSE cash-market hours and emails only a new confirmed signal set. A NIFTY 500 scan is larger than NIFTY 50 and can take several minutes.

## Railway variables

Set these in the Railway service:

```text
UPSTOX_API_KEY=...
UPSTOX_API_SECRET=...
UPSTOX_REDIRECT_URI=https://YOUR-RAILWAY-DOMAIN/callback
SMTP_HOST=smtp.gmail.com
SMTP_PORT=587
SMTP_USERNAME=...
SMTP_PASSWORD=...
EMAIL_FROM=...
EMAIL_TO=...
VOLUME_MULTIPLIER=1.5
HOURLY_LOOKBACK_DAYS=60
DAILY_LOOKBACK_DAYS=365
UPSTOX_THROTTLE_SECONDS=0.35
AUTO_SCAN_INTERVAL_MINUTES=60
AUTO_SCAN_DELAY_MINUTES=0
```

Register `https://YOUR-RAILWAY-DOMAIN/callback` as the Upstox app redirect URL. The standard Upstox access token expires at 3:30 AM the following day, so reconnect through the dashboard each trading morning. Do not commit API secrets.

The existing `railway.json` starts the app with one Gunicorn worker, which is required because the automatic scheduler runs inside the web process.

