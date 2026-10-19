# NIFTY 500 P-KAMA Scanner

This Railway app scans the current NIFTY 500 constituents using the Upstox API.

The current NSE constituent list is matched against Upstox's daily NSE BOD JSON instrument master before scanning. Placeholder or unavailable NSE entries are skipped, so invalid keys such as `DUMMYMHEG` do not interrupt or inflate the scan universe.

## Separate signals

- **1H signal:** the latest completed 60-minute candle crosses from below to above P-KAMA, and its volume is at least `VOLUME_MULTIPLIER` times the previous 20-bar average.
- **1D signal:** the latest completed daily candle crosses from below to above P-KAMA, and its volume is at least `VOLUME_MULTIPLIER` times the previous 20-day average.

The timeframes are evaluated independently. A stock can appear in either list; it does not need to cross on both timeframes together. A manual scan started after 15:35 IST runs 1H and 1D concurrently. Before 15:35 it runs 1H only, because the current daily candle is not yet confirmed.

Automatic 1H scans run at `10:16`, `11:16`, `12:16`, `13:16`, `14:16`, and `15:16` IST. The `09:15` opening candle is excluded. The automatic 1D scan runs once at `15:35` IST, after the NSE cash session has closed and the completed candle is available from Upstox. If another scan is still running, the 1D scan waits and starts afterward. Every scheduled slot, scan start, completion, autosave, wait, skip, or error is recorded in the dashboard terminal and Railway service logs.

P-KAMA is recursive, so the scanner requests 90 calendar days of hourly history and 10 years of daily history before evaluating the latest cross. This warm-up prevents a short-history initialization from materially shifting the P-KAMA line away from a long-running chart.

Every completed timeframe scan is stored in the internal SQLite journal, including scans that find zero signals. Each highlighted stock is retained separately for 1H and 1D and followed for 21 completed NSE trading sessions. The Journal screen separates 1H and 1D records, then groups them by identification month and date. Each dated section shows its scan runs and the entry close, latest close, current return, maximum favorable excursion, maximum adverse excursion, and closing returns after 1, 3, 5, 10, and 21 sessions. The stock remains under its original signal date while those tracking values continue to update. The post-close 1D process refreshes all open journal records automatically. The Journal screen can still export the tracked records as CSV when needed.

Earlier cumulative CSV journals are imported automatically the first time the database is opened, when those files are present. The live database is stored at `SCAN_DATA_DIR/pkama_journal.db`. A NIFTY 500 scan can take several minutes.

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
HOURLY_LOOKBACK_DAYS=90
DAILY_LOOKBACK_DAYS=3650
UPSTOX_THROTTLE_SECONDS=0.35
AUTO_SCAN_INTERVAL_MINUTES=60
AUTO_SCAN_DELAY_MINUTES=1
SCAN_DATA_DIR=data
```

Register `https://YOUR-RAILWAY-DOMAIN/callback` as the Upstox app redirect URL. The standard Upstox access token expires at 3:30 AM the following day, so reconnect through the dashboard each trading morning. Do not commit API secrets.

The existing `railway.json` starts the app with one Gunicorn worker, which is required because the automatic scheduler runs inside the web process.

Saved files and the internal journal database are written to an attached Railway Volume automatically through `RAILWAY_VOLUME_MOUNT_PATH`; outside Railway they use `SCAN_DATA_DIR` (default `data`). Attach a persistent Volume to the scanner service—`/app/data` or `/data` are suitable mount paths—so the journal survives service restarts and redeploys. Without a Volume, Railway's deployment filesystem is ephemeral.

