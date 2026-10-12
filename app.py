"""NIFTY 500 P-KAMA scanner for Railway and the Upstox API.

The confirmed bullish condition is:
  * the latest completed 60-minute candle crosses from below to above P-KAMA;
  * that 60-minute candle has high volume;
  * the latest completed daily candle crosses from below to above P-KAMA;
  * that daily candle has high volume.

The service is a scanner/alert system only. It never places orders.
"""

from __future__ import annotations

import csv
import hashlib
import hmac
import io
import json
import logging
import math
import os
import queue
import secrets
import smtplib
import threading
import time
from datetime import datetime, time as clock_time, timedelta
from email.message import EmailMessage
from urllib.parse import quote
from zoneinfo import ZoneInfo

import requests
from flask import Flask, Response, jsonify, redirect, render_template, request, send_file


app = Flask(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

UPSTOX_API_BASE = "https://api.upstox.com"
API_KEY = os.environ.get("UPSTOX_API_KEY", "")
API_SECRET = os.environ.get("UPSTOX_API_SECRET", "")
PUBLIC_DOMAIN = os.environ.get("RAILWAY_PUBLIC_DOMAIN", "")
REDIRECT_URI = os.environ.get(
    "UPSTOX_REDIRECT_URI",
    f"https://{PUBLIC_DOMAIN}/callback" if PUBLIC_DOMAIN else "http://localhost:5000/callback",
)
IST = ZoneInfo("Asia/Kolkata")
INDEX_LABEL = "NIFTY 500"
CONSTITUENT_URLS = (
    os.getenv(
        "NIFTY500_CONSTITUENT_URL",
        "https://nsearchives.nseindia.com/content/indices/ind_nifty500list.csv",
    ),
    "https://www.niftyindices.com/IndexConstituent/ind_nifty500list.csv",
)
VOLUME_LENGTH = 20

_state = {
    "access_token": None,
    "token_time": None,
    "last_results": [],
    "last_scan_time": None,
    "last_scan_status": "Waiting for Upstox login",
    "last_scanned_count": 0,
    "last_saved_at": None,
    "last_saved_files": [],
    "activity_log": [],
    "activity_seq": 0,
    "scan_running": False,
    "scan_mode": None,
    "scan_slot": None,
    "scan_progress_scanned": 0,
    "scan_progress_total": 0,
    "scan_progress_pct": 0,
    "last_email_fingerprint": None,
    "oauth_state": None,
}
_universe_cache: dict[str, str] = {}
_scan_lock = threading.Lock()
_activity_lock = threading.Lock()
_progress_lock = threading.Lock()
_scheduler_started = False


def record_activity(message: str, cls: str = "info") -> None:
    """Store a server-side activity entry for the dashboard terminal."""
    now = datetime.now(IST)
    with _activity_lock:
        _state["activity_seq"] += 1
        _state["activity_log"].append(
            {
                "id": _state["activity_seq"],
                "time": now.strftime("%d %b %H:%M:%S"),
                "timestamp": now.isoformat(),
                "msg": message,
                "cls": cls,
            }
        )
        _state["activity_log"] = _state["activity_log"][-200:]
    if cls == "err":
        logging.error(message)
    elif cls == "warn":
        logging.warning(message)
    else:
        logging.info(message)


def begin_scan_progress(mode: str, slot: str | None = None) -> None:
    with _progress_lock:
        _state["scan_running"] = True
        _state["scan_mode"] = mode
        _state["scan_slot"] = slot
        _state["scan_progress_scanned"] = 0
        _state["scan_progress_total"] = 0
        _state["scan_progress_pct"] = 0


def update_scan_progress(event: dict) -> None:
    if event.get("type") != "progress":
        return
    with _progress_lock:
        _state["scan_progress_scanned"] = int(event.get("scanned", 0))
        _state["scan_progress_total"] = int(event.get("total", 0))
        _state["scan_progress_pct"] = int(event.get("pct", 0))


def finish_scan_progress() -> None:
    with _progress_lock:
        _state["scan_running"] = False


def scan_progress_snapshot() -> dict:
    with _progress_lock:
        return {
            "running": _state["scan_running"],
            "mode": _state["scan_mode"],
            "slot": _state["scan_slot"],
            "scanned": _state["scan_progress_scanned"],
            "total": _state["scan_progress_total"],
            "pct": _state["scan_progress_pct"],
        }


def fetch_nifty500_instruments() -> dict[str, str]:
    """Fetch the current NIFTY 500 and construct Upstox NSE keys from ISINs."""
    global _universe_cache
    if _universe_cache:
        return _universe_cache
    errors = []
    for constituent_url in dict.fromkeys(CONSTITUENT_URLS):
        try:
            host = "nseindia.com" if "nseindia.com" in constituent_url else "niftyindices.com"
            response = requests.get(
                constituent_url,
                headers={
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/131 Safari/537.36",
                    "Accept": "text/csv,text/plain,application/octet-stream;q=0.9,*/*;q=0.8",
                    "Referer": f"https://www.{host}/",
                },
                timeout=30,
            )
            if response.status_code != 200:
                errors.append(f"{constituent_url} returned HTTP {response.status_code}")
                continue
            reader = csv.DictReader(io.StringIO(response.content.decode("utf-8-sig")))
            instruments = {}
            for row in reader:
                symbol = (row.get("Symbol") or row.get("SYMBOL") or "").strip().upper()
                isin = (row.get("ISIN Code") or row.get("ISIN") or "").strip().upper()
                if symbol and isin:
                    instruments[symbol] = f"NSE_EQ|{isin}"
            if len(instruments) >= 450:
                _universe_cache = instruments
                return instruments
            errors.append(f"{constituent_url} returned only {len(instruments)} usable instruments")
        except requests.RequestException as exc:
            errors.append(f"{constituent_url}: {exc}")
    raise RuntimeError("Could not load the NIFTY 500 constituent list. " + " | ".join(errors))


def upstox_headers() -> dict[str, str]:
    if not API_KEY:
        raise RuntimeError("UPSTOX_API_KEY is not configured")
    if not _state["access_token"]:
        raise RuntimeError("Upstox login required")
    return {
        "Accept": "application/json",
        "Authorization": f"Bearer {_state['access_token']}",
    }


def upstox_json(url: str, **kwargs) -> dict:
    response = requests.get(url, headers=upstox_headers(), timeout=30, **kwargs)
    if response.status_code != 200:
        raise RuntimeError(f"Upstox HTTP {response.status_code}: {response.text[:300]}")
    payload = response.json()
    if payload.get("status") != "success":
        raise RuntimeError(f"Upstox request failed: {payload}")
    return payload.get("data") or {}


def parse_upstox_candles(data: dict) -> list[dict]:
    parsed = []
    for row in data.get("candles", []):
        if len(row) < 6:
            continue
        stamp = str(row[0]).replace("Z", "+00:00")
        parsed.append(
            {
                "date": datetime.fromisoformat(stamp),
                "open": float(row[1]),
                "high": float(row[2]),
                "low": float(row[3]),
                "close": float(row[4]),
                "volume": float(row[5]),
            }
        )
    return parsed


def get_upstox_candles(instrument_key: str, unit: str, interval: str, days: int) -> list[dict]:
    now = datetime.now(IST)
    from_date = (now - timedelta(days=days)).date().isoformat()
    to_date = now.date().isoformat()
    encoded_key = quote(instrument_key, safe="")
    url = f"{UPSTOX_API_BASE}/v3/historical-candle/{encoded_key}/{unit}/{interval}/{to_date}/{from_date}"
    candles = parse_upstox_candles(upstox_json(url))

    # Historical data can omit today's still-forming session. Merge the current
    # intraday feed so the latest completed 1-hour candle is available.
    if unit == "hours":
        intraday_url = f"{UPSTOX_API_BASE}/v3/historical-candle/intraday/{encoded_key}/{unit}/{interval}"
        candles.extend(parse_upstox_candles(upstox_json(intraday_url)))

    unique = {c["date"].isoformat(): c for c in candles}
    return [unique[key] for key in sorted(unique)]


def is_complete_candle(candle_date, interval: str, now: datetime) -> bool:
    stamp = candle_date
    if stamp.tzinfo is None:
        stamp = stamp.replace(tzinfo=IST)
    stamp_ist = stamp.astimezone(IST)
    if interval == "day":
        # The daily candle is confirmed once the NSE cash session has closed.
        return stamp_ist.date() != now.date() or now.time() >= clock_time(15, 35)
    return stamp + timedelta(minutes=60) <= now


def completed_candles(candles: list[dict], interval: str) -> list[dict]:
    now = datetime.now(IST)
    result = []
    for candle in candles:
        stamp = candle["date"]
        if is_complete_candle(stamp, interval, now):
            result.append(candle)
    return result


def calc_pkama(closes: list[float], length: int, self_powered: bool = True, factor: float = 3.0) -> list[float]:
    """Match the supplied Pine P-KAMA formula."""
    kama = [float("nan")] * len(closes)
    for i in range(length, len(closes)):
        direction = abs(closes[i] - closes[i - length])
        volatility = sum(abs(closes[j] - closes[j - 1]) for j in range(i - length + 1, i + 1))
        er = direction / volatility if volatility else 0.0
        power = (1.0 / er) if self_powered and er else (1e9 if self_powered else factor)
        per = er**power
        previous = closes[i] if math.isnan(kama[i - 1]) else kama[i - 1]
        kama[i] = per * closes[i] + (1.0 - per) * previous
    return kama


def volume_ratio(volumes: list[float]) -> float:
    if len(volumes) < VOLUME_LENGTH + 1:
        return 0.0
    previous_average = sum(volumes[-VOLUME_LENGTH - 1 : -1]) / VOLUME_LENGTH
    return volumes[-1] / previous_average if previous_average else 0.0


def evaluate_signal(
    hourly: list[dict],
    daily: list[dict],
    length: int,
    self_powered: bool,
    factor: float,
    vol_mult: float,
) -> dict | None:
    hourly = completed_candles(hourly, "60minute")
    daily = completed_candles(daily, "day")
    if len(hourly) < max(length + 2, VOLUME_LENGTH + 1) or len(daily) < max(length + 2, VOLUME_LENGTH + 1):
        return None
    latest_hourly_stamp = hourly[-1]["date"]
    if latest_hourly_stamp.tzinfo is None:
        latest_hourly_stamp = latest_hourly_stamp.replace(tzinfo=IST)
    latest_hourly_stamp = latest_hourly_stamp.astimezone(IST)
    if datetime.now(IST) - latest_hourly_stamp > timedelta(hours=2):
        return None

    h_closes = [float(c["close"]) for c in hourly]
    h_volumes = [float(c["volume"]) for c in hourly]
    d_closes = [float(c["close"]) for c in daily]
    d_volumes = [float(c["volume"]) for c in daily]
    h_kama = calc_pkama(h_closes, length, self_powered, factor)
    d_kama = calc_pkama(d_closes, length, self_powered, factor)

    hp = h_kama[-2]
    hc = h_kama[-1]
    dp = d_kama[-2]
    dc = d_kama[-1]
    if math.isnan(hp) or math.isnan(hc) or math.isnan(dp) or math.isnan(dc):
        return None

    hourly_cross = h_closes[-2] <= hp and h_closes[-1] > hc
    hourly_volume = volume_ratio(h_volumes)
    daily_cross = d_closes[-2] <= dp and d_closes[-1] > dc
    daily_volume = volume_ratio(d_volumes)
    if not (hourly_cross and hourly_volume >= vol_mult and daily_cross and daily_volume >= vol_mult):
        return None

    return {
        "signal": "bullish",
        "ltp": round(h_closes[-1], 2),
        "pkama": round(hc, 2),
        "pkama_1h": round(hc, 2),
        "vol_ratio": round(hourly_volume, 2),
        "vol_ratio_1h": round(hourly_volume, 2),
        "dist_pct": round((h_closes[-1] - hc) / hc * 100, 2) if hc else 0,
        "sig_time": hourly[-1]["date"].isoformat(),
        "sig_time_1h": hourly[-1]["date"].isoformat(),
        "daily_close": round(d_closes[-1], 2),
        "pkama_1d": round(d_kama[-1], 2),
        "vol_ratio_1d": round(daily_volume, 2),
        "daily_time": daily[-1]["date"].isoformat(),
        "spark_closes": [round(v, 2) for v in h_closes[-20:]],
        "spark_kama": [None if math.isnan(v) else round(v, 2) for v in h_kama[-20:]],
    }


def scan_market(length: int, self_powered: bool, factor: float, vol_mult: float, emit=None) -> list[dict]:
    instruments = fetch_nifty500_instruments()
    symbols = list(instruments)
    results = []
    total = len(symbols)
    for index, symbol in enumerate(symbols, 1):
        if emit:
            emit({"type": "progress", "pct": round(index / total * 100), "scanned": index, "total": total})
        instrument_key = instruments.get(symbol)
        if not instrument_key:
            continue
        try:
            hourly = get_upstox_candles(
                instrument_key, "hours", "1", int(os.getenv("HOURLY_LOOKBACK_DAYS", "60"))
            )
            daily = get_upstox_candles(
                instrument_key, "days", "1", int(os.getenv("DAILY_LOOKBACK_DAYS", "365"))
            )
            signal = evaluate_signal(hourly, daily, length, self_powered, factor, vol_mult)
            if signal:
                signal["symbol"] = symbol
                results.append(signal)
                if emit:
                    emit({"type": "signal", **signal})
        except Exception as exc:  # Continue scanning if one instrument fails.
            logging.warning("%s scan failed: %s", symbol, exc)
            if emit:
                emit({"type": "log", "msg": f"{symbol}: {exc}", "cls": "err"})
        time.sleep(float(os.getenv("UPSTOX_THROTTLE_SECONDS", "0.35")))
    return results


def send_signal_email(results: list[dict]) -> None:
    host = os.getenv("SMTP_HOST")
    username = os.getenv("SMTP_USERNAME")
    password = os.getenv("SMTP_PASSWORD")
    sender = os.getenv("EMAIL_FROM", username or "")
    recipients = [x.strip() for x in os.getenv("EMAIL_TO", "").split(",") if x.strip()]
    if not all([host, username, password, sender, recipients]):
        raise RuntimeError("SMTP configuration is incomplete")
    lines = [
        "Symbol,1H close,1H P-KAMA,1H volume ratio,Daily close,Daily P-KAMA,Daily volume ratio,1H candle,Daily candle"
    ]
    for row in results:
        lines.append(
            f"{row['symbol']},{row['ltp']},{row['pkama_1h']},{row['vol_ratio_1h']},"
            f"{row['daily_close']},{row['pkama_1d']},{row['vol_ratio_1d']},"
            f"{row['sig_time_1h']},{row['daily_time']}"
        )
    csv_text = "\n".join(lines) + "\n"
    message = EmailMessage()
    message["Subject"] = f"Confirmed P-KAMA signal: {len(results)} {INDEX_LABEL} stock(s)"
    message["From"] = sender
    message["To"] = ", ".join(recipients)
    message.set_content("The attached CSV contains stocks confirmed by both 1H and daily P-KAMA conditions. No orders were placed.")
    message.add_attachment(csv_text.encode(), maintype="text", subtype="csv", filename="pkama_confirmed_signals.csv")
    with smtplib.SMTP(host, int(os.getenv("SMTP_PORT", "587")), timeout=30) as smtp:
        smtp.starttls()
        smtp.login(username, password)
        smtp.send_message(message)


RESULT_FIELDS = [
    "symbol",
    "signal",
    "ltp",
    "pkama_1h",
    "vol_ratio_1h",
    "daily_close",
    "pkama_1d",
    "vol_ratio_1d",
    "sig_time_1h",
    "daily_time",
]


def save_scan_results(results: list[dict], scan_type: str, params: dict) -> dict:
    """Save each completed scan as timestamped JSON/CSV and update latest copies."""
    directory = os.getenv("SCAN_DATA_DIR", "data")
    os.makedirs(directory, exist_ok=True)
    saved_at = datetime.now(IST)
    stamp = saved_at.strftime("%Y%m%d_%H%M%S")
    saved_at_text = saved_at.isoformat()

    csv_output = io.StringIO()
    writer = csv.DictWriter(csv_output, fieldnames=RESULT_FIELDS, extrasaction="ignore")
    writer.writeheader()
    writer.writerows(results)
    csv_text = csv_output.getvalue()
    payload = {
        "saved_at": saved_at_text,
        "scan_type": scan_type,
        "index": INDEX_LABEL,
        "scanned_symbols": len(fetch_nifty500_instruments()),
        "parameters": params,
        "results": results,
    }
    json_text = json.dumps(payload, ensure_ascii=False, indent=2, default=str)

    def write_atomically(path: str, content: str) -> None:
        temp_path = f"{path}.tmp"
        with open(temp_path, "w", encoding="utf-8", newline="") as handle:
            handle.write(content)
        os.replace(temp_path, path)

    json_path = os.path.join(directory, f"scan_{stamp}.json")
    csv_path = os.path.join(directory, f"scan_{stamp}.csv")
    latest_json_path = os.path.join(directory, "latest_results.json")
    latest_csv_path = os.path.join(directory, "latest_results.csv")
    for path, content in (
        (json_path, json_text),
        (csv_path, csv_text),
        (latest_json_path, json_text),
        (latest_csv_path, csv_text),
    ):
        write_atomically(path, content)

    _state["last_saved_at"] = saved_at_text
    _state["last_saved_files"] = [json_path, csv_path, latest_json_path, latest_csv_path]
    logging.info("Saved scan results to %s", json_path)
    return {"saved_at": saved_at_text, "json": json_path, "csv": csv_path}


def fingerprint(results: list[dict]) -> str:
    value = json.dumps(
        [(x["symbol"], x["sig_time_1h"], x["daily_time"]) for x in results], sort_keys=True
    )
    return hashlib.sha256(value.encode()).hexdigest()


def run_automatic_scan(scheduled_for: datetime | None = None) -> None:
    slot_label = scheduled_for.strftime("%H:%M") if scheduled_for else "scheduled"
    if not _state["access_token"]:
        record_activity(f"Automatic scan {slot_label} skipped: Upstox login is not active.", "warn")
        return
    if not _scan_lock.acquire(blocking=False):
        record_activity(f"Automatic scan {slot_label} skipped: another scan is still running.", "warn")
        return
    begin_scan_progress("automatic", slot_label)
    record_activity(f"Automatic scan {slot_label} started for {INDEX_LABEL}.")
    try:
        scan_params = {
            "length": int(os.getenv("PKAMA_LENGTH", "50")),
            "self_powered": os.getenv("PKAMA_SELF_POWERED", "true").lower() == "true",
            "factor": float(os.getenv("PKAMA_FACTOR", "3")),
            "vol_mult": float(os.getenv("VOLUME_MULTIPLIER", "1.5")),
        }
        def track_progress(event: dict) -> None:
            update_scan_progress(event)

        results = scan_market(
            scan_params["length"],
            scan_params["self_powered"],
            scan_params["factor"],
            scan_params["vol_mult"],
            emit=track_progress,
        )
        save_scan_results(results, "automatic", scan_params)
        scanned_count = len(fetch_nifty500_instruments())
        _state["last_results"] = results
        _state["last_scanned_count"] = scanned_count
        _state["last_scan_time"] = datetime.now(IST).isoformat()
        _state["last_scan_status"] = f"Automatic scan complete: {len(results)} confirmed signal(s)"
        notify_if_new(results)
        record_activity(
            f"Automatic scan {slot_label} completed: {scanned_count} stocks scanned, "
            f"{len(results)} confirmed signal(s); data autosaved.",
            "ok",
        )
    except Exception as exc:
        logging.exception("Automatic scan failed")
        _state["last_scan_status"] = f"Scan error: {exc}"
        record_activity(f"Automatic scan {slot_label} failed: {exc}", "err")
    finally:
        finish_scan_progress()
        _scan_lock.release()


def notify_if_new(results: list[dict]) -> None:
    current_fingerprint = fingerprint(results) if results else None
    if results and current_fingerprint != _state["last_email_fingerprint"]:
        send_signal_email(results)
        _state["last_email_fingerprint"] = current_fingerprint


def scheduler_loop() -> None:
    last_slot = None
    while True:
        now = datetime.now(IST)
        # A NIFTY 500 scan makes roughly 1,500 historical-data requests
        # (hourly history + intraday hourly history + daily history). Run it
        # once per hourly slot by default instead of every 5 minutes, which
        # could exceed Upstox's rolling API limits.
        interval_minutes = max(5, int(os.getenv("AUTO_SCAN_INTERVAL_MINUTES", "60")))
        delay_minutes = max(1, int(os.getenv("AUTO_SCAN_DELAY_MINUTES", "1")))
        market_minutes = (now.hour * 60 + now.minute) - (9 * 60 + 15)
        # With the default one-minute delay, this triggers at 10:16, 11:16,
        # 12:16, 13:16, 14:16, and 15:16 IST. The 09:15 opening candle is
        # deliberately excluded because it is not a completed trading hour.
        first_slot = interval_minutes + delay_minutes
        due = market_minutes >= first_slot and (market_minutes - first_slot) % interval_minutes < 5
        slot = (market_minutes - first_slot) // interval_minutes if due else None
        market_open = now.weekday() < 5 and clock_time(9, 15) <= now.time() <= clock_time(15, 40)
        if market_open and due and slot != last_slot:
            last_slot = slot
            session_start = now.replace(hour=9, minute=15, second=0, microsecond=0)
            scheduled_for = session_start + timedelta(minutes=first_slot + slot * interval_minutes)
            record_activity(f"Scheduled slot {scheduled_for.strftime('%H:%M')} reached.")
            threading.Thread(target=run_automatic_scan, args=(scheduled_for,), daemon=True).start()
        time.sleep(20)


def start_scheduler() -> None:
    global _scheduler_started
    if not _scheduler_started:
        _scheduler_started = True
        record_activity("Automatic scheduler active for 10:16, 11:16, 12:16, 13:16, 14:16 and 15:16 IST.")
        threading.Thread(target=scheduler_loop, daemon=True, name="p-kama-scheduler").start()


@app.route("/")
def index():
    start_scheduler()
    if not API_KEY:
        login_url = "#"
    else:
        state = secrets.token_urlsafe(32)
        _state["oauth_state"] = state
        login_url = (
            "https://api.upstox.com/v2/login/authorization/dialog?"
            f"client_id={quote(API_KEY)}&redirect_uri={quote(REDIRECT_URI)}"
            f"&response_type=code&state={quote(state)}"
        )
    token_set = _state["access_token"] is not None
    time_str = _state["token_time"].strftime("%d %b %Y, %I:%M %p") if _state["token_time"] else None
    return render_template(
        "index.html",
        login_url=login_url,
        token_set=token_set,
        token_time=time_str,
        status=_state["last_scan_status"],
        last_scan_time=_state["last_scan_time"],
        result_count=len(_state["last_results"]),
    )


@app.route("/callback")
def callback():
    authorization_code = request.args.get("code")
    returned_state = request.args.get("state")
    if not authorization_code:
        return render_template("callback_error.html", msg="No Upstox authorization code found. Please try again."), 400
    if not _state["oauth_state"] or not returned_state or not hmac.compare_digest(_state["oauth_state"], returned_state):
        return render_template("callback_error.html", msg="Invalid login state. Please start login again."), 400
    _state["oauth_state"] = None
    try:
        if not API_KEY or not API_SECRET:
            raise RuntimeError("UPSTOX_API_KEY or UPSTOX_API_SECRET is not configured")
        response = requests.post(
            f"{UPSTOX_API_BASE}/v2/login/authorization/token",
            headers={"Accept": "application/json", "Content-Type": "application/x-www-form-urlencoded"},
            data={
                "code": authorization_code,
                "client_id": API_KEY,
                "client_secret": API_SECRET,
                "redirect_uri": REDIRECT_URI,
                "grant_type": "authorization_code",
            },
            timeout=30,
        )
        if response.status_code != 200:
            raise RuntimeError(f"Upstox token exchange failed ({response.status_code}): {response.text[:300]}")
        data = response.json()
        if not data.get("access_token"):
            raise RuntimeError(f"Upstox token exchange returned no access token: {data}")
        _state["access_token"] = data["access_token"]
        _state["token_time"] = datetime.now(IST)
        _state["last_scan_status"] = "Upstox connected; automatic scanning is active"
        return redirect("/")
    except Exception as exc:
        logging.exception("Upstox login failed")
        return render_template("callback_error.html", msg=f"Upstox login failed: {exc}"), 502


@app.route("/api/token-status")
def token_status():
    return jsonify(
        {
            "set": _state["access_token"] is not None,
            "time": _state["token_time"].strftime("%d %b, %I:%M %p") if _state["token_time"] else None,
            "status": _state["last_scan_status"],
            "last_scan_time": _state["last_scan_time"],
            "saved_at": _state["last_saved_at"],
            "scanned": _state["last_scanned_count"],
            "signals": len(_state["last_results"]),
            "scan": scan_progress_snapshot(),
        }
    )


@app.route("/api/latest-results")
def latest_results():
    return jsonify(
        {
            "results": _state["last_results"],
            "status": _state["last_scan_status"],
            "saved_at": _state["last_saved_at"],
            "scanned": _state["last_scanned_count"],
            "activity_log": _state["activity_log"],
            "scan": scan_progress_snapshot(),
        }
    )


@app.route("/api/download/latest.csv")
def download_latest_results():
    path = os.path.join(os.getenv("SCAN_DATA_DIR", "data"), "latest_results.csv")
    if not os.path.exists(path):
        return jsonify({"error": "No scan has been saved yet."}), 404
    return send_file(path, mimetype="text/csv", as_attachment=True, download_name="pkama_latest_results.csv")


@app.route("/api/scan")
def manual_scan():
    if not _state["access_token"]:
        return Response(
            f"data: {json.dumps({'type': 'error', 'msg': 'Please connect Upstox first.'})}\n\n",
            mimetype="text/event-stream",
        )
    if not _scan_lock.acquire(blocking=False):
        return Response(
            f"data: {json.dumps({'type': 'error', 'msg': 'Another scan is already running.'})}\n\n",
            mimetype="text/event-stream",
        )

    length = int(request.args.get("length", 50))
    self_powered = request.args.get("self_powered", "true").lower() == "true"
    factor = float(request.args.get("factor", 3.0))
    vol_mult = float(request.args.get("vol_mult", 1.5))

    events = queue.Queue()

    def emit(event):
        update_scan_progress(event)
        events.put(event)

    begin_scan_progress("manual")

    def worker():
        try:
            record_activity(f"Manual scan started for {INDEX_LABEL}.")
            results = scan_market(length, self_powered, factor, vol_mult, emit=emit)
            save_scan_results(
                results,
                "manual",
                {
                    "length": length,
                    "self_powered": self_powered,
                    "factor": factor,
                    "vol_mult": vol_mult,
                },
            )
            scanned_count = len(fetch_nifty500_instruments())
            _state["last_results"] = results
            _state["last_scanned_count"] = scanned_count
            _state["last_scan_time"] = datetime.now(IST).isoformat()
            _state["last_scan_status"] = f"Manual scan complete: {len(results)} confirmed signal(s)"
            notify_if_new(results)
            record_activity(
                f"Manual scan completed: {scanned_count} stocks scanned, "
                f"{len(results)} confirmed signal(s); data autosaved.",
                "ok",
            )
            events.put({"type": "done", "scanned": scanned_count})
        except Exception as exc:
            _state["last_scan_status"] = f"Scan error: {exc}"
            record_activity(f"Manual scan failed: {exc}", "err")
            events.put({"type": "error", "msg": str(exc)})
        finally:
            finish_scan_progress()
            events.put(None)
            _scan_lock.release()

    threading.Thread(target=worker, daemon=True).start()

    def generate():
        yield f"data: {json.dumps({'type': 'log', 'msg': f'Scanning {INDEX_LABEL} on 1H + daily confirmation…', 'cls': 'info'})}\n\n"
        while True:
            event = events.get()
            if event is None:
                break
            yield f"data: {json.dumps(event, default=str)}\n\n"

    return Response(generate(), mimetype="text/event-stream", headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


start_scheduler()


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)), debug=False, threaded=True)

