"""NIFTY 50 P-KAMA scanner for Railway and Zerodha Kite Connect.

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
from flask import Flask, Response, jsonify, redirect, render_template, request
from kiteconnect import KiteConnect


app = Flask(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

API_KEY = os.environ.get("KITE_API_KEY", "")
API_SECRET = os.environ.get("KITE_API_SECRET", "")
IST = ZoneInfo("Asia/Kolkata")
CONSTITUENT_URL = "https://www.niftyindices.com/IndexConstituent/ind_nifty50list.csv"
VOLUME_LENGTH = 20

_state = {
    "access_token": None,
    "token_time": None,
    "last_results": [],
    "last_scan_time": None,
    "last_scan_status": "Waiting for Zerodha login",
    "last_email_fingerprint": None,
    "oauth_state": None,
}
_nifty_cache: list[str] = []
_scan_lock = threading.Lock()
_scheduler_started = False


def fetch_nifty50_symbols() -> list[str]:
    """Fetch the current NIFTY 50 constituents from Nifty Indices."""
    global _nifty_cache
    if _nifty_cache:
        return _nifty_cache
    response = requests.get(CONSTITUENT_URL, headers={"User-Agent": "Mozilla/5.0"}, timeout=30)
    response.raise_for_status()
    reader = csv.DictReader(io.StringIO(response.content.decode("utf-8-sig")))
    symbols = []
    for row in reader:
        symbol = (row.get("Symbol") or row.get("SYMBOL") or "").strip().upper()
        if symbol:
            symbols.append(symbol)
    if len(symbols) < 40:
        raise RuntimeError(f"NIFTY 50 constituent file returned only {len(symbols)} symbols")
    _nifty_cache = symbols
    return symbols


def kite_client() -> KiteConnect:
    if not API_KEY:
        raise RuntimeError("KITE_API_KEY is not configured")
    if not _state["access_token"]:
        raise RuntimeError("Zerodha login required")
    kite = KiteConnect(api_key=API_KEY)
    kite.set_access_token(_state["access_token"])
    return kite


def get_instruments(kite: KiteConnect, symbols: list[str]) -> dict[str, int]:
    records = kite.instruments("NSE")
    wanted = set(symbols)
    return {
        str(row["tradingsymbol"]).upper(): int(row["instrument_token"])
        for row in records
        if str(row.get("instrument_type", "")).upper() == "EQ"
        and str(row.get("tradingsymbol", "")).upper() in wanted
    }


def get_historical(kite: KiteConnect, token: int, interval: str, days: int) -> list[dict]:
    now = datetime.now(IST)
    start = now - timedelta(days=days)
    return kite.historical_data(token, start, now, interval, continuous=False, oi=False)


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
    kite = kite_client()
    symbols = fetch_nifty50_symbols()
    instruments = get_instruments(kite, symbols)
    results = []
    total = len(symbols)
    for index, symbol in enumerate(symbols, 1):
        if emit:
            emit({"type": "progress", "pct": round(index / total * 100), "scanned": index})
        token = instruments.get(symbol)
        if not token:
            continue
        try:
            hourly = get_historical(kite, token, "60minute", int(os.getenv("HOURLY_LOOKBACK_DAYS", "60")))
            daily = get_historical(kite, token, "day", int(os.getenv("DAILY_LOOKBACK_DAYS", "365")))
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
        time.sleep(float(os.getenv("KITE_THROTTLE_SECONDS", "0.35")))
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
    message["Subject"] = f"Confirmed P-KAMA signal: {len(results)} NIFTY 50 stock(s)"
    message["From"] = sender
    message["To"] = ", ".join(recipients)
    message.set_content("The attached CSV contains stocks confirmed by both 1H and daily P-KAMA conditions. No orders were placed.")
    message.add_attachment(csv_text.encode(), maintype="text", subtype="csv", filename="pkama_confirmed_signals.csv")
    with smtplib.SMTP(host, int(os.getenv("SMTP_PORT", "587")), timeout=30) as smtp:
        smtp.starttls()
        smtp.login(username, password)
        smtp.send_message(message)


def fingerprint(results: list[dict]) -> str:
    value = json.dumps(
        [(x["symbol"], x["sig_time_1h"], x["daily_time"]) for x in results], sort_keys=True
    )
    return hashlib.sha256(value.encode()).hexdigest()


def run_automatic_scan() -> None:
    if not _state["access_token"] or not _scan_lock.acquire(blocking=False):
        return
    try:
        params = (
            int(os.getenv("PKAMA_LENGTH", "50")),
            os.getenv("PKAMA_SELF_POWERED", "true").lower() == "true",
            float(os.getenv("PKAMA_FACTOR", "3")),
            float(os.getenv("VOLUME_MULTIPLIER", "1.5")),
        )
        results = scan_market(*params)
        _state["last_results"] = results
        _state["last_scan_time"] = datetime.now(IST).isoformat()
        _state["last_scan_status"] = f"Automatic scan complete: {len(results)} confirmed signal(s)"
        notify_if_new(results)
    except Exception as exc:
        logging.exception("Automatic scan failed")
        _state["last_scan_status"] = f"Scan error: {exc}"
    finally:
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
        slot = now.replace(second=0, microsecond=0, minute=(now.minute // 5) * 5)
        market_open = now.weekday() < 5 and clock_time(9, 15) <= now.time() <= clock_time(15, 40)
        if market_open and slot != last_slot:
            last_slot = slot
            threading.Thread(target=run_automatic_scan, daemon=True).start()
        time.sleep(20)


def start_scheduler() -> None:
    global _scheduler_started
    if not _scheduler_started:
        _scheduler_started = True
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
            "https://kite.zerodha.com/connect/login?"
            f"api_key={quote(API_KEY)}&v=3&redirect_params={quote('state=' + state)}"
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
    request_token = request.args.get("request_token")
    returned_state = request.args.get("state")
    if not request_token:
        return render_template("callback_error.html", msg="No request_token found. Please try again."), 400
    if not _state["oauth_state"] or not returned_state or not hmac.compare_digest(_state["oauth_state"], returned_state):
        return render_template("callback_error.html", msg="Invalid login state. Please start login again."), 400
    _state["oauth_state"] = None
    try:
        kite = KiteConnect(api_key=API_KEY)
        data = kite.generate_session(request_token, api_secret=API_SECRET)
        _state["access_token"] = data["access_token"]
        _state["token_time"] = datetime.now(IST)
        _state["last_scan_status"] = "Zerodha connected; automatic scanning is active"
        return redirect("/")
    except Exception as exc:
        logging.exception("Zerodha login failed")
        return render_template("callback_error.html", msg=f"Zerodha login failed: {exc}"), 502


@app.route("/api/token-status")
def token_status():
    return jsonify(
        {
            "set": _state["access_token"] is not None,
            "time": _state["token_time"].strftime("%d %b, %I:%M %p") if _state["token_time"] else None,
            "status": _state["last_scan_status"],
            "last_scan_time": _state["last_scan_time"],
            "signals": len(_state["last_results"]),
        }
    )


@app.route("/api/latest-results")
def latest_results():
    return jsonify({"results": _state["last_results"], "status": _state["last_scan_status"]})


@app.route("/api/scan")
def manual_scan():
    if not _state["access_token"]:
        return Response(
            f"data: {json.dumps({'type': 'error', 'msg': 'Please connect Zerodha first.'})}\n\n",
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
        events.put(event)

    def worker():
        try:
            results = scan_market(length, self_powered, factor, vol_mult, emit=emit)
            _state["last_results"] = results
            _state["last_scan_time"] = datetime.now(IST).isoformat()
            _state["last_scan_status"] = f"Manual scan complete: {len(results)} confirmed signal(s)"
            notify_if_new(results)
            events.put({"type": "done", "scanned": 50})
        except Exception as exc:
            _state["last_scan_status"] = f"Scan error: {exc}"
            events.put({"type": "error", "msg": str(exc)})
        finally:
            events.put(None)
            _scan_lock.release()

    threading.Thread(target=worker, daemon=True).start()

    def generate():
        yield f"data: {json.dumps({'type': 'log', 'msg': 'Scanning NIFTY 50 on 1H + daily confirmation…', 'cls': 'info'})}\n\n"
        while True:
            event = events.get()
            if event is None:
                break
            yield f"data: {json.dumps(event, default=str)}\n\n"

    return Response(generate(), mimetype="text/event-stream", headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


start_scheduler()


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)), debug=False, threaded=True)
