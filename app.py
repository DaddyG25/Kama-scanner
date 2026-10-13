"""NIFTY 500 P-KAMA scanner for Railway and the Upstox API.

The scanner produces two independent bullish lists in parallel:
  * latest completed 60-minute candle crosses above P-KAMA with high volume;
  * latest completed daily candle crosses above P-KAMA with high volume.

The service is a scanner/alert system only. It never places orders.
"""

from __future__ import annotations

import csv
import gzip
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
from concurrent.futures import ThreadPoolExecutor
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
UPSTOX_NSE_INSTRUMENTS_URL = os.getenv(
    "UPSTOX_NSE_INSTRUMENTS_URL",
    "https://assets.upstox.com/market-quote/instruments/exchange/NSE.json.gz",
)
VOLUME_LENGTH = 20
# These are Upstox V3's supported retrieval windows for the selected units.
# Keep them fixed so an older Railway variable (60/365) cannot reintroduce the
# short warm-up that caused materially incorrect recursive P-KAMA values.
HOURLY_HISTORY_DAYS = 90
DAILY_HISTORY_DAYS = 3650

_state = {
    "access_token": None,
    "token_time": None,
    "last_results": [],
    "last_results_1h": [],
    "last_results_1d": [],
    "last_scan_time": None,
    "last_scan_status": "Waiting for Upstox login",
    "last_scanned_count": 0,
    "last_saved_at": None,
    "last_saved_files": [],
    "activity_log": [],
    "activity_seq": 0,
    "scan_running": False,
    "scan_timeframes": [],
    "scan_mode": None,
    "scan_slot": None,
    "scan_progress_scanned": 0,
    "scan_progress_total": 0,
    "scan_progress_pct": 0,
    "scan_progress_1h_scanned": 0,
    "scan_progress_1h_total": 0,
    "scan_progress_1h_pct": 0,
    "scan_progress_1d_scanned": 0,
    "scan_progress_1d_total": 0,
    "scan_progress_1d_pct": 0,
    "last_email_fingerprint": None,
    "oauth_state": None,
    "daily_scan_date": None,
}
_universe_cache: dict[str, str] = {}
_scan_lock = threading.Lock()
_activity_lock = threading.Lock()
_progress_lock = threading.Lock()
_journal_lock = threading.Lock()
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


def begin_scan_progress(mode: str, slot: str | None = None, timeframes=("1h", "1d")) -> None:
    with _progress_lock:
        _state["scan_running"] = True
        _state["scan_timeframes"] = list(timeframes)
        _state["scan_mode"] = mode
        _state["scan_slot"] = slot
        _state["scan_progress_scanned"] = 0
        _state["scan_progress_total"] = 0
        _state["scan_progress_pct"] = 0
        for timeframe in timeframes:
            _state[f"scan_progress_{timeframe}_scanned"] = 0
            _state[f"scan_progress_{timeframe}_total"] = 0
            _state[f"scan_progress_{timeframe}_pct"] = 0


def update_scan_progress(event: dict) -> None:
    if event.get("type") != "progress":
        return
    timeframe = event.get("timeframe")
    with _progress_lock:
        if timeframe in {"1h", "1d"}:
            _state[f"scan_progress_{timeframe}_scanned"] = int(event.get("scanned", 0))
            _state[f"scan_progress_{timeframe}_total"] = int(event.get("total", 0))
            _state[f"scan_progress_{timeframe}_pct"] = int(event.get("pct", 0))
        _state["scan_progress_scanned"] = (
            _state["scan_progress_1h_scanned"] + _state["scan_progress_1d_scanned"]
        )
        _state["scan_progress_total"] = (
            _state["scan_progress_1h_total"] + _state["scan_progress_1d_total"]
        )
        percentages = [
            _state[f"scan_progress_{timeframe}_pct"]
            for timeframe in _state["scan_timeframes"]
        ]
        _state["scan_progress_pct"] = round(sum(percentages) / len(percentages)) if percentages else 0


def finish_scan_progress() -> None:
    with _progress_lock:
        _state["scan_running"] = False
        _state["scan_timeframes"] = []


def scan_progress_snapshot() -> dict:
    with _progress_lock:
        return {
            "running": _state["scan_running"],
            "mode": _state["scan_mode"],
            "slot": _state["scan_slot"],
            "timeframes": list(_state["scan_timeframes"]),
            "scanned": _state["scan_progress_scanned"],
            "total": _state["scan_progress_total"],
            "pct": _state["scan_progress_pct"],
            "one_hour": {
                "scanned": _state["scan_progress_1h_scanned"],
                "total": _state["scan_progress_1h_total"],
                "pct": _state["scan_progress_1h_pct"],
            },
            "one_day": {
                "scanned": _state["scan_progress_1d_scanned"],
                "total": _state["scan_progress_1d_total"],
                "pct": _state["scan_progress_1d_pct"],
            },
        }


def fetch_upstox_nse_instrument_maps() -> tuple[dict[str, str], dict[str, str]]:
    """Load valid NSE equity keys from Upstox's daily BOD JSON master."""
    response = requests.get(
        UPSTOX_NSE_INSTRUMENTS_URL,
        headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json,application/gzip"},
        timeout=90,
    )
    response.raise_for_status()
    raw = response.content
    if raw.startswith(b"\x1f\x8b"):
        raw = gzip.decompress(raw)
    payload = json.loads(raw.decode("utf-8"))
    if isinstance(payload, dict):
        payload = payload.get("data", [])

    by_isin: dict[str, str] = {}
    by_symbol: dict[str, str] = {}
    for item in payload:
        if item.get("segment") != "NSE_EQ" or item.get("instrument_type") != "EQ":
            continue
        key = str(item.get("instrument_key") or "").strip()
        isin = str(item.get("isin") or "").strip().upper()
        symbol = str(item.get("trading_symbol") or "").strip().upper()
        if not key.startswith("NSE_EQ|") or key.split("|", 1)[1].upper().startswith("DUMMY"):
            continue
        if isin:
            by_isin[isin] = key
        if symbol:
            by_symbol[symbol] = key
    if len(by_isin) < 1000:
        raise RuntimeError(f"Upstox NSE instrument master returned only {len(by_isin)} equity instruments")
    return by_isin, by_symbol


def fetch_nifty500_instruments() -> dict[str, str]:
    """Match current NIFTY 500 members to valid Upstox NSE instrument keys."""
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
            constituents = []
            for row in reader:
                symbol = (row.get("Symbol") or row.get("SYMBOL") or "").strip().upper()
                isin = (row.get("ISIN Code") or row.get("ISIN") or "").strip().upper()
                if symbol and isin:
                    constituents.append((symbol, isin))
            if len(constituents) >= 450:
                try:
                    by_isin, by_symbol = fetch_upstox_nse_instrument_maps()
                    instruments = {}
                    skipped = []
                    for symbol, isin in constituents:
                        key = by_isin.get(isin) or by_symbol.get(symbol)
                        if key:
                            instruments[symbol] = key
                        else:
                            skipped.append(symbol)
                    if len(instruments) < 450:
                        raise RuntimeError(
                            f"Only {len(instruments)} NIFTY 500 members matched the Upstox instrument master"
                        )
                    if skipped:
                        preview = ", ".join(skipped[:8])
                        record_activity(
                            f"Loaded {len(instruments)} valid Upstox instruments; "
                            f"skipped {len(skipped)} unavailable NSE entr{'y' if len(skipped) == 1 else 'ies'}: {preview}.",
                            "warn",
                        )
                except (requests.RequestException, OSError, ValueError, RuntimeError) as exc:
                    # Safe fallback: genuine Indian equity ISINs only. This
                    # rejects NSE placeholder keys such as DUMMYMHEG.
                    instruments = {
                        symbol: f"NSE_EQ|{isin}"
                        for symbol, isin in constituents
                        if len(isin) == 12 and isin.startswith("INE")
                    }
                    record_activity(
                        f"Upstox instrument master unavailable; using {len(instruments)} validated ISIN keys: {exc}",
                        "warn",
                    )
                _universe_cache = instruments
                return instruments
            errors.append(f"{constituent_url} returned only {len(constituents)} usable instruments")
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

    # Historical data can omit today's session. Merge Upstox's current-day
    # endpoint so the latest completed hourly or post-close daily candle is
    # available. For daily data, deduplicate by trading date because the two
    # endpoints can use different start-of-candle timestamps.
    if unit in {"hours", "days"}:
        intraday_url = f"{UPSTOX_API_BASE}/v3/historical-candle/intraday/{encoded_key}/{unit}/{interval}"
        candles.extend(parse_upstox_candles(upstox_json(intraday_url)))

    if unit == "days":
        unique = {
            c["date"].astimezone(IST).date().isoformat(): c
            for c in candles
        }
    else:
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


def evaluate_timeframe_signal(
    candles: list[dict],
    timeframe: str,
    length: int,
    self_powered: bool,
    factor: float,
    vol_mult: float,
) -> dict | None:
    interval = "60minute" if timeframe == "1h" else "day"
    candles = completed_candles(candles, interval)
    if len(candles) < max(length + 2, VOLUME_LENGTH + 1):
        return None
    latest_stamp = candles[-1]["date"]
    if latest_stamp.tzinfo is None:
        latest_stamp = latest_stamp.replace(tzinfo=IST)
    latest_stamp = latest_stamp.astimezone(IST)
    if timeframe == "1h" and datetime.now(IST) - latest_stamp > timedelta(hours=2):
        return None
    if timeframe == "1d" and latest_stamp.date() != datetime.now(IST).date():
        return None

    closes = [float(c["close"]) for c in candles]
    volumes = [float(c["volume"]) for c in candles]
    kama = calc_pkama(closes, length, self_powered, factor)
    previous_kama = kama[-2]
    current_kama = kama[-1]
    if math.isnan(previous_kama) or math.isnan(current_kama):
        return None
    crossed_above = closes[-2] <= previous_kama and closes[-1] > current_kama
    current_volume_ratio = volume_ratio(volumes)
    if not (crossed_above and current_volume_ratio >= vol_mult):
        return None

    return {
        "timeframe": timeframe,
        "signal": "bullish",
        "ltp": round(closes[-1], 2),
        "pkama": round(current_kama, 2),
        "vol_ratio": round(current_volume_ratio, 2),
        "dist_pct": round((closes[-1] - current_kama) / current_kama * 100, 2) if current_kama else 0,
        "sig_time": candles[-1]["date"].isoformat(),
        "spark_closes": [round(value, 2) for value in closes[-20:]],
        "spark_kama": [None if math.isnan(value) else round(value, 2) for value in kama[-20:]],
    }


def scan_timeframe(
    timeframe: str,
    instruments: dict[str, str],
    length: int,
    self_powered: bool,
    factor: float,
    vol_mult: float,
    emit=None,
) -> list[dict]:
    symbols = list(instruments)
    results = []
    total = len(symbols)
    for index, symbol in enumerate(symbols, 1):
        if emit:
            emit(
                {
                    "type": "progress",
                    "timeframe": timeframe,
                    "pct": round(index / total * 100),
                    "scanned": index,
                    "total": total,
                }
            )
        instrument_key = instruments.get(symbol)
        if not instrument_key:
            continue
        try:
            if timeframe == "1h":
                candles = get_upstox_candles(
                    instrument_key, "hours", "1", HOURLY_HISTORY_DAYS
                )
            else:
                candles = get_upstox_candles(
                    instrument_key, "days", "1", DAILY_HISTORY_DAYS
                )
            signal = evaluate_timeframe_signal(
                candles, timeframe, length, self_powered, factor, vol_mult
            )
            if signal:
                signal["symbol"] = symbol
                results.append(signal)
                if emit:
                    emit({"type": "signal", **signal})
        except Exception as exc:  # Continue scanning if one instrument fails.
            message = str(exc)
            invalid_key = "UDAPI100011" in message or "Invalid Instrument key" in message
            logging.warning("%s %s scan failed: %s", symbol, timeframe, message)
            if emit:
                if invalid_key:
                    emit({"type": "log", "msg": f"{symbol} [{timeframe.upper()}]: skipped because Upstox has no valid instrument key.", "cls": "warn"})
                else:
                    emit({"type": "log", "msg": f"{symbol} [{timeframe.upper()}]: {message}", "cls": "err"})
        time.sleep(float(os.getenv("UPSTOX_THROTTLE_SECONDS", "0.35")))
    return results


def scan_timeframes(
    timeframes, length: int, self_powered: bool, factor: float, vol_mult: float, emit=None
) -> dict[str, list[dict]]:
    """Run the requested timeframe scans, concurrently when both are requested."""
    instruments = fetch_nifty500_instruments()
    requested = tuple(dict.fromkeys(timeframes))
    with ThreadPoolExecutor(max_workers=len(requested), thread_name_prefix="pkama-timeframe") as executor:
        futures = {
            timeframe: executor.submit(
                scan_timeframe,
                timeframe,
                instruments,
                length,
                self_powered,
                factor,
                vol_mult,
                emit,
            )
            for timeframe in requested
        }
        return {timeframe: future.result() for timeframe, future in futures.items()}


def send_signal_email(results: list[dict]) -> None:
    host = os.getenv("SMTP_HOST")
    username = os.getenv("SMTP_USERNAME")
    password = os.getenv("SMTP_PASSWORD")
    sender = os.getenv("EMAIL_FROM", username or "")
    recipients = [x.strip() for x in os.getenv("EMAIL_TO", "").split(",") if x.strip()]
    if not all([host, username, password, sender, recipients]):
        raise RuntimeError("SMTP configuration is incomplete")
    lines = ["Timeframe,Symbol,Close,P-KAMA,Volume ratio,Candle"]
    for row in results:
        lines.append(
            f"{row['timeframe']},{row['symbol']},{row['ltp']},{row['pkama']},"
            f"{row['vol_ratio']},{row['sig_time']}"
        )
    csv_text = "\n".join(lines) + "\n"
    one_hour_count = sum(row["timeframe"] == "1h" for row in results)
    one_day_count = sum(row["timeframe"] == "1d" for row in results)
    message = EmailMessage()
    message["Subject"] = f"P-KAMA signals: {one_hour_count} 1H + {one_day_count} 1D"
    message["From"] = sender
    message["To"] = ", ".join(recipients)
    message.set_content("The attached CSV contains separate 1H and 1D P-KAMA crossover signals with high volume. No orders were placed.")
    message.add_attachment(csv_text.encode(), maintype="text", subtype="csv", filename="pkama_timeframe_signals.csv")
    with smtplib.SMTP(host, int(os.getenv("SMTP_PORT", "587")), timeout=30) as smtp:
        smtp.starttls()
        smtp.login(username, password)
        smtp.send_message(message)


RESULT_FIELDS = [
    "timeframe",
    "symbol",
    "signal",
    "ltp",
    "pkama",
    "vol_ratio",
    "dist_pct",
    "sig_time",
]

JOURNAL_FIELDS = [
    "scan_id",
    "scan_date",
    "scan_time",
    "scan_type",
    "timeframe",
    "status",
    "scanned_symbols",
    "signal_count",
    "symbol",
    "close",
    "pkama",
    "volume_ratio",
    "distance_pct",
    "candle_time",
    "pkama_length",
    "volume_multiplier",
]


def write_atomically(path: str, content: str) -> None:
    temp_path = f"{path}.tmp"
    with open(temp_path, "w", encoding="utf-8", newline="") as handle:
        handle.write(content)
    os.replace(temp_path, path)


def update_timeframe_results(results_by_timeframe: dict[str, list[dict]]) -> None:
    """Replace only the timeframe that ran, preserving the other latest list."""
    for timeframe, results in results_by_timeframe.items():
        _state[f"last_results_{timeframe}"] = results
    _state["last_results"] = _state["last_results_1h"] + _state["last_results_1d"]


def append_timeframe_journal(
    timeframe: str,
    results: list[dict],
    scan_type: str,
    params: dict,
    scanned_symbols: int,
) -> list[str]:
    """Append one auditable journal entry for every scan, including zero-signal runs."""
    directory = os.getenv("SCAN_DATA_DIR", "data")
    journal_directory = os.path.join(directory, "journals")
    os.makedirs(journal_directory, exist_ok=True)
    saved_at = datetime.now(IST)
    scan_id = saved_at.strftime("%Y%m%d_%H%M%S_%f")
    common = {
        "scan_id": scan_id,
        "scan_date": saved_at.date().isoformat(),
        "scan_time": saved_at.strftime("%H:%M:%S %Z"),
        "scan_type": scan_type,
        "timeframe": timeframe,
        "scanned_symbols": scanned_symbols,
        "signal_count": len(results),
        "pkama_length": params["length"],
        "volume_multiplier": params["vol_mult"],
    }
    rows = []
    for result in results:
        rows.append(
            {
                **common,
                "status": "SIGNAL",
                "symbol": result.get("symbol", ""),
                "close": result.get("ltp", ""),
                "pkama": result.get("pkama", ""),
                "volume_ratio": result.get("vol_ratio", ""),
                "distance_pct": result.get("dist_pct", ""),
                "candle_time": result.get("sig_time", ""),
            }
        )
    if not rows:
        rows.append({**common, "status": "NO_SIGNALS"})

    cumulative_path = os.path.join(journal_directory, f"journal_{timeframe}.csv")
    daily_path = os.path.join(
        journal_directory, f"journal_{timeframe}_{saved_at.date().isoformat()}.csv"
    )
    latest_path = os.path.join(journal_directory, f"latest_{timeframe}.csv")
    with _journal_lock:
        for path in (cumulative_path, daily_path):
            exists = os.path.exists(path) and os.path.getsize(path) > 0
            with open(path, "a", encoding="utf-8", newline="") as handle:
                writer = csv.DictWriter(handle, fieldnames=JOURNAL_FIELDS, extrasaction="ignore")
                if not exists:
                    writer.writeheader()
                writer.writerows(rows)
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=JOURNAL_FIELDS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
        write_atomically(latest_path, output.getvalue())
    return [cumulative_path, daily_path, latest_path]


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
        [(x["timeframe"], x["symbol"], x["sig_time"]) for x in results], sort_keys=True
    )
    return hashlib.sha256(value.encode()).hexdigest()


def run_automatic_scan(
    scheduled_for: datetime | None = None, timeframes=("1h",)
) -> None:
    timeframes = tuple(dict.fromkeys(timeframes))
    slot_label = scheduled_for.strftime("%H:%M") if scheduled_for else "scheduled"
    timeframe_label = " + ".join(timeframe.upper() for timeframe in timeframes)
    if not _state["access_token"]:
        record_activity(
            f"Automatic {timeframe_label} scan {slot_label} skipped: Upstox login is not active.",
            "warn",
        )
        return
    if not _scan_lock.acquire(blocking=False):
        record_activity(
            f"Automatic {timeframe_label} scan {slot_label} waiting: another scan is still running.",
            "warn",
        )
        return
    if "1d" in timeframes:
        _state["daily_scan_date"] = datetime.now(IST).date().isoformat()
    begin_scan_progress("automatic", slot_label, timeframes)
    update_timeframe_results({timeframe: [] for timeframe in timeframes})
    _state["last_scanned_count"] = 0
    record_activity(
        f"Automatic {timeframe_label} scan {slot_label} started for {INDEX_LABEL}."
    )
    try:
        scan_params = {
            "length": int(os.getenv("PKAMA_LENGTH", "50")),
            "self_powered": os.getenv("PKAMA_SELF_POWERED", "true").lower() == "true",
            "factor": float(os.getenv("PKAMA_FACTOR", "3")),
            "vol_mult": float(os.getenv("VOLUME_MULTIPLIER", "1.5")),
            "timeframes": list(timeframes),
        }
        def track_progress(event: dict) -> None:
            update_scan_progress(event)
            if event.get("type") == "signal":
                timeframe = event.get("timeframe")
                if timeframe in timeframes:
                    _state[f"last_results_{timeframe}"].append(
                        {key: value for key, value in event.items() if key != "type"}
                    )
                    _state["last_results"] = (
                        _state["last_results_1h"] + _state["last_results_1d"]
                    )

        results_by_timeframe = scan_timeframes(
            timeframes,
            scan_params["length"],
            scan_params["self_powered"],
            scan_params["factor"],
            scan_params["vol_mult"],
            emit=track_progress,
        )
        results = [
            result
            for timeframe in timeframes
            for result in results_by_timeframe[timeframe]
        ]
        update_timeframe_results(results_by_timeframe)
        save_scan_results(results, "automatic", scan_params)
        scanned_count = len(fetch_nifty500_instruments())
        journal_files = []
        for timeframe in timeframes:
            journal_files.extend(
                append_timeframe_journal(
                    timeframe,
                    results_by_timeframe[timeframe],
                    "automatic",
                    scan_params,
                    scanned_count,
                )
            )
        _state["last_saved_files"].extend(journal_files)
        one_hour_count = sum(row["timeframe"] == "1h" for row in results)
        one_day_count = sum(row["timeframe"] == "1d" for row in results)
        _state["last_scanned_count"] = scanned_count
        _state["last_scan_time"] = datetime.now(IST).isoformat()
        _state["last_scan_status"] = (
            f"Automatic {timeframe_label} scan complete: "
            f"{one_hour_count} 1H + {one_day_count} 1D signal(s)"
        )
        try:
            notify_if_new(results)
        except Exception as exc:
            record_activity(f"Signal email could not be sent: {exc}", "warn")
        record_activity(
            f"Automatic {timeframe_label} scan {slot_label} completed: {scanned_count} stocks; "
            f"{one_hour_count} 1H + {one_day_count} 1D signal(s); journal autosaved.",
            "ok",
        )
    except Exception as exc:
        logging.exception("Automatic scan failed")
        _state["last_scan_status"] = f"Scan error: {exc}"
        if "1d" in timeframes:
            _state["daily_scan_date"] = None
        record_activity(
            f"Automatic {timeframe_label} scan {slot_label} failed: {exc}", "err"
        )
    finally:
        finish_scan_progress()
        _scan_lock.release()


def notify_if_new(results: list[dict]) -> None:
    current_fingerprint = fingerprint(results) if results else None
    if results and current_fingerprint != _state["last_email_fingerprint"]:
        send_signal_email(results)
        _state["last_email_fingerprint"] = current_fingerprint


def scheduler_loop() -> None:
    last_slot_key = None
    daily_wait_logged_date = None
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
        market_open = now.weekday() < 5 and clock_time(9, 15) <= now.time() <= clock_time(15, 25)
        slot_key = (now.date().isoformat(), slot) if due else None
        if market_open and due and slot_key != last_slot_key:
            last_slot_key = slot_key
            session_start = now.replace(hour=9, minute=15, second=0, microsecond=0)
            scheduled_for = session_start + timedelta(minutes=first_slot + slot * interval_minutes)
            record_activity(f"Scheduled 1H slot {scheduled_for.strftime('%H:%M')} reached.")
            threading.Thread(
                target=run_automatic_scan,
                args=(scheduled_for, ("1h",)),
                daemon=True,
            ).start()

        # The daily candle is scanned once only, after Upstox has had a few
        # minutes to publish the completed 15:30 NSE cash-session candle.
        today = now.date().isoformat()
        daily_due = (
            now.weekday() < 5
            and clock_time(15, 35) <= now.time() <= clock_time(16, 30)
            and _state["daily_scan_date"] != today
        )
        if daily_due:
            if not _state["access_token"]:
                if daily_wait_logged_date != today:
                    daily_wait_logged_date = today
                    record_activity(
                        "Post-close 1D scan is waiting for Upstox login.", "warn"
                    )
            elif not _scan_lock.locked():
                daily_for = now.replace(hour=15, minute=35, second=0, microsecond=0)
                record_activity("Post-close 1D scan slot 15:35 reached.")
                threading.Thread(
                    target=run_automatic_scan,
                    args=(daily_for, ("1d",)),
                    daemon=True,
                ).start()
            elif daily_wait_logged_date != today:
                daily_wait_logged_date = today
                record_activity(
                    "Post-close 1D scan is queued until the current 1H/manual scan finishes.",
                    "warn",
                )
        time.sleep(20)


def start_scheduler() -> None:
    global _scheduler_started
    if not _scheduler_started:
        _scheduler_started = True
        record_activity(
            "Automatic scheduler active: 1H at 10:16, 11:16, 12:16, 13:16, 14:16 and 15:16; "
            "1D once post-close at 15:35 IST."
        )
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


@app.route("/api/download/journal/<timeframe>.csv")
def download_journal(timeframe: str):
    if timeframe not in {"1h", "1d"}:
        return jsonify({"error": "Timeframe must be 1h or 1d."}), 400
    path = os.path.join(
        os.getenv("SCAN_DATA_DIR", "data"), "journals", f"journal_{timeframe}.csv"
    )
    if not os.path.exists(path):
        return jsonify({"error": f"No {timeframe.upper()} journal has been saved yet."}), 404
    return send_file(
        path,
        mimetype="text/csv",
        as_attachment=True,
        download_name=f"pkama_{timeframe}_journal.csv",
    )


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
    now = datetime.now(IST)
    timeframes = ("1h", "1d") if now.time() >= clock_time(15, 35) else ("1h",)

    events = queue.Queue()

    def emit(event):
        update_scan_progress(event)
        events.put(event)

    begin_scan_progress("manual", timeframes=timeframes)

    def worker():
        try:
            timeframe_label = " + ".join(timeframe.upper() for timeframe in timeframes)
            if "1d" not in timeframes:
                record_activity(
                    "Manual 1D scan skipped before 15:35 IST; the daily candle is not confirmed yet.",
                    "warn",
                )
            record_activity(f"Manual {timeframe_label} scan started for {INDEX_LABEL}.")
            results_by_timeframe = scan_timeframes(
                timeframes, length, self_powered, factor, vol_mult, emit=emit
            )
            results = [
                result
                for timeframe in timeframes
                for result in results_by_timeframe[timeframe]
            ]
            scan_params = {
                "length": length,
                "self_powered": self_powered,
                "factor": factor,
                "vol_mult": vol_mult,
                "timeframes": list(timeframes),
            }
            save_scan_results(
                results,
                "manual",
                scan_params,
            )
            scanned_count = len(fetch_nifty500_instruments())
            journal_files = []
            for timeframe in timeframes:
                journal_files.extend(
                    append_timeframe_journal(
                        timeframe,
                        results_by_timeframe[timeframe],
                        "manual",
                        scan_params,
                        scanned_count,
                    )
                )
            _state["last_saved_files"].extend(journal_files)
            one_hour_count = sum(row["timeframe"] == "1h" for row in results)
            one_day_count = sum(row["timeframe"] == "1d" for row in results)
            update_timeframe_results(results_by_timeframe)
            _state["last_scanned_count"] = scanned_count
            _state["last_scan_time"] = datetime.now(IST).isoformat()
            _state["last_scan_status"] = (
                f"Manual {timeframe_label} scan complete: "
                f"{one_hour_count} 1H + {one_day_count} 1D signal(s)"
            )
            try:
                notify_if_new(results)
            except Exception as exc:
                record_activity(f"Signal email could not be sent: {exc}", "warn")
            record_activity(
                f"Manual {timeframe_label} scan completed: {scanned_count} stocks; "
                f"{one_hour_count} 1H + {one_day_count} 1D signal(s); journal autosaved.",
                "ok",
            )
            events.put(
                {
                    "type": "done",
                    "scanned": scanned_count,
                    "scanned_1h": scanned_count if "1h" in timeframes else 0,
                    "scanned_1d": scanned_count if "1d" in timeframes else 0,
                }
            )
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
        timeframe_label = " + ".join(timeframe.upper() for timeframe in timeframes)
        yield f"data: {json.dumps({'type': 'start', 'timeframes': list(timeframes)})}\n\n"
        yield f"data: {json.dumps({'type': 'log', 'msg': f'Scanning {INDEX_LABEL}: {timeframe_label} scan started…', 'cls': 'info'})}\n\n"
        while True:
            event = events.get()
            if event is None:
                break
            yield f"data: {json.dumps(event, default=str)}\n\n"

    return Response(generate(), mimetype="text/event-stream", headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


start_scheduler()


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)), debug=False, threaded=True)

