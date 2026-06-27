"""
P-KAMA Nifty 500 Scanner — Railway Deployment
Credentials come from Railway environment variables (never hardcoded)
"""

from flask import Flask, jsonify, request, render_template, Response, redirect
from kiteconnect import KiteConnect
import requests, math, json, time, os
from datetime import datetime, timedelta

app = Flask(__name__)

# ── Credentials from Railway environment variables ─────────────────────────
API_KEY    = os.environ.get("KITE_API_KEY", "")
API_SECRET = os.environ.get("KITE_API_SECRET", "")
KITE_BASE  = "https://api.kite.trade"

# ── In-memory token (lives as long as Railway container is up) ─────────────
_state = {"access_token": None, "token_time": None}

# ── Live Nifty Total Market list from NSE ───────────────────────────────────────────
# Cached so we only fetch once per app restart
_nifty_cache = []

def fetch_nifty500_symbols():
    """
    Fetch Nifty Total Market symbols from NSE CSV — most reliable method.
    Falls back to Nifty 500 CSV if Total Market fails.
    """
    global _nifty_cache
    if _nifty_cache:
        return _nifty_cache

    session = requests.Session()
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                      "Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,*/*",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.nseindia.com/",
    }

    # Warm up session with cookies first
    try:
        session.get("https://www.nseindia.com", headers=headers, timeout=10)
        time.sleep(1)
    except Exception:
        pass

    # Try multiple sources in order of preference
    sources = [
        # 1. Nifty Total Market CSV
        ("Nifty Total Market",
         "https://nsearchives.nseindia.com/content/indices/ind_niftytotalmarket_list.csv"),
        # 2. Nifty 500 CSV (fallback)
        ("Nifty 500",
         "https://nsearchives.nseindia.com/content/indices/ind_nifty500list.csv"),
    ]

    for name, url in sources:
        try:
            resp = session.get(url, headers=headers, timeout=15)
            resp.raise_for_status()
            lines = resp.text.strip().split("\n")
            symbols = []
            for line in lines[1:]:   # skip header
                cols = line.split(",")
                if len(cols) >= 3:
                    sym = cols[2].strip().strip('"')   # Symbol is 3rd column
                    if sym and sym.isalpha() or (sym and "-" in sym):
                        symbols.append(sym)
            if len(symbols) > 100:
                _nifty_cache = symbols
                print(f"✅ Fetched {len(symbols)} symbols from {name} CSV")
                return symbols
            else:
                print(f"⚠️ {name} CSV returned only {len(symbols)} symbols, trying next...")
        except Exception as e:
            print(f"⚠️ {name} CSV failed: {e}, trying next...")

    # Final fallback — use Kite instruments filtered by EQ series (all liquid NSE stocks)
    print("⚠️ All NSE CSV sources failed — using Kite instrument list directly")
    return []   # empty = will be handled below in scan route

# ── Helpers ────────────────────────────────────────────────────────────────
def kite_headers():
    return {
        "X-Kite-Version": "3",
        "Authorization": f"token {API_KEY}:{_state['access_token']}"
    }

def get_instruments():
    r = requests.get(f"{KITE_BASE}/instruments/NSE", headers=kite_headers(), timeout=15)
    r.raise_for_status()
    lines = r.text.strip().split("\n")
    hdrs  = [h.strip() for h in lines[0].split(",")]
    ti, si = hdrs.index("instrument_token"), hdrs.index("tradingsymbol")
    m = {}
    for line in lines[1:]:
        cols = line.split(",")
        if len(cols) > max(ti, si):
            m[cols[si].strip()] = cols[ti].strip()
    return m

def get_historical(token, from_dt, to_dt):
    url = (f"{KITE_BASE}/instruments/historical/{token}/60minute"
           f"?from={from_dt}&to={to_dt}&continuous=0&oi=0")
    r = requests.get(url, headers=kite_headers(), timeout=15)
    r.raise_for_status()
    return r.json().get("data", {}).get("candles", [])

def calc_pkama(closes, length, self_powered=True, factor=3.0):
    n    = len(closes)
    kama = [float('nan')] * n
    for i in range(length, n):
        direction  = abs(closes[i] - closes[i - length])
        volatility = sum(abs(closes[j] - closes[j-1]) for j in range(i - length + 1, i + 1))
        er      = (direction / volatility) if volatility != 0 else 0.0
        pow_val = (1.0 / er) if (self_powered and er != 0) else (1e9 if self_powered else factor)
        per     = er ** pow_val
        prev    = closes[i] if math.isnan(kama[i-1]) else kama[i-1]
        kama[i] = per * closes[i] + (1 - per) * prev
    return kama

def detect_cross(closes, kama, times):
    """
    Signal ONLY on the last completed 1H bar.
    Verifies the crossover bar is within the last 2 trading hours
    (i.e. it IS the last completed bar, not an old bar).
    n-1 = current forming bar (ignored)
    n-2 = last completed bar  (must be the crossover bar)
    n-3 = bar before that
    """
    n = len(closes)
    if n < 3: return None
    last = n - 2  # last completed bar
    prev = n - 3  # bar before that
    if math.isnan(kama[prev]) or math.isnan(kama[last]): return None

    # Parse the timestamp of the last completed bar
    try:
        sig_time = times[last]
        if isinstance(sig_time, str):
            sig_dt = datetime.fromisoformat(sig_time.replace("Z","")).replace(tzinfo=None)
        else:
            sig_dt = sig_time.replace(tzinfo=None) if hasattr(sig_time, 'tzinfo') else sig_time
        # Must be within last 2 hours (i.e. the most recent closed 1H bar)
        age_hours = (datetime.now() - sig_dt).total_seconds() / 3600
        if age_hours > 2:
            return None   # stale — not the last bar
    except Exception:
        pass  # if time parse fails, still check crossover

    if closes[prev] <= kama[prev] and closes[last] > kama[last]: return 'bullish'
    if closes[prev] >= kama[prev] and closes[last] < kama[last]: return 'bearish'
    return None

def check_volume(volumes, vol_mult=1.5):
    n = len(volumes)
    if n < 22: return False, 0.0
    avg   = sum(volumes[n-22:n-2]) / 20
    ratio = (volumes[n-2] / avg) if avg > 0 else 0.0
    return ratio >= vol_mult, round(ratio, 2)

# ══════════════════════════════════════════════════════════════════════════
#  ROUTES
# ══════════════════════════════════════════════════════════════════════════

@app.route("/")
def index():
    kite      = KiteConnect(api_key=API_KEY)
    login_url = kite.login_url()
    token_set = _state["access_token"] is not None
    time_str  = _state["token_time"].strftime("%d %b %Y, %I:%M %p") if _state["token_time"] else None
    return render_template("index.html",
                           login_url=login_url,
                           token_set=token_set,
                           token_time=time_str)

# ── Zerodha redirects here after login ────────────────────────────────────
@app.route("/callback")
def callback():
    request_token = request.args.get("request_token")
    if not request_token:
        return "<h2 style='font-family:monospace;color:#ff4d6d'>❌ No request_token found. Please try logging in again.</h2>"
    try:
        kite = KiteConnect(api_key=API_KEY)
        data = kite.generate_session(request_token, api_secret=API_SECRET)
        _state["access_token"] = data["access_token"]
        _state["token_time"]   = datetime.now()
        print(f"✅ Token refreshed at {_state['token_time'].strftime('%I:%M %p')}")
        return redirect("/")
    except Exception as e:
        return f"<h2 style='font-family:monospace;color:#ff4d6d'>❌ Login failed: {e}</h2>"

@app.route("/api/token-status")
def token_status():
    return jsonify({
        "set":  _state["access_token"] is not None,
        "time": _state["token_time"].strftime("%d %b, %I:%M %p") if _state["token_time"] else None
    })

# ── Drojun pushes the fresh token here after every login ──────────────────
@app.route("/receive_token", methods=["POST"])
def receive_token():
    try:
        data = request.json
        token = data.get("access_token")
        if not token:
            return jsonify({"status": "error", "msg": "No token provided"}), 400
        _state["access_token"] = token
        _state["token_time"]   = datetime.now()
        print(f"✅ Token received from Drojun at {_state['token_time'].strftime('%I:%M %p')}")
        return jsonify({"status": "ok"})
    except Exception as e:
        return jsonify({"status": "error", "msg": str(e)}), 500

@app.route("/api/scan")
def scan():
    if not _state["access_token"]:
        def no_token():
            yield f"data: {json.dumps({'type':'error','msg':'Please login with Zerodha first.'})}\n\n"
        return Response(no_token(), mimetype="text/event-stream")

    length       = int(request.args.get("length", 50))
    self_powered = request.args.get("self_powered", "true").lower() == "true"
    factor       = float(request.args.get("factor", 3.0))
    vol_mult     = float(request.args.get("vol_mult", 1.5))

    def generate():
        def send(obj):
            return f"data: {json.dumps(obj)}\n\n"

        try:
            yield send({"type":"log","msg":"Fetching Nifty Total Market list from NSE…","cls":"info"})
            symbols = fetch_nifty500_symbols()

            yield send({"type":"log","msg":"Fetching Kite instrument tokens…","cls":"info"})
            inst_map = get_instruments()
            yield send({"type":"log","msg":f"Loaded {len(inst_map)} instruments.","cls":"ok"})

            # If NSE CSV failed, build symbol list from Kite instruments (EQ series only)
            if not symbols:
                yield send({"type":"log","msg":"NSE fetch failed — using Kite EQ instrument list…","cls":"warn"})
                symbols = [sym for sym, tok in inst_map.items()
                           if not any(c.isdigit() for c in sym)
                           and len(sym) <= 20]
                yield send({"type":"log","msg":f"Using {len(symbols)} NSE EQ symbols from Kite.","cls":"warn"})
            else:
                yield send({"type":"log","msg":f"Got {len(symbols)} Nifty Total Market symbols.","cls":"ok"})

            yield send({"type":"connected"})
        except Exception as e:
            yield send({"type":"log","msg":f"✗ Failed: {e}","cls":"err"})
            yield send({"type":"error","msg":str(e)})
            return

        to_dt   = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        from_dt = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d %H:%M:%S")
        total   = len(symbols)
        yield send({"type":"log","msg":f"Scanning {total} symbols on 1H TF…","cls":"info"})

        for idx, sym in enumerate(symbols):
            token    = inst_map.get(sym)
            progress = round((idx / total) * 100)
            yield send({"type":"progress","pct":progress,"scanned":idx})
            if not token:
                continue
            try:
                candles = get_historical(token, from_dt, to_dt)
                if not candles or len(candles) < length + 5:
                    continue
                closes  = [c[4] for c in candles]
                volumes = [c[5] for c in candles]
                times   = [c[0] for c in candles]
                kama    = calc_pkama(closes, length, self_powered, factor)
                signal  = detect_cross(closes, kama, times)
                if signal:
                    vol_pass, vol_ratio = check_volume(volumes, vol_mult)
                    if vol_pass:
                        ltp      = closes[-2]
                        pkama    = kama[-2]
                        dist_pct = round((ltp - pkama) / pkama * 100, 2) if pkama else 0
                        sk       = [None if math.isnan(v) else round(v,2) for v in kama[-20:]]
                        yield send({
                            "type":"signal","symbol":sym,"signal":signal,
                            "ltp":round(ltp,2),"pkama":round(pkama,2),
                            "dist_pct":dist_pct,"vol_ratio":vol_ratio,
                            "sig_time":times[-2],
                            "spark_closes":[round(v,2) for v in closes[-20:]],
                            "spark_kama":sk,
                        })
                        yield send({"type":"log",
                                    "msg":f"✓ {sym} — {signal.upper()} @ ₹{ltp:.2f} | Vol: {vol_ratio:.2f}x",
                                    "cls":"ok"})
                time.sleep(0.35)
            except Exception as e:
                yield send({"type":"log","msg":f"✗ {sym}: {e}","cls":"err"})
                time.sleep(0.5)

        yield send({"type":"progress","pct":100,"scanned":total})
        yield send({"type":"done","scanned":total})
        yield send({"type":"log","msg":"Scan complete!","cls":"ok"})

    return Response(generate(), mimetype="text/event-stream",
                    headers={"Cache-Control":"no-cache","X-Accel-Buffering":"no"})

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    print(f"\n🟢  P-KAMA Scanner running on port {port}\n")
    app.run(host="0.0.0.0", port=port, debug=False, threaded=True)
