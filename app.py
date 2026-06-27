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

# ── Nifty 500 ──────────────────────────────────────────────────────────────
NIFTY500 = [
    "RELIANCE","TCS","HDFCBANK","ICICIBANK","BHARTIARTL","INFOSYS","SBIN","HINDUNILVR",
    "ITC","LT","KOTAKBANK","HCLTECH","BAJFINANCE","MARUTI","NTPC","ONGC","POWERGRID",
    "ULTRACEMCO","NESTLEIND","AXISBANK","WIPRO","ADANIENT","JSWSTEEL","TATASTEEL",
    "SUNPHARMA","TITAN","TECHM","ADANIPORTS","HDFCLIFE","BAJAJFINSV","COALINDIA",
    "INDUSINDBK","GRASIM","DIVISLAB","CIPLA","EICHERMOT","DRREDDY","BAJAJ-AUTO",
    "BPCL","TATACONSUM","HINDALCO","M&M","APOLLOHOSP","SBILIFE","BRITANNIA","HEROMOTOCO",
    "ASIANPAINT","LTIM","SHRIRAMFIN","CHOLAFIN","SIEMENS","PIDILITIND","DMART","MUTHOOTFIN",
    "GODREJCP","BOSCHLTD","ABB","HAVELLS","POLYCAB","MARICO","DABUR","BERGEPAINT",
    "COLPAL","AMBUJACEM","LUPIN","BIOCON","TORNTPHARM","IPCALAB","AUROPHARMA",
    "ALKEM","ABBOTINDIA","GLAXO","PFIZER","SANOFI","GLENMARK","NATCOPHARM",
    "ZYDUSLIFE","CONCOR","IRCTC","ADANIGREEN","ADANIPOWER","ADANITRANS","CANBK",
    "PNB","BANKBARODA","UNIONBANK","FEDERALBNK","IDFCFIRSTB","BANDHANBNK","RBLBANK",
    "AUBANK","DCBBANK","KARURVYSYA","SOUTHBANK","UJJIVANSFB","EQUITASBNK",
    "LICHSGFIN","MANAPPURAM","BAJAJHLDNG","RECLTD","PFC","IRFC","HUDCO",
    "MOTHERSON","EXIDEIND","AMARAJABAT","TIINDIA","SUNDRMFAST","CRAFTSMAN",
    "APLAPOLLO","JSWENERGY","TATAPOWER","CESC","TORNTPOWER","ATGL","IGL","MGL",
    "GUJGASLTD","PETRONET","GAIL","IOC","HPCL",
    "TATACHEM","AARTIIND","GNFC","COROMANDEL","PIIND","RALLIS","DEEPAKNTR",
    "NAVINFLUOR","ATUL","FINEORG","NOCIL","SUDARSCHEM",
    "VOLTAS","BLUESTARCO","SYMPHONY","CROMPTON","ORIENTELEC",
    "RAJESHEXPO","KALYANIJIN","SENCO","ZOMATO","NYKAA","PAYTM","POLICYBZR",
    "DELHIVERY","EASEMYTRIP","POWERMECH","KEC","KALPATPOWR","BHEL","AIAENG",
    "GRINDWELL","CARBORUNIV","CUMMINSIND","THERMAX","ELGIEQUIP","KIRLOSBROS",
    "ESCORTS","JKCEMENT","RAMCOCEM","HEIDELBERGCE","INDIACEM","DALMIACENT",
    "NUVOCO","BIRLACORPN","TATAELXSI","PERSISTENT","MPHASIS","COFORGE",
    "MASTEK","CYIENT","KPITTECH","LTTS","SONATSOFTW","HAPPSTMNDS","TANLA",
    "ROUTE","HFCL","STLTECH","RAILTEL","CMSINFO","RATEGAIN","JUSTDIAL",
    "INDIAMART","NAUKRI","ZENSARTECH","CRISIL","MFSL","ICICIGI","HDFCAMC",
    "NIPPONLIFE","ABSLAMC","ANGELONE","BSE","CDSL","MCX","MOTILALOFS",
    "PVRINOX","INOXWIND","GIPCL","SUZLON","WAAREEENER","OLECTRA","ESAB",
    "SKFINDIA","SCHAEFFLER","TIMKEN","NRB","SUPRAJIT","FINOLEX","BAJAJELEC",
    "HINDPETRO","CASTROLIND","GODFRYPHLP","VSTIND","RADICO","UNITDSPR",
    "GLOBUSSPR","TATACOMM","NAZARA","ZEEL","SUNTV","VENKEYS","HATSUN",
    "AVANTIFEED","WATERBASE","COCHINSHIP","GRSE","BEML","HAL","BEL",
    "DATAPATTNS","IDEAFORGE","SOLARINDS","MTAR","NEWGEN","INTELLECT",
    "NUCLEUS","ZENSAR","OFSS","3MINDIA","HONAUT","PGHH","GILLETTE",
    "EMAMILTD","ZYDUSWELL","JYOTHYLAB","BAYERCROP","FDC","SHILPAMED",
    "POLYMED","NEULANDLAB","GRANULES","CAPLIPOINT","LALPATHLAB","METROPOLIS",
    "MAXHEALTH","NARAYHEALTH","FORTIS","SHOPERSTOP","TRENT","VMART","ABFRL",
    "MANYAVAR","VEDL","HINDZINC","NMDC","MOIL","GMRINFRA","IRB","RITES",
    "IRCON","NBCC","NHPC","SJVN","SYNGENE","DIVI","LAURUSLABS","SEQUENT",
    "SOLARA","MEDPLUS","VIJAYA","YATHARTH","NUVAMA","JMFINANCIL","IIFL",
    "PNBHOUSING","CANFINHOME","APTUS","HOMEFIRST","AAVAS","REPCO",
    "MAHINDCIE","ENDURANCE","MINDAIND","LUMAX","ASAHIINDIA","MINDA",
    "GABRIEL","SHARDAMOTR","TTKPRESTIG","HAWKINCOOK","VAIBHAVGBL",
    "THANGAMAY","PCJEWELLER","KALYAN","DOMS","KAYNES","SYRMA","AVALON",
    "DIXON","AMBER","PGEL","APCOTEXIND","VINDHYATEL"
]
seen = set()
NIFTY500_UNIQUE = [s for s in NIFTY500 if not (s in seen or seen.add(s))]

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

def detect_cross(closes, kama):
    n = len(closes)
    if n < 3: return None
    i, p = n - 2, n - 3
    if math.isnan(kama[p]) or math.isnan(kama[i]): return None
    if closes[p] <= kama[p] and closes[i] > kama[i]: return 'bullish'
    if closes[p] >= kama[p] and closes[i] < kama[i]: return 'bearish'
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
        return render_template("callback_error.html", msg="No request_token in URL.")
    try:
        kite = KiteConnect(api_key=API_KEY)
        data = kite.generate_session(request_token, api_secret=API_SECRET)
        _state["access_token"] = data["access_token"]
        _state["token_time"]   = datetime.now()
        print(f"✅ Token refreshed: {_state['token_time'].strftime('%I:%M %p')}")
        return redirect("/")
    except Exception as e:
        return render_template("callback_error.html", msg=str(e))

@app.route("/api/token-status")
def token_status():
    return jsonify({
        "set":  _state["access_token"] is not None,
        "time": _state["token_time"].strftime("%d %b, %I:%M %p") if _state["token_time"] else None
    })

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
            yield send({"type":"log","msg":"Fetching NSE instrument list…","cls":"info"})
            inst_map = get_instruments()
            yield send({"type":"log","msg":f"Loaded {len(inst_map)} instruments.","cls":"ok"})
            yield send({"type":"connected"})
        except Exception as e:
            yield send({"type":"log","msg":f"✗ Failed: {e}","cls":"err"})
            yield send({"type":"error","msg":str(e)})
            return

        to_dt   = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        from_dt = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d %H:%M:%S")
        total   = len(NIFTY500_UNIQUE)
        yield send({"type":"log","msg":f"Scanning {total} symbols on 1H TF…","cls":"info"})

        for idx, sym in enumerate(NIFTY500_UNIQUE):
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
                signal  = detect_cross(closes, kama)
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
