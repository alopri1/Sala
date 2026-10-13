"""
EURUSD Engine v3
Fondamental quantitatif -> Biais -> Structure multi-timeframe (1m -> mensuel) -> Clusters de confluence
(OB, imbalance, breaker, liquidité, VWAP/POC/VAH/VAL, sessions...) -> Réaction -> DXY/SMT -> SETUP CONFIRMED -> Entry/SL/TP/R:R

Outil d'aide à la décision, pas un conseil financier. Aucune stratégie n'est validée : teste en démo avant tout argent réel.

Variables d'environnement (toutes OPTIONNELLES, aucune clé n'est nécessaire) :
  FRED_KEY : plus stable que le CSV public de FRED (gratuit)
  TD_KEY   : source de secours EUR/USD (Twelve Data, gratuit)
"""
import os, io, csv, math, time, threading
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor

import requests
from flask import Flask, jsonify, Response

try:
    from zoneinfo import ZoneInfo
    NYZ, LNZ = ZoneInfo("America/New_York"), ZoneInfo("Europe/London")
except Exception:                      # tzdata absent : repli sur des décalages fixes
    NYZ = LNZ = None

FRED_KEY = os.getenv("FRED_KEY")
TD_KEY = os.getenv("TD_KEY")
PIP = 0.0001
MIN_RR = 1.5          # R:R minimum pour valider un setup
FUND_TH = 25          # score fondamental (sur ±100) pour avoir un biais
TECH_TH = 25          # score technique multi-timeframe (sur ±100) pour avoir un biais
MIN_SETUP = 9.0       # score de confluence minimum (grade B)
UA = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/125 Safari/537.36"}

app = Flask(__name__)
LOCK = threading.RLock()
CLOCK = threading.Lock()
CACHE = {}
STATE = {"plan": None, "log": [], "res": (0, None)}


def cached(key, ttl, fn):
    with CLOCK:
        e = CACHE.get(key)
    if e and time.time() - e[0] < ttl:
        return e[1]
    try:
        v = fn()
    except Exception:
        if e and time.time() - e[0] < ttl * 20 + 900:
            return e[1]            # garde l'ancienne valeur si la source tombe
        raise
    with CLOCK:
        CACHE[key] = (time.time(), v)
    return v


def log(msg):
    STATE["log"].insert(0, {"t": datetime.now(timezone.utc).strftime("%d/%m %H:%M UTC"), "msg": msg})
    del STATE["log"][14:]


# ====================== TEMPS & SESSIONS ======================
def _loc(t, kind):
    d = datetime.fromtimestamp(t, timezone.utc)
    if kind == "ny":
        return d.astimezone(NYZ) if NYZ else d - timedelta(hours=5)
    return d.astimezone(LNZ) if LNZ else d


def fxday(t):                          # jour FX : bascule à 17:00 New York
    return (_loc(t, "ny") + timedelta(hours=7)).date()


def fxweek(t):
    iso = fxday(t).isocalendar()
    return (iso[0], iso[1])


def session(t):
    hl, hn = _loc(t, "ln").hour, _loc(t, "ny").hour
    if hl < 7:
        return "asia"
    if hl < 12:
        return "london"
    return "ny" if 8 <= hn < 17 else "off"


def killzone(t):
    hl, hn = _loc(t, "ln").hour, _loc(t, "ny").hour
    return "Londres" if 7 <= hl < 10 else ("New York" if 7 <= hn < 10 else None)


def market_closed(t):
    n = _loc(t, "ny")
    return n.weekday() == 5 or (n.weekday() == 4 and n.hour >= 17) or (n.weekday() == 6 and n.hour < 17)


# ====================== DONNÉES ======================
def yahoo(symbol, rng, interval):
    last = None
    for host in ("query1", "query2"):
        try:
            r = requests.get(f"https://{host}.finance.yahoo.com/v8/finance/chart/{symbol}",
                             params=dict(range=rng, interval=interval), headers=UA, timeout=15)
            r.raise_for_status()
            res = r.json()["chart"]["result"][0]
            ts, q = res["timestamp"], res["indicators"]["quote"][0]
            vol = q.get("volume") or [0] * len(ts)
            out = []
            for i, t in enumerate(ts):
                o, h, l, c = q["open"][i], q["high"][i], q["low"][i], q["close"][i]
                if None in (o, h, l, c):
                    continue
                out.append(dict(t=t, o=o, h=h, l=l, c=c, v=vol[i] or 0))
            if len(out) < 5:
                raise ValueError("historique vide")
            return out
        except Exception as e:
            last = e
    raise RuntimeError(f"Yahoo {symbol} {interval}: {last}")


TD_MAP = {"1m": "1min", "5m": "5min", "15m": "15min", "1h": "1h", "1d": "1day"}


def twelve(tf, n=400):
    r = requests.get("https://api.twelvedata.com/time_series", timeout=20,
                     params=dict(symbol="EUR/USD", interval=TD_MAP[tf], outputsize=n, timezone="UTC",
                                 order="ASC", apikey=TD_KEY)).json()
    if "values" not in r:
        raise RuntimeError(r.get("message", "Twelve Data error"))
    return [dict(t=int(datetime.fromisoformat(x["datetime"]).replace(tzinfo=timezone.utc).timestamp()),
                 o=float(x["open"]), h=float(x["high"]), l=float(x["low"]), c=float(x["close"]), v=0)
            for x in r["values"]]


TFS = {"1m": ("2d", "1m", 40), "5m": ("5d", "5m", 50), "15m": ("1mo", "15m", 80), "1h": ("3mo", "60m", 300),
       "1d": ("2y", "1d", 1500), "1w": ("10y", "1wk", 7200), "1M": ("20y", "1mo", 14400)}


def spot(tf):
    rng, iv, ttl = TFS[tf]

    def f():
        try:
            return yahoo("EURUSD=X", rng, iv)
        except Exception:
            if TD_KEY and tf in TD_MAP:
                return twelve(tf)
            raise
    return cached("px_" + tf, ttl, f)


def load_all():
    jobs = {"s_" + k: (lambda k=k: spot(k)) for k in TFS}
    jobs["f15"] = lambda: cached("f15", 80, lambda: [x for x in yahoo("6E=F", "1mo", "15m") if x["v"] > 0])
    jobs["f5"] = lambda: cached("f5", 50, lambda: [x for x in yahoo("6E=F", "5d", "5m") if x["v"] > 0])
    jobs["dx15"] = lambda: cached("dx15", 80, lambda: yahoo("DX-Y.NYB", "5d", "15m"))
    jobs["dx1h"] = lambda: cached("dx1h", 300, lambda: yahoo("DX-Y.NYB", "1mo", "60m"))
    jobs["dx1d"] = lambda: cached("dx1d", 1800, lambda: yahoo("DX-Y.NYB", "1y", "1d"))
    jobs["gb15"] = lambda: cached("gb15", 80, lambda: yahoo("GBPUSD=X", "5d", "15m"))
    jobs["vix"] = lambda: cached("vix", 1800, lambda: yahoo("^VIX", "3mo", "1d"))
    out, err = {}, {}
    with ThreadPoolExecutor(max_workers=6) as ex:
        futs = {k: ex.submit(fn) for k, fn in jobs.items()}
        for k, f in futs.items():
            try:
                out[k] = f.result(timeout=45)
            except Exception as e:
                err[k] = str(e)[:90]
    return out, err


def resample(bars, hours=4, offset=2):
    """H4 aligné sur 22h/02h/06h... UTC à partir du 1h."""
    out, cur, key = [], None, None
    for b in bars:
        k = (b["t"] + offset * 3600) // (hours * 3600)
        if k != key:
            if cur:
                out.append(cur)
            cur, key = dict(t=k * hours * 3600 - offset * 3600, o=b["o"], h=b["h"], l=b["l"], c=b["c"], v=b["v"]), k
        else:
            cur["h"], cur["l"], cur["c"], cur["v"] = max(cur["h"], b["h"]), min(cur["l"], b["l"]), b["c"], cur["v"] + b["v"]
    if cur:
        out.append(cur)
    return out


# ====================== STATS ======================
def mean(x):
    return sum(x) / len(x)


def std(x):
    if len(x) < 2:
        return 0.0
    m = mean(x)
    return (sum((v - m) ** 2 for v in x) / (len(x) - 1)) ** 0.5


def clamp(x, a, b):
    return max(a, min(b, x))


def sign(x, th):
    return 1 if x >= th else (-1 if x <= -th else 0)


def zchg(vals, lag):
    """z-score du dernier changement sur `lag` points par rapport à son historique."""
    d = [vals[i] - vals[i - lag] for i in range(lag, len(vals))]
    if len(d) < 25:
        return None
    s = std(d) or 1e-9
    return (d[-1] - mean(d)) / s, d[-1]


def align(a, b):
    db = dict(b)
    return {d: (v, db[d]) for d, v in a if d in db}


# ====================== 1. FONDAMENTAL QUANTITATIF ======================
def _get(url, params=None):
    r = requests.get(url, params=params, headers=UA, timeout=20)
    r.raise_for_status()
    return r


def fred(series, days=1100):
    start = (datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y-%m-%d")
    if FRED_KEY:
        j = _get("https://api.stlouisfed.org/fred/series/observations",
                 dict(series_id=series, api_key=FRED_KEY, file_type="json", observation_start=start)).json()
        rows = [(o["date"], float(o["value"])) for o in j["observations"] if o["value"] not in (".", "")]
    else:                              # CSV public, sans clé
        rd = csv.reader(io.StringIO(_get("https://fred.stlouisfed.org/graph/fredgraph.csv",
                                         {"id": series, "cosd": start}).text))
        next(rd, None)
        rows = [(r[0], float(r[1])) for r in rd if len(r) > 1 and r[1] not in (".", "")]
    if not rows:
        raise ValueError(series)
    return rows


def ecb(key, n=420):
    rd = csv.DictReader(io.StringIO(_get(f"https://data-api.ecb.europa.eu/service/data/{key}",
                                         {"lastNObservations": n, "format": "csvdata"}).text))
    rows = [(r["TIME_PERIOD"], float(r["OBS_VALUE"])) for r in rd if r.get("OBS_VALUE")]
    if not rows:
        raise ValueError(key)
    return rows


def cot_rows():
    rows = _get("https://publicreporting.cftc.gov/resource/6dca-aqww.json",
                {"cftc_contract_market_code": "099741", "$order": "report_date_as_yyyy_mm_dd DESC", "$limit": 70}).json()
    net = [int(float(r["noncomm_positions_long_all"])) - int(float(r["noncomm_positions_short_all"])) for r in rows]
    return {"net": net, "date": rows[0]["report_date_as_yyyy_mm_dd"][:10]}


RAW = {
    "us2": (3600, lambda: fred("DGS2", 800)), "us10": (3600, lambda: fred("DGS10", 800)),
    "real10": (3600, lambda: fred("DFII10", 800)), "dff": (3600, lambda: fred("DFF", 400)),
    "ecbr": (3600, lambda: fred("ECBDFR", 400)), "core": (21600, lambda: fred("CPILFESL", 1100)),
    "unrate": (21600, lambda: fred("UNRATE", 1100)), "pay": (21600, lambda: fred("PAYEMS", 1100)),
    "nfci": (7200, lambda: fred("NFCI", 900)),
    "ea2": (3600, lambda: ecb("YC/B.U2.EUR.4F.G_N_A.SV_C_YM.SR_2Y")),
    "ea10": (3600, lambda: ecb("YC/B.U2.EUR.4F.G_N_A.SV_C_YM.SR_10Y")),
    "hicp": (21600, lambda: ecb("ICP/M.U2.N.000000.4.ANR", 40)), "cot": (21600, cot_rows),
}


def comp(label, z, w, note):
    return {"label": label, "score": round(clamp(z, -2, 2), 2), "w": w, "note": note}


def fund_static():
    """Composantes qui ne dépendent pas du prix : données macro officielles (FRED, BCE, CFTC)."""
    R, miss = {}, []
    with ThreadPoolExecutor(max_workers=6) as ex:
        futs = {k: ex.submit(cached, "raw_" + k, ttl, fn) for k, (ttl, fn) in RAW.items()}
        for k, f in futs.items():
            try:
                R[k] = f.result(timeout=40)
            except Exception:
                miss.append(k)
    C, info, spread2 = [], {}, None
    try:
        al = align(R["ea2"], R["us2"])
        ds = sorted(al)
        sp = [al[d][0] - al[d][1] for d in ds]
        spread2 = (ds, sp)
        z = zchg(sp, 20)
        info["spread2"] = round(sp[-1], 2)
        C.append(comp("Écart de rendement 2 ans EUR−US", z[0], 3.0,
                      "%+.2f pt (Δ20j %+.2f, z=%+.1f) : premier moteur du change à court terme" % (sp[-1], z[1], z[0])))
    except Exception:
        pass
    try:
        al = align(R["ea10"], R["us10"])
        ds = sorted(al)
        sp = [al[d][0] - al[d][1] for d in ds]
        z = zchg(sp, 20)
        info["spread10"] = round(sp[-1], 2)
        C.append(comp("Écart de rendement 10 ans EUR−US", z[0], 1.5,
                      "%+.2f pt (Δ20j %+.2f, z=%+.1f)" % (sp[-1], z[1], z[0])))
    except Exception:
        pass
    try:
        z = zchg([v for _, v in R["real10"]], 20)
        C.append(comp("Taux réel US 10 ans (TIPS)", -z[0], 2.0,
                      "%.2f%% (Δ20j %+.2f, z=%+.1f) : hausse = dollar soutenu" % (R["real10"][-1][1], z[1], z[0])))
    except Exception:
        pass
    try:
        al = align(R["ecbr"], R["dff"])
        ds = sorted(al)
        dif = [al[d][0] - al[d][1] for d in ds]
        info["policy_diff"] = round(dif[-1], 2)
        info["fed"], info["ecb"] = round(al[ds[-1]][1], 2), round(al[ds[-1]][0], 2)
        ch = dif[-1] - dif[-min(90, len(dif))]
        C.append(comp("Trajectoire des taux directeurs (BCE−Fed, 90 j)", ch / 0.25, 1.5,
                      "écart %+.2f pt, variation 90 j %+.2f pt" % (dif[-1], ch)))
        C.append(comp("Portage (carry) BCE−Fed", dif[-1] / 1.5, 0.8,
                      "taux directeur BCE %.2f%% vs Fed %.2f%%" % (info["ecb"], info["fed"])))
    except Exception:
        pass
    try:
        idx = [v for _, v in R["core"]]
        yoy_now, yoy_3m = idx[-1] / idx[-13] - 1, idx[-4] / idx[-16] - 1
        us_chg = (yoy_now - yoy_3m) * 100
        hi = [v for _, v in R["hicp"]]
        ez_chg = hi[-1] - hi[-4]
        C.append(comp("Dynamique d'inflation EUR vs US (3 mois)", (ez_chg - us_chg) / 0.25, 1.0,
                      "US core %.2f%% (%+.2f) · zone euro %.1f%% (%+.1f)" % (yoy_now * 100, us_chg, hi[-1], ez_chg)))
    except Exception:
        pass
    try:
        parts, txt = [], []
        if R.get("unrate"):
            u = [v for _, v in R["unrate"]]
            parts.append((u[-1] - u[-4]) / 0.2)
            txt.append("chômage US %.1f%% (%+.1f sur 3 mois)" % (u[-1], u[-1] - u[-4]))
        if R.get("pay"):
            p = [v for _, v in R["pay"]]
            d = [p[i] - p[i - 1] for i in range(1, len(p))]
            a3, a12 = mean(d[-3:]), mean(d[-12:])
            parts.append(-(a3 - a12) / 80)
            txt.append("payrolls 3 mois %+.0fk vs 12 mois %+.0fk" % (a3, a12))
        C.append(comp("Emploi US (un marché du travail plus faible = USD plus faible)", mean(parts), 1.0, " · ".join(txt)))
    except Exception:
        pass
    try:
        nf = [v for _, v in R["nfci"]]
        z = zchg(nf, 4)
        C.append(comp("Conditions financières US (NFCI)", -z[0], 0.6,
                      "%.2f (Δ4 sem. %+.2f, z=%+.1f) : resserrement = dollar refuge" % (nf[-1], z[1], z[0])))
    except Exception:
        pass
    cot = None
    try:
        net = R["cot"]["net"]
        d4 = [net[i] - net[i + 3] for i in range(0, len(net) - 3)]     # net[0] = plus récent
        z4 = (d4[0] - mean(d4)) / (std(d4) or 1)
        pct = 100 * sum(1 for v in net[:52] if v <= net[0]) / len(net[:52])
        adj = -1.0 if (pct > 90 and z4 > 0) else (1.0 if (pct < 10 and z4 < 0) else 0.0)
        cot = {"net": net[0], "pct": round(pct), "date": R["cot"]["date"]}
        C.append(comp("Positionnement COT des spéculateurs (euro)", z4 + adj, 1.2,
                      "net %+d contrats, %de percentile sur 52 sem., Δ4 sem. z=%+.1f%s" % (
                          net[0], round(pct), z4, " · positionnement bondé, risque de retournement" if adj else "")))
    except Exception:
        pass
    return {"comps": C, "info": info, "spread2": spread2, "cot": cot, "missing": miss}


def fund_dynamic(price, eur1d, dx1d, vix, spread2):
    """Composantes qui dépendent du prix et des marchés : DXY, VIX, modèle de valeur théorique."""
    C, model = [], None
    try:
        c = [x["c"] for x in dx1d]
        d20, d50 = c[-1] / mean(c[-20:]) - 1, c[-1] / mean(c[-50:]) - 1
        C.append(comp("Dollar (DXY) journalier", -(0.6 * d20 / 0.006 + 0.4 * d50 / 0.012), 1.5,
                      "%.2f : %+.2f%% vs moy. 20 j, %+.2f%% vs moy. 50 j" % (c[-1], d20 * 100, d50 * 100)))
    except Exception:
        pass
    try:
        v = [x["c"] for x in vix]
        ma = mean(v[-20:])
        z = ((v[-1] - ma) / ma) / 0.15 + (0.5 if v[-1] > 25 else 0)
        C.append(comp("Aversion au risque (VIX)", -z, 1.0, "VIX %.1f vs moyenne 20 j %.1f : risk-off = refuge USD" % (v[-1], ma)))
    except Exception:
        pass
    try:
        ds, sp = spread2
        spd = dict(zip(ds, sp))
        pts = [(spd[fxday(b["t"]).isoformat()], b["c"]) for b in eur1d if fxday(b["t"]).isoformat() in spd][-250:]
        xs, ys = [p[0] for p in pts], [p[1] for p in pts]
        mx, my = mean(xs), mean(ys)
        vx = sum((x - mx) ** 2 for x in xs)
        slope = sum((x - mx) * (y - my) for x, y in zip(xs, ys)) / vx
        icpt = my - slope * mx
        res = [y - (icpt + slope * x) for x, y in zip(xs, ys)]
        sres = std(res)
        r2 = 1 - sum(r * r for r in res) / sum((y - my) ** 2 for y in ys)
        fair = icpt + slope * sp[-1]
        zz = (price - fair) / sres
        model = {"fair": round(fair, 5), "z": round(zz, 2), "r2": round(r2, 2), "n": len(pts)}
        if r2 >= 0.2:
            C.append(comp("Valeur théorique (modèle écart 2 ans)", -zz * min(1, r2 / 0.5), 1.0,
                          "juste valeur %.5f, écart %+.1f σ, R²=%.2f sur %d jours : sous la valeur = rappel haussier" % (fair, zz, r2, len(pts))))
    except Exception:
        pass
    return C, model


def fund_compose(comps, info, cot, model, missing):
    full_w = 16.1
    tw = sum(c["w"] for c in comps)
    if not comps or tw <= 0:
        return {"score": 0, "bias": 0, "label": "indisponible", "comps": [], "coverage": 0, "breadth": 0, "info": info,
                "cot": cot, "model": model, "dominant": None, "missing": missing}
    raw = sum(c["w"] * c["score"] for c in comps) / (2 * tw)
    cov = tw / full_w
    score = round(100 * raw * min(1.0, cov / 0.7), 1)
    sg = 1 if score >= 0 else -1
    agree = sum(c["w"] for c in comps if c["score"] * sg > 0.15) / tw
    dom = max(comps, key=lambda c: abs(c["w"] * c["score"]))
    return {"score": score, "bias": sign(score, FUND_TH), "coverage": round(cov, 2), "breadth": round(agree, 2),
            "dominant": dom["label"], "comps": comps, "info": info, "cot": cot, "model": model, "missing": missing}


def calendar():
    r = _get("https://nfs.faireconomy.media/ff_calendar_thisweek.json").json()
    now = datetime.now(timezone.utc)
    out = []
    for e in r:
        if e.get("impact") not in ("High", "Medium") or e.get("country") not in ("USD", "EUR"):
            continue
        t = datetime.fromisoformat(e["date"]).astimezone(timezone.utc)
        m = int((t - now).total_seconds() / 60)
        if -30 <= m <= 36 * 60:
            out.append({"title": e["title"], "country": e["country"], "impact": e["impact"],
                        "time": t.strftime("%a %H:%M UTC"), "minutes": m,
                        "forecast": e.get("forecast") or "", "previous": e.get("previous") or ""})
    return sorted(out, key=lambda x: x["minutes"])


# ====================== 2. PRIMITIVES PRICE ACTION ======================
TFN = {"1M": "Mensuel", "1w": "Hebdo", "1d": "Daily", "4h": "H4", "1h": "H1", "15m": "M15", "5m": "M5", "1m": "M1"}
TFW = {"1M": 5.0, "1w": 4.0, "1d": 3.2, "4h": 2.6, "1h": 2.0, "15m": 1.2, "5m": 0.7, "1m": 0.4}
KW = {"OB": 1.0, "BRK": 0.9, "FVG": 0.8}


def atr_series(b, n=14):
    out, a, trs = [], 0.0, []
    for i, x in enumerate(b):
        tr = x["h"] - x["l"] if i == 0 else max(x["h"] - x["l"], abs(x["h"] - b[i - 1]["c"]), abs(x["l"] - b[i - 1]["c"]))
        trs.append(tr)
        a = sum(trs) / len(trs) if i < n else (a * (n - 1) + tr) / n
        out.append(a)
    return out


def ema(v, n):
    k, e, out = 2 / (n + 1), v[0], []
    for x in v:
        e = x * k + e * (1 - k)
        out.append(e)
    return out


def rsi_series(c, n=14):
    out, ag, al = [50.0] * len(c), 0.0, 0.0
    for i in range(1, len(c)):
        d = c[i] - c[i - 1]
        g, l = max(d, 0), max(-d, 0)
        if i <= n:
            ag, al = ag + g / n, al + l / n
        else:
            ag, al = (ag * (n - 1) + g) / n, (al * (n - 1) + l) / n
        out[i] = 100.0 if al == 0 else 100 - 100 / (1 + ag / al) if i >= n else 50.0
    return out


def structure(b, k=3):
    """Passe causale : pivots confirmés k bougies après, puis BOS / CHoCH sur clôture."""
    h, l, c = [x["h"] for x in b], [x["l"] for x in b], [x["c"] for x in b]
    st, lh, ll, hb, lb = 0, None, None, True, True
    ev, ph, pl = [], [], []
    for i in range(len(b)):
        p = i - k
        if p >= k:
            if h[p] >= max(h[p - k:p + k + 1]) and h[p] > max(h[p - k:p]):
                ph.append((p, h[p])); lh = h[p]; hb = False
            if l[p] <= min(l[p - k:p + k + 1]) and l[p] < min(l[p - k:p]):
                pl.append((p, l[p])); ll = l[p]; lb = False
        if not hb and c[i] > lh:
            ev.append((i, "CHoCH" if st == -1 else "BOS", 1, lh)); st = 1; hb = True
        elif not lb and c[i] < ll:
            ev.append((i, "CHoCH" if st == 1 else "BOS", -1, ll)); st = -1; lb = True
    return {"state": st, "ev": ev, "ph": ph, "pl": pl, "lh": lh, "ll": ll}


def find_fvg(b, at, look):
    N, out = len(b), []
    for i in range(max(2, N - look), N):
        a = at[i]
        x0, x2 = b[i - 2], b[i]
        if x2["l"] > x0["h"] and x2["l"] - x0["h"] >= 0.1 * a:
            d, lo, hi = 1, x0["h"], x2["l"]
            ext = min([x["l"] for x in b[i + 1:]], default=None)
            filled = ext is not None and ext <= lo
            fill = 0 if ext is None or ext >= hi else min(1, (hi - ext) / (hi - lo))
        elif x2["h"] < x0["l"] and x0["l"] - x2["h"] >= 0.1 * a:
            d, lo, hi = -1, x2["h"], x0["l"]
            ext = max([x["h"] for x in b[i + 1:]], default=None)
            filled = ext is not None and ext >= hi
            fill = 0 if ext is None or ext <= lo else min(1, (ext - lo) / (hi - lo))
        else:
            continue
        if not filled:
            body = abs(b[i - 1]["c"] - b[i - 1]["o"])
            out.append(dict(kind="FVG", dir=d, lo=lo, hi=hi, age=N - 1 - i, tested=fill > 0,
                            strong=body >= 0.8 * a, fill=round(fill, 2)))
    return out


def find_ob(b, at, look):
    """Order blocks (dernière bougie opposée avant un déplacement qui casse la structure) et breakers."""
    N, ob, brk = len(b), [], []
    for i in range(max(11, N - look), N):
        a = at[i]
        if abs(b[i]["c"] - b[i]["o"]) < 1.2 * a:
            continue
        j, bb = i - 1, b[i - 1]
        if b[i]["c"] > b[i]["o"] and bb["c"] < bb["o"] and b[i]["c"] > max(x["h"] for x in b[i - 10:i]):
            d, lo, hi = 1, bb["l"], max(bb["o"], bb["c"])
            broke = next((k for k in range(i + 1, N) if b[k]["c"] < lo), None)
        elif b[i]["c"] < b[i]["o"] and bb["c"] > bb["o"] and b[i]["c"] < min(x["l"] for x in b[i - 10:i]):
            d, lo, hi = -1, min(bb["o"], bb["c"]), bb["h"]
            broke = next((k for k in range(i + 1, N) if b[k]["c"] > hi), None)
        else:
            continue
        if broke is None:
            tested = any((x["l"] <= hi) if d == 1 else (x["h"] >= lo) for x in b[i + 1:])
            ob.append(dict(kind="OB", dir=d, lo=lo, hi=hi, age=N - 1 - j, tested=tested))
        else:                           # OB cassé -> breaker de polarité inverse, tant qu'il n'est pas re-cassé
            back = any((x["c"] > hi) if d == 1 else (x["c"] < lo) for x in b[broke + 1:])
            if not back:
                brk.append(dict(kind="BRK", dir=-d, lo=lo, hi=hi, age=N - 1 - broke, tested=False))
    return ob, brk


def pools(piv, h, l, tol, side):
    """Equal highs / equal lows non balayés (liquidité visible)."""
    out, used = [], set()
    pv = piv[-14:]
    for a in range(len(pv)):
        if a in used:
            continue
        grp = [a] + [x for x in range(a + 1, len(pv)) if abs(pv[x][1] - pv[a][1]) <= tol]
        if len(grp) < 2:
            continue
        used.update(grp)
        lvl = mean([pv[g][1] for g in grp])
        li = max(pv[g][0] for g in grp)
        rest = h[li + 1:] if side == "high" else l[li + 1:]
        swept = bool(rest) and ((max(rest) > lvl + 0.4 * tol) if side == "high" else (min(rest) < lvl - 0.4 * tol))
        if not swept:
            out.append({"price": lvl, "side": side, "n": len(grp)})
    return out


def pattern(b):
    if len(b) < 2:
        return 0, "—"
    p, q = b[-2], b[-1]
    rng = q["h"] - q["l"]
    if rng <= 0:
        return 0, "—"
    body = abs(q["c"] - q["o"])
    lw, uw = min(q["o"], q["c"]) - q["l"], q["h"] - max(q["o"], q["c"])
    if p["c"] < p["o"] and q["c"] > q["o"] and q["c"] >= p["o"] and q["o"] <= p["c"]:
        return 1, "engulfing haussier"
    if p["c"] > p["o"] and q["c"] < q["o"] and q["c"] <= p["o"] and q["o"] >= p["c"]:
        return -1, "engulfing baissier"
    if lw >= 2 * max(body, 0.05 * rng) and lw >= 0.6 * rng:
        return 1, "pin bar haussier"
    if uw >= 2 * max(body, 0.05 * rng) and uw >= 0.6 * rng:
        return -1, "pin bar baissier"
    return 0, "—"


def divergence(b, st, rs):
    """Divergence RSI entre les deux derniers pivots (récents)."""
    N = len(b)
    pl, ph = st["pl"], st["ph"]
    if len(pl) >= 2 and N - pl[-1][0] <= 40 and pl[-1][1] < pl[-2][1] and rs[pl[-1][0]] > rs[pl[-2][0]] + 2:
        return 1
    if len(ph) >= 2 and N - ph[-1][0] <= 40 and ph[-1][1] > ph[-2][1] and rs[ph[-1][0]] < rs[ph[-2][0]] - 2:
        return -1
    return 0


def tf_analyze(b, tf):
    if len(b) < 30:
        return None
    c = [x["c"] for x in b]
    at = atr_series(b)
    k = 2 if tf in ("1w", "1M") else (2 if tf == "1m" else 3)
    st = structure(b, k)
    e20, e50 = ema(c, 20)[-1], ema(c, 50)[-1]
    e200 = ema(c, 200)[-1] if len(c) >= 120 else None
    price, a, N = c[-1], at[-1], len(b)
    s = st["state"]
    eb = 1 if e20 > e50 and price > e50 else (-1 if e20 < e50 and price < e50 else 0)
    bias = float(s) if (s and s == eb) else (s * 0.5 if s and eb == 0 else (eb * 0.5 if s == 0 else 0.0))
    look = {"1M": 120, "1w": 200, "1d": 150, "4h": 120}.get(tf, 80)
    ob, brk = find_ob(b, at, look)
    rs = rsi_series(c)
    h, l = [x["h"] for x in b], [x["l"] for x in b]
    ph, pl = st["ph"], st["pl"]
    pos = ote = None
    if ph and pl:
        H, L = ph[-1][1], pl[-1][1]
        if H > L:
            pos = (price - L) / (H - L)
            if pl[-1][0] < ph[-1][0]:      # jambe haussière -> zone OTE de repli (62-79 %)
                ote = dict(dir=1, lo=H - 0.79 * (H - L), hi=H - 0.62 * (H - L))
            else:
                ote = dict(dir=-1, lo=L + 0.62 * (H - L), hi=L + 0.79 * (H - L))
    ev = st["ev"][-1] if st["ev"] else None
    return {"tf": tf, "price": price, "atr": a, "state": s, "bias": bias, "pos": pos, "ote": ote, "rsi": rs[-1],
            "ev": {"kind": ev[1], "dir": ev[2], "level": ev[3], "ago": N - 1 - ev[0], "i": ev[0]} if ev else None,
            "fvg": find_fvg(b, at, look), "ob": ob, "brk": brk, "div": divergence(b, st, rs),
            "pools": pools(ph, h, l, 0.2 * a, "high") + pools(pl, h, l, 0.2 * a, "low"),
            "swings": {"high": [p for _, p in ph[-2:]], "low": [p for _, p in pl[-2:]]},
            "pat": pattern(b), "e200": e200, "st": st}


# ====================== 3. VOLUME : PROFIL, VWAP, POC / VAH / VAL ======================
def vprofile(bars, step):
    bars = [x for x in bars if x["v"] > 0]
    if len(bars) < 8:
        return None
    lo, hi = min(x["l"] for x in bars), max(x["h"] for x in bars)
    n = int((hi - lo) / step) + 1
    prof = [0.0] * n
    for x in bars:
        i0, i1 = int((x["l"] - lo) / step), int((x["h"] - lo) / step)
        sh = x["v"] / (i1 - i0 + 1)
        for i in range(i0, i1 + 1):
            prof[i] += sh
    tot = sum(prof)
    pi = max(range(n), key=prof.__getitem__)
    a = bb = pi
    cov = prof[pi]
    while cov < 0.7 * tot and (a > 0 or bb < n - 1):
        lv = prof[a - 1] if a > 0 else -1
        rv = prof[bb + 1] if bb < n - 1 else -1
        if rv >= lv:
            bb += 1; cov += prof[bb]
        else:
            a -= 1; cov += prof[a]
    nz = [v for v in prof if v > 0]
    avg = mean(nz) if nz else 0
    hvn = [lo + (i + 0.5) * step for i in range(1, n - 1)
           if prof[i] >= prof[i - 1] and prof[i] >= prof[i + 1] and prof[i] >= 1.35 * avg and i != pi]
    lvn = [lo + (i + 0.5) * step for i in range(1, n - 1)
           if prof[i] <= prof[i - 1] and prof[i] <= prof[i + 1] and 0 < prof[i] <= 0.45 * avg]
    return {"poc": lo + (pi + 0.5) * step, "vah": lo + (bb + 1) * step, "val": lo + a * step,
            "hvn": hvn[:4], "lvn": lvn[:4]}


def vwap(bars, since=None):
    bs = [x for x in bars if x["v"] > 0 and (since is None or x["t"] >= since)]
    if len(bs) < 6:
        return None
    tp = [(x["h"] + x["l"] + x["c"]) / 3 for x in bs]
    tv = sum(x["v"] for x in bs)
    vw = sum(p * x["v"] for p, x in zip(tp, bs)) / tv
    sd = (sum(x["v"] * (p - vw) ** 2 for p, x in zip(tp, bs)) / tv) ** 0.5
    return {"vwap": vw, "sd": sd}


def spot_basis(s15, f15):
    d = {x["t"]: x["c"] for x in f15}
    diffs = [x["c"] - d[x["t"]] for x in s15[-40:] if x["t"] in d]
    if len(diffs) >= 4:
        diffs.sort()
        return diffs[len(diffs) // 2]
    return s15[-1]["c"] - f15[-1]["c"]


def build_profiles(f15, off, now_t):
    """Profils de volume réels (futures 6E CME, 15 min) ramenés en prix spot."""
    if not f15 or len(f15) < 40:
        return None
    days = {}
    for x in f15:
        days.setdefault(fxday(x["t"]), []).append(x)
    keys = sorted(days)
    today = fxday(now_t)
    cur = days.get(today, [])
    prevk = [k for k in keys if k < today]
    prev = days[prevk[-1]] if prevk else []
    wk = fxweek(now_t)
    week = [x for x in f15 if fxweek(x["t"]) == wk]
    last5 = [x for k in keys[-5:] for x in days[k]]
    sh = lambda p: None if p is None else p + off
    P = {}
    for name, bars, step in (("day", cur, 0.0002), ("prev", prev, 0.0002), ("week", week, 0.0004), ("comp", last5, 0.0004)):
        vp = vprofile(bars, step)
        P[name] = None if not vp else {"poc": sh(vp["poc"]), "vah": sh(vp["vah"]), "val": sh(vp["val"]),
                                       "hvn": [sh(p) for p in vp["hvn"]], "lvn": [sh(p) for p in vp["lvn"]]}
    day_start = min([x["t"] for x in cur], default=None)
    wv, ww = vwap(cur), vwap(week)
    P["vwap"] = None if not wv else {"v": sh(wv["vwap"]), "sd": wv["sd"]}
    P["wvwap"] = None if not ww else {"v": sh(ww["vwap"]), "sd": ww["sd"]}
    naked = []
    for k in keys[-6:-1]:
        vp = vprofile(days[k], 0.0002)
        if not vp:
            continue
        end = max(x["t"] for x in days[k])
        later = [x for x in f15 if x["t"] > end]
        if later and not any(x["l"] <= vp["poc"] <= x["h"] for x in later):
            naked.append(sh(vp["poc"]))
    P["naked"] = naked[-3:]
    P["_f15"] = f15
    P["_off"] = off
    return P


def anchored_vwap(P, t0):
    if not P or t0 is None:
        return None
    r = vwap(P["_f15"], t0)
    return None if not r else {"v": r["vwap"] + P["_off"], "sd": r["sd"]}


def close_delta(bars):
    """Delta estimé : volume pondéré par la position de la clôture dans la bougie (proxy gratuit)."""
    tot = sum(x["v"] for x in bars) or 1
    return sum(x["v"] * ((2 * x["c"] - x["h"] - x["l"]) / (x["h"] - x["l"]) if x["h"] > x["l"] else 0) for x in bars) / tot


# ====================== 4. NIVEAUX DE RÉFÉRENCE : JOURS, SEMAINES, MOIS, SESSIONS ======================
def key_levels(s1h, s1d, s5, now_t):
    K = {}
    days, weeks, months = {}, {}, {}
    for x in s1h:
        days.setdefault(fxday(x["t"]), []).append(x)
        weeks.setdefault(fxweek(x["t"]), []).append(x)
    for x in s1d:
        d = fxday(x["t"])
        months.setdefault((d.year, d.month), []).append(x)
    today, wk = fxday(now_t), fxweek(now_t)
    td = fxday(now_t)
    pk = [k for k in sorted(days) if k < today]
    if pk and len(days[pk[-1]]) >= 8:
        K["PDH"], K["PDL"] = max(x["h"] for x in days[pk[-1]]), min(x["l"] for x in days[pk[-1]])
    if today in days:
        K["DO"] = days[today][0]["o"]
    wp = [k for k in sorted(weeks) if k < wk]
    if wp and len(weeks[wp[-1]]) >= 20:
        K["PWH"], K["PWL"] = max(x["h"] for x in weeks[wp[-1]]), min(x["l"] for x in weeks[wp[-1]])
    if wk in weeks:
        K["WO"] = weeks[wk][0]["o"]
    mk, mp = (td.year, td.month), [k for k in sorted(months) if k < (td.year, td.month)]
    if mp and len(months[mp[-1]]) >= 10:
        K["PMH"], K["PML"] = max(x["h"] for x in months[mp[-1]]), min(x["l"] for x in months[mp[-1]])
    if mk in months:
        K["MO"] = months[mk][0]["o"]
    ses = {}
    for x in s5:
        ses.setdefault((fxday(x["t"]), session(x["t"])), []).append(x)
    rng = lambda k: (max(x["h"] for x in ses[k]), min(x["l"] for x in ses[k])) if len(ses.get(k, [])) >= 6 else None
    cs = session(now_t)
    a = rng((today, "asia"))
    if a:
        K["ASH"], K["ASL"], K["asia_done"] = a[0], a[1], cs != "asia"
    pd_ = [d for d in sorted({k[0] for k in ses}) if d < today]
    if pd_:
        l_, n_ = rng((pd_[-1], "london")), rng((pd_[-1], "ny"))
        if l_:
            K["LOH"], K["LOL"] = l_
        if n_:
            K["NYH"], K["NYL"] = n_
    return K


def detect_sweeps(b, levels, look, tf):
    """Mèche au-delà d'un niveau de liquidité + clôture de réintégration, sans nouvelle clôture au-delà ensuite."""
    N, best = len(b), {1: None, -1: None}
    for i in range(max(1, N - look), N):
        for lv in levels:
            p = lv["price"]
            if lv["side"] == "low" and b[i]["l"] < p < b[i]["c"] and min(x["c"] for x in b[i:]) > p:
                d, ext = 1, min(x["l"] for x in b[i:])
            elif lv["side"] == "high" and b[i]["h"] > p > b[i]["c"] and max(x["c"] for x in b[i:]) < p:
                d, ext = -1, max(x["h"] for x in b[i:])
            else:
                continue
            c = dict(dir=d, level=p, label=lv["label"], w=lv["w"], ext=ext, ago=N - 1 - i, tf=tf)
            o = best[d]
            if o is None or (c["ago"], -c["w"]) < (o["ago"], -o["w"]):
                best[d] = c
    return best


def smt(eu, other, d, inverse, look=48, rec=12):
    """Divergence SMT : EUR/USD fait un nouvel extrême que le corrélé (GBP) ou l'inverse (DXY) ne confirme pas."""
    if len(eu) < look or len(other) < look:
        return False
    R, Pv = eu[-rec:], eu[-look:-rec]
    t0 = R[0]["t"]
    oR = [x for x in other if x["t"] >= t0]
    oP = [x for x in other if x["t"] < t0][-(look - rec):]
    if len(oR) < 3 or len(oP) < 10:
        return False
    if d == 1:
        eu_ext = min(x["l"] for x in R) < min(x["l"] for x in Pv)
        ot = (max(x["h"] for x in oR) > max(x["h"] for x in oP)) if inverse else (min(x["l"] for x in oR) < min(x["l"] for x in oP))
    else:
        eu_ext = max(x["h"] for x in R) > max(x["h"] for x in Pv)
        ot = (min(x["l"] for x in oR) < min(x["l"] for x in oP)) if inverse else (max(x["h"] for x in oR) > max(x["h"] for x in oP))
    return eu_ext and not ot


# ====================== 5. ÉLÉMENTS DE CONFLUENCE & CLUSTERS ======================
FAM_N = {"zone": "zone", "liq": "liquidité", "vp": "profil de volume", "vwap": "VWAP", "ref": "niveau de référence", "fib": "OTE"}


def build_elements(T, K, P, price, a15):
    E, win = [], max(0.015, 30 * a15)
    pad = max(0.12 * a15, 0.00008)

    def add(lo, hi, d, w, label, fam, tf=None):
        mid = (lo + hi) / 2
        if abs(mid - price) <= win:
            E.append(dict(lo=lo, hi=hi, mid=mid, dir=d, w=w, label=label, fam=fam, tf=tf))

    def lvl(p, d, w, label, fam):
        if p is not None:
            add(p - pad, p + pad, d, w, label, fam)

    for tf, t in T.items():
        if not t:
            continue
        for z in t["fvg"] + t["ob"] + t["brk"]:
            w = TFW[tf] * KW[z["kind"]] * (1.2 if not z["tested"] else 1.0) * (1.15 if z.get("strong") else 1.0)
            nm = {"OB": "Order block", "FVG": "Imbalance", "BRK": "Breaker"}[z["kind"]]
            add(z["lo"], z["hi"], z["dir"], w, "%s %s %s" % (nm, TFN[tf], "↑" if z["dir"] > 0 else "↓"), "zone", tf)
        if tf in ("15m", "1h", "4h", "1d", "1w"):
            pw = {"15m": 1.6, "1h": 2.2, "4h": 2.6, "1d": 3.0, "1w": 3.4}[tf]
            for p in t["pools"]:
                lvl(p["price"], 1 if p["side"] == "low" else -1, pw, "EQ%s %s" % ("L" if p["side"] == "low" else "H", TFN[tf]), "liq")
        if tf in ("1h", "4h", "1d", "1w"):
            sw = {"1h": 1.4, "4h": 1.8, "1d": 2.2, "1w": 2.8}[tf]
            for p in t["swings"]["low"]:
                lvl(p, 1, sw, "Swing low %s" % TFN[tf], "liq")
            for p in t["swings"]["high"]:
                lvl(p, -1, sw, "Swing high %s" % TFN[tf], "liq")
        if tf in ("1h", "4h", "1d") and t["ote"]:
            o = t["ote"]
            add(o["lo"], o["hi"], o["dir"], {"1h": 1.5, "4h": 2.0, "1d": 2.5}[tf], "OTE 62-79%% %s" % TFN[tf], "fib", tf)
    for key, d, w, name in (("PDH", -1, 2.2, "Haut de la veille"), ("PDL", 1, 2.2, "Bas de la veille"),
                            ("PWH", -1, 3.0, "Haut de la semaine passée"), ("PWL", 1, 3.0, "Bas de la semaine passée"),
                            ("PMH", -1, 3.5, "Haut du mois passé"), ("PML", 1, 3.5, "Bas du mois passé"),
                            ("ASH", -1, 2.2, "Haut Asie"), ("ASL", 1, 2.2, "Bas Asie"),
                            ("LOH", -1, 1.8, "Haut Londres (veille)"), ("LOL", 1, 1.8, "Bas Londres (veille)"),
                            ("NYH", -1, 1.4, "Haut New York (veille)"), ("NYL", 1, 1.4, "Bas New York (veille)")):
        lvl(K.get(key), d, w, name, "liq")
    for key, w, name in (("DO", 1.2, "Ouverture du jour"), ("WO", 1.6, "Ouverture de la semaine"), ("MO", 1.8, "Ouverture du mois")):
        lvl(K.get(key), 0, w, name, "ref")
    base = round(price / 0.005) * 0.005
    for k in range(-3, 4):
        p = base + k * 0.005
        big = abs(p / 0.01 - round(p / 0.01)) < 1e-6
        lvl(p, 0, 1.0 if big else 0.7, "Big figure %.4f" % p, "ref")
    if P:
        for nm, lab, wts in (("day", "jour", (1.8, 1.6, 1.6)), ("prev", "veille", (2.4, 2.2, 2.2)),
                             ("week", "semaine", (2.6, 2.4, 2.4)), ("comp", "5 jours", (2.0, 1.6, 1.6))):
            v = P.get(nm)
            if v:
                lvl(v["poc"], 0, wts[0], "POC %s" % lab, "vp")
                lvl(v["vah"], -1, wts[1], "VAH %s" % lab, "vp")
                lvl(v["val"], 1, wts[2], "VAL %s" % lab, "vp")
        if P.get("comp"):
            for p in P["comp"]["hvn"]:
                lvl(p, 0, 1.2, "HVN 5 jours", "vp")
        for p in P.get("naked", []):
            lvl(p, 0, 2.6, "POC naked", "vp")
        for key, lab, w0 in (("vwap", "VWAP jour", 1.8), ("wvwap", "VWAP semaine", 1.8), ("avwap", "AVWAP (dernier CHoCH H1)", 1.8)):
            v = P.get(key)
            if v:
                lvl(v["v"], 0, w0, lab, "vwap")
                for m, wm in ((1, 1.2), (-1, 1.2), (2, 2.0), (-2, 2.0)):
                    if key != "avwap" or abs(m) == 1:
                        lvl(v["v"] + m * v["sd"], 1 if m < 0 else -1, wm, "%s %+dσ" % (lab, m), "vwap")
    return E


def clusters(E, d, price, a15):
    u, tol = max(a15, 0.0003), 0.25 * a15
    maxd = max(0.006, 10 * a15)
    cand = [e for e in E if e["dir"] in (d, 0) and abs(e["mid"] - price) <= maxd
            and ((e["mid"] <= price + tol) if d == 1 else (e["mid"] >= price - tol))]
    cand.sort(key=lambda e: e["mid"])
    out, cur = [], None
    for e in cand:
        if cur and e["lo"] - cur["hi"] <= 0.3 * u and max(cur["hi"], e["hi"]) - cur["lo"] <= 2.2 * u:
            cur["m"].append(e)
            cur["hi"] = max(cur["hi"], e["hi"])
        else:
            if cur:
                out.append(cur)
            cur = dict(lo=e["lo"], hi=e["hi"], m=[e])
    if cur:
        out.append(cur)
    for c in out:
        ws = sorted((m["w"] for m in c["m"]), reverse=True)
        fams = {m["fam"] for m in c["m"]}
        c["score"] = round(ws[0] + 0.7 * (ws[1] if len(ws) > 1 else 0) + 0.5 * (ws[2] if len(ws) > 2 else 0)
                           + 0.35 * sum(ws[3:]) + (len(fams) - 1) * 1.0, 2)
        c["fams"] = sorted(FAM_N[f] for f in fams)
        cnt, order = {}, []
        for m in sorted(c["m"], key=lambda m: -m["w"]):
            if m["label"] not in cnt:
                order.append(m["label"])
            cnt[m["label"]] = cnt.get(m["label"], 0) + 1
        c["labels"] = [l + (" ×%d" % cnt[l] if cnt[l] > 1 else "") for l in order][:6]
        c["inside"] = c["lo"] - tol <= price <= c["hi"] + tol
        c["dist"] = 0.0 if c["inside"] else (price - c["hi"] if d == 1 else c["lo"] - price)
    return out


def targets(E, d, price, a15):
    tol = 0.25 * a15
    T = [(e["lo"] if d == 1 else e["hi"], e["label"], e["w"]) for e in E
         if e["dir"] in (-d, 0) and e["w"] >= 1.0 and ((e["lo"] > price + tol) if d == 1 else (e["hi"] < price - tol))]
    T.sort(key=lambda x: abs(x[0] - price))
    return T


def grade(score):
    return "A+" if score >= 16 else ("A" if score >= 12.5 else ("B" if score >= MIN_SETUP else "C"))


def context(d, T, sw, smtf, fscore, kz, flow24):
    b, why = 0.0, []
    for tf, w in (("1M", 0.5), ("1w", 1.0), ("1d", 1.5), ("4h", 1.5), ("1h", 1.5)):
        t = T.get(tf)
        if t and t["state"]:
            ok = t["state"] == d
            b += w if ok else -w
            why.append("%s%g structure %s %s" % ("+" if ok else "−", w, TFN[tf], "alignée" if ok else "contre"))
    for tf in ("4h", "1d"):
        t = T.get(tf)
        if t and t["pos"] is not None:
            good = (d == 1 and t["pos"] <= 0.5) or (d == -1 and t["pos"] >= 0.5)
            b += 0.75 if good else -0.75
            why.append("%s0.75 %s en %s du range %s" % ("+" if good else "−", "prix", ("discount" if t["pos"] <= 0.5 else "premium"), TFN[tf]))
    s = sw.get(d)
    if s:
        tfm = {"5m": 0.8, "15m": 1.0, "1h": 1.3}[s["tf"]]
        v = round((1.5 + min(2.0, s["w"] / 1.8)) * tfm, 1)
        judas = "Asie" in s["label"] and kz
        if judas:
            v += 1.0
        b += v
        why.append("+%g %s : %s (%s, il y a %d bougies)" % (v, "Judas swing (AMD)" if judas else "Balayage de liquidité", s["label"], TFN[s["tf"]], s["ago"]))
    if smtf.get(d):
        b += 1.5
        why.append("+1.5 divergence SMT (EUR/USD vs GBP/DXY)")
    for tf, v in (("1h", 1.2), ("15m", 1.2), ("5m", 0.6)):
        t = T.get(tf)
        if t and t["div"] == d:
            b += v
            why.append("+%g divergence RSI %s" % (v, TFN[tf]))
    if kz:
        b += 1.0
        why.append("+1 killzone %s" % kz)
    if fscore:
        v = round(fscore / 100 * 2 * d, 1)
        b += v
        why.append("%s%.1f fondamentaux (score %+.0f)" % ("+" if v >= 0 else "−", abs(v), fscore))
    if flow24 is not None and flow24 * d > 0.05:
        b += 0.6
        why.append("+0.6 delta 6E aligné")
    return round(b, 2), why


# ====================== 6. RÉACTION, DXY, PLAN ======================
def reaction(d, cl, s5, s1, f5, off, a15, T):
    tol = 0.25 * a15
    near = (lambda x: x["l"] <= cl["hi"] + tol) if d == 1 else (lambda x: x["h"] >= cl["lo"] - tol)
    price = s5[-1]["c"]
    away = (price - cl["hi"]) if d == 1 else (cl["lo"] - price)
    touched = any(near(x) for x in s5[-8:]) and away <= 2.0 * a15
    sw = lambda x: (x["l"] < cl["lo"] and x["c"] > cl["lo"]) if d == 1 else (x["h"] > cl["hi"] and x["c"] < cl["hi"])
    c1 = any(sw(x) for x in s5[-12:]) or any(sw(x) for x in s1[-40:])
    c2 = False
    for j in range(3):
        sub = s5[:len(s5) - j]
        p = pattern(sub)
        if p[0] == d and near(sub[-1]):
            c2 = True
    ev = structure(s1[-150:], 2)["ev"] if len(s1) > 40 else []
    c3 = bool(ev) and ev[-1][2] == d and (len(s1[-150:]) - 1 - ev[-1][0]) <= 25
    has_f = bool(f5) and len(f5) >= 40
    c4 = c5 = False
    if has_f:
        base = mean([x["v"] for x in f5[-38:-8]]) or 1
        for x in f5[-8:]:
            rng = x["h"] - x["l"]
            zone_hit = (x["l"] + off <= cl["hi"] + tol) if d == 1 else (x["h"] + off >= cl["lo"] - tol)
            if rng > 0 and x["v"] >= 1.5 * base and abs(x["c"] - x["o"]) / rng <= 0.4 and zone_hit:
                c4 = True
        c5 = close_delta(f5[-6:]) * d > 0.1
    c6 = any((T.get(tf) or {}).get("div") == d for tf in ("5m", "15m"))
    checks = [
        {"label": "Balayage + reprise de la zone", "ok": c1, "av": True},
        {"label": "Rejet : pin bar / engulfing sur la zone", "ok": c2, "av": True},
        {"label": "CHoCH / BOS M1 dans le sens du trade", "ok": c3, "av": True},
        {"label": "Absorption : gros volume 6E, petit corps", "ok": c4, "av": has_f},
        {"label": "Delta acheteur/vendeur (proxy 6E)", "ok": c5, "av": has_f},
        {"label": "Divergence RSI M5/M15", "ok": c6, "av": True}]
    n_ok = sum(1 for c in checks if c["ok"])
    need = 3 if has_f else 2
    return {"touched": touched, "checks": checks, "n_ok": n_ok, "n_av": sum(1 for c in checks if c["av"]), "need": need,
            "confirmed": touched and n_ok >= need and (c1 or c2 or c3)}


def dxy_dir(b):
    if not b or len(b) < 55:
        return 0
    c = [x["c"] for x in b]
    e20, e50 = ema(c, 20)[-1], ema(c, 50)[-1]
    return 1 if c[-1] > e20 > e50 else (-1 if c[-1] < e20 < e50 else 0)


def dxy_state(d, D):
    d15, d1h = dxy_dir(D.get("dx15")), dxy_dir(D.get("dx1h"))
    if not D.get("dx15") and not D.get("dx1h"):
        return "NEUTRE", "DXY intraday indisponible"
    v = 0.6 * (-d1h) + 0.4 * (-d15)
    st = "CONFIRME" if v * d >= 0.6 else ("CONTRE" if v * d <= -0.6 else "NEUTRE")
    nm = {1: "haussier", -1: "baissier", 0: "neutre"}
    return st, "DXY H1 %s · M15 %s (le DXY doit aller à l'inverse du trade)" % (nm[d1h], nm[d15])


def make_plan(d, cl, price, touched, s5, sw, tg, a15, meta):
    ext = None
    if touched:
        e5 = min(x["l"] for x in s5[-6:]) if d == 1 else max(x["h"] for x in s5[-6:])
        ref = min(cl["lo"], e5) if d == 1 else max(cl["hi"], e5)
        if sw:
            ref = min(ref, sw["ext"]) if d == 1 else max(ref, sw["ext"])
        entry, mode = price, "IMMÉDIATE : réaction sur la zone"
    else:
        ref = cl["lo"] if d == 1 else cl["hi"]
        entry, mode = (cl["lo"] + cl["hi"]) / 2, "LIMITE (prévisionnel) : attendre la zone"
    sl = ref - d * 0.3 * a15
    risk = max(abs(entry - sl), 0.6 * a15, 4 * PIP)
    sl = entry - d * risk
    if risk > (3.0 if touched else 3.5) * a15:
        return None
    pick = [(p, k) for p, k, w in tg if (p - entry) * d > 0 and (p - entry) * d / risk >= MIN_RR]
    if not pick:
        return None
    tp, tk = pick[0]
    tp2 = pick[1] if len(pick) > 1 else None
    plan = dict(dir=d, mode=mode, entry=entry, sl=sl, tp=tp, tp_kind=tk, rr=round((tp - entry) * d / risk, 2),
                tp2=tp2[0] if tp2 else None, tp2_kind=tp2[1] if tp2 else None,
                rr2=round((tp2[0] - entry) * d / risk, 2) if tp2 else None,
                pips_sl=round(risk / PIP, 1), pips_tp=round(abs(tp - entry) / PIP, 1), t=time.time(), be=False)
    plan.update(meta)
    return plan


# ====================== 7. MOTEUR ======================
TW = {"1M": 0.5, "1w": 1.0, "1d": 2.0, "4h": 2.0, "1h": 1.5, "15m": 1.0}


FUND_JOB = {"running": False}


def _fund_worker():
    try:
        cached("fund", 600, fund_static)
    except Exception:
        pass
    FUND_JOB["running"] = False


def fund_get():
    """Les données macro (FRED, BCE, CFTC) se chargent en arrière-plan : la page ne bloque jamais dessus."""
    with CLOCK:
        e = CACHE.get("fund")
    if e and time.time() - e[0] < 600:
        return e[1], False
    if not FUND_JOB["running"]:
        FUND_JOB["running"] = True
        threading.Thread(target=_fund_worker, daemon=True).start()
    return (e[1] if e else None), True


def flow_items(f5):
    items = []
    if f5 and len(f5) >= 60:
        d24, d4 = close_delta(f5[-288:]), close_delta(f5[-48:])
        rv = mean([x["v"] for x in f5[-12:]]) / (mean([x["v"] for x in f5[-100:]]) or 1)
        items.append({"label": "Delta estimé futures 6E (24 h) : %+.1f%%" % (d24 * 100), "score": 1 if d24 > 0.04 else (-1 if d24 < -0.04 else 0)})
        items.append({"label": "Delta estimé 6E (4 h) : %+.1f%%" % (d4 * 100), "score": 1 if d4 > 0.05 else (-1 if d4 < -0.05 else 0)})
        items.append({"label": "Volume relatif 6E (1 h vs moyenne) : x%.2f" % rv, "score": 0})
        return items, d24
    return [{"label": "Futures 6E indisponibles (marché fermé ?)", "score": 0}], None


def analyse():
    now_t = time.time()
    D, err = load_all()
    for k in ("s_5m", "s_15m", "s_1h", "s_1d"):
        if k not in D:
            raise RuntimeError("Données EUR/USD incomplètes (%s). Source publique indisponible, réessaie dans une minute." % k[2:])
    S = {tf: D.get("s_" + tf) for tf in TFS}
    S["4h"] = resample(S["1h"])
    s5, s1 = S["5m"], S.get("1m") or S["5m"]
    price = s1[-1]["c"]
    T = {}
    for tf in ("1M", "1w", "1d", "4h", "1h", "15m", "5m", "1m"):
        if S.get(tf):
            try:
                T[tf] = tf_analyze(S[tf], tf)
            except Exception as e:
                err["tf_" + tf] = str(e)[:60]
    a15, a1h = T["15m"]["atr"], T["1h"]["atr"]

    # volume réel (futures 6E) -> VWAP, POC, VAH, VAL ramenés en spot
    f15, f5 = D.get("f15"), D.get("f5")
    P = None
    try:
        off = spot_basis(S["15m"], f15) if f15 else 0.0
        P = build_profiles(f15, off, now_t)
        if P and T.get("1h") and T["1h"]["ev"]:
            t0 = S["1h"][T["1h"]["ev"]["i"]]["t"]
            P["avwap"] = anchored_vwap(P, t0)
    except Exception as e:
        err["profil"] = str(e)[:60]
    off = (P or {}).get("_off", 0.0)
    K = key_levels(S["1h"], S["1d"], s5, now_t)
    E = build_elements(T, K, P, price, a15)

    # sweeps, SMT
    levs = [dict(price=e["mid"], side="low" if e["dir"] > 0 else "high", label=e["label"], w=e["w"]) for e in E if e["fam"] == "liq"]
    sw = {1: None, -1: None}
    for tf, look in (("5m", 18), ("15m", 6), ("1h", 3)):
        if S.get(tf):
            r = detect_sweeps(S[tf], levs, look, tf)
            for d in (1, -1):
                c = r[d]
                if c and (sw[d] is None or c["w"] * {"5m": 0.8, "15m": 1.0, "1h": 1.3}[tf] - 0.1 * c["ago"] > sw[d]["_k"]):
                    c["_k"] = c["w"] * {"5m": 0.8, "15m": 1.0, "1h": 1.3}[tf] - 0.1 * c["ago"]
                    sw[d] = c
    smtf = {}
    for d in (1, -1):
        smtf[d] = bool((D.get("gb15") and smt(S["15m"], D["gb15"], d, False)) or (D.get("dx15") and smt(S["15m"], D["dx15"], d, True)))

    # fondamental
    fund = {"score": 0, "bias": 0, "comps": [], "coverage": 0, "breadth": 0, "info": {}, "cot": None, "model": None, "dominant": None, "missing": [], "loading": False}
    try:
        st, loading = fund_get()
        fund["loading"] = loading and st is None
        if st:
            dyn, model = fund_dynamic(price, S["1d"], D.get("dx1d"), D.get("vix"), st["spread2"]) if st["spread2"] else ([], None)
            fund = fund_compose(st["comps"] + dyn, st["info"], st["cot"], model, st["missing"])
            fund["loading"] = False
    except Exception as e:
        err["fondamental"] = str(e)[:80]
    try:
        news = cached("cal", 900, calendar)
    except Exception:
        news = []
    news_risk = any(n["impact"] == "High" and -30 <= n["minutes"] <= 90 for n in news)

    # biais technique multi-timeframe
    avail = [tf for tf in TW if T.get(tf)]
    tech = round(100 * sum(TW[tf] * T[tf]["bias"] for tf in avail) / sum(TW[tf] for tf in avail), 1)
    fb, td = fund["bias"], sign(tech, TECH_TH)
    if fb and td and fb != td:
        dirn, mode = 0, "CONFLIT"
    elif fb and td:
        dirn, mode = fb, "ALIGNÉ"
    elif fb:
        dirn, mode = fb, "FONDAMENTAL SEUL"
    elif td:
        dirn, mode = td, "TECHNIQUE SEULE"
    else:
        dirn, mode = 0, "NEUTRE"
    need_score = MIN_SETUP + (0 if mode == "ALIGNÉ" else 2)

    flow, d24 = flow_items(f5)
    ofb = sign(sum(i["score"] for i in flow), 1)
    kz = killzone(now_t)
    closed = market_closed(now_t)

    # setups des deux côtés
    setups, best = [], {}
    for d in (1, -1):
        dstate, dtxt = dxy_state(d, D)
        bonus, why = context(d, T, sw, smtf, fund["score"], kz, d24)
        if dstate == "CONFIRME":
            bonus += 1.5
            why.append("+1.5 DXY confirme")
        tg = targets(E, d, price, a15)
        cls = clusters(E, d, price, a15)
        for c in cls:
            c["total"] = round(c["score"] + bonus, 1)
        cls.sort(key=lambda c: -(c["total"] - c["dist"] / PIP / 25))
        for rank, c in enumerate(cls[:2]):
            rx = reaction(d, c, s5, s1, f5, off, a15, T)
            meta = dict(level=(c["lo"] + c["hi"]) / 2, labels=c["labels"], grade=grade(c["total"]), score=c["total"], mode_fb=mode)
            plan = make_plan(d, c, price, rx["touched"], s5, sw.get(d), tg, a15, meta)
            allowed = (d == dirn) and c["total"] >= need_score
            setups.append(dict(dir=d, grade=grade(c["total"]), score=c["total"], lo=c["lo"], hi=c["hi"], labels=c["labels"],
                               fams=c["fams"], inside=c["inside"], dist_pips=round(c["dist"] / PIP, 1), why=why, plan=plan,
                               rx=rx, allowed=allowed, dxy=dstate, dxy_txt=dtxt))
    setups.sort(key=lambda s: (-int(s["allowed"]), -(s["score"] - s["dist_pips"] / 25)))
    cand = next((s for s in setups if s["allowed"]), None)

    # machine d'état
    plan, status, msg, shown = STATE["plan"], "NEUTRE", "", None
    flow_state = lambda d: "CONFIRME" if ofb == d else ("CONTRE" if ofb == -d else "NEUTRE")
    if plan:
        d = plan["dir"]
        hit_sl = price <= plan["sl"] if d == 1 else price >= plan["sl"]
        hit_tp = price >= plan["tp"] if d == 1 else price <= plan["tp"]
        reason = None
        if hit_tp:
            log("🎯 Objectif atteint (%.5f)" % price)
            STATE["plan"] = plan = None
            status, msg = "OBJECTIF ATTEINT", "TP touché : scénario terminé."
        elif hit_sl:
            reason = "niveau d'invalidation cassé (%.5f)" % price
        elif fb == -d:
            reason = "le biais fondamental s'est retourné"
        elif dxy_state(d, D)[0] == "CONTRE":
            reason = "le DXY est passé contre le scénario"
        elif flow_state(d) == "CONTRE":
            reason = "l'order flow est passé contre le scénario"
        elif now_t - plan["t"] > 12 * 3600:
            reason = "scénario expiré (12 h)"
        if reason:
            log("⛔ STOP : " + reason)
            STATE["plan"] = plan = None
            status, msg = "STOP", "Scénario arrêté : " + reason
        elif plan:
            if not plan["be"] and (price - plan["entry"]) * d >= abs(plan["entry"] - plan["sl"]):
                plan["be"] = True
                log("🔒 1R atteint : stop à break-even conseillé")
            status = "EN COURS"
            msg = "1R atteint : stop à break-even conseillé." if plan["be"] else "Le scénario suit toujours l'analyse."
    if not plan and status not in ("STOP", "OBJECTIF ATTEINT"):
        if closed:
            status, msg = "MARCHÉ FERMÉ", "Week-end : aucune nouvelle entrée."
        elif news_risk:
            status, msg = "PAUSE NEWS", "News majeure USD/EUR proche : pas de nouvelle entrée."
        elif mode == "CONFLIT":
            status, msg = "ATTENTE", "Fondamental (%s) et technique (%s) en désaccord : on n'entre pas." % (
                "haussier" if fb == 1 else "baissier", "haussière" if td == 1 else "baissière")
        elif dirn == 0:
            status, msg = "NEUTRE", "Fondamental et technique neutres : pas de trade."
        elif not cand:
            status, msg = "ATTENTE", "Biais %s (%s) mais aucune zone de confluence au niveau requis (%.0f)." % (
                "haussier" if dirn == 1 else "baissier", mode.lower(), need_score)
        else:
            d, rx, p = cand["dir"], cand["rx"], cand["plan"]
            shown = cand
            if flow_state(d) == "CONTRE":
                status, msg = "ATTENTE", "Order flow contre le biais : on attend l'alignement."
            elif not p:
                status, msg = "ATTENTE", "Zone repérée mais aucun objectif avec R:R >= %.1f ou stop trop large." % MIN_RR
            elif rx["confirmed"]:
                if cand["dxy"] != "CONTRE":
                    STATE["plan"] = plan = p
                    status, msg = "SETUP CONFIRMED", "Biais + zone de confluence + réaction + DXY : alignés (grade %s)." % cand["grade"]
                    log("🟢 SETUP CONFIRMED %s @ %.5f (grade %s, R:R %.2f)" % ("ACHAT" if d == 1 else "VENTE", p["entry"], cand["grade"], p["rr"]))
                else:
                    status, msg = "RÉACTION", "Réaction validée mais le DXY est contre."
            elif rx["touched"]:
                status, msg = "RÉACTION", "Zone touchée : %d/%d confirmations (il en faut %d)." % (rx["n_ok"], rx["n_av"], rx["need"])
            else:
                status = "SURVEILLANCE"
                msg = "Biais %s. Zone surveillée à %.0f pips du prix." % ("haussier" if d == 1 else "baissier", cand["dist_pips"])

    # tableau multi-timeframe
    nm = {1: "haussière", -1: "baissière", 0: "neutre"}
    rows = []
    for tf in ("1M", "1w", "1d", "4h", "1h", "15m", "5m", "1m"):
        t = T.get(tf)
        if not t:
            continue
        ev = t["ev"]
        rows.append({"tf": TFN[tf], "trend": nm[t["state"]], "state": t["state"],
                     "ev": ("%s %s il y a %d b." % (ev["kind"], "↑" if ev["dir"] > 0 else "↓", ev["ago"])) if ev else "—",
                     "pos": None if t["pos"] is None else round(t["pos"] * 100), "rsi": round(t["rsi"]),
                     "atr": round(t["atr"] / PIP, 1), "div": t["div"], "pat": t["pat"][1]})

    # échelle de niveaux
    lad = []
    for e in E:
        if e["fam"] == "zone" or e["w"] < 1.0:
            continue
        lad.append({"name": e["label"], "p": round(e["mid"], 5), "w": round(e["w"], 1), "fam": e["fam"]})
    lad.sort(key=lambda x: abs(x["p"] - price))
    lad = sorted(lad[:26], key=lambda x: -x["p"])
    zl = [{"name": e["label"], "lo": round(e["lo"], 5), "hi": round(e["hi"], 5), "w": round(e["w"], 1), "dir": e["dir"]}
          for e in E if e["fam"] == "zone"]
    zl.sort(key=lambda z: abs((z["lo"] + z["hi"]) / 2 - price))
    prof = None
    if P:
        r5 = lambda v: None if not v else {k: ([round(x, 5) for x in val] if isinstance(val, list) else (round(val, 5) if val is not None else None)) for k, val in v.items()}
        prof = {"day": r5(P.get("day")), "prev": r5(P.get("prev")), "week": r5(P.get("week")), "comp": r5(P.get("comp")),
                "vwap": None if not P.get("vwap") else {"v": round(P["vwap"]["v"], 5), "sd": round(P["vwap"]["sd"], 5)},
                "avwap": None if not P.get("avwap") else {"v": round(P["avwap"]["v"], 5), "sd": round(P["avwap"]["sd"], 5)},
                "naked": [round(x, 5) for x in P.get("naked", [])]}
    dst = dxy_state(dirn or 1, D)
    return {"price": price, "source": "Yahoo Finance · FRED · BCE · CFTC · Forex Factory", "status": status, "message": msg,
            "mode": mode, "dir": dirn, "need_score": need_score, "tech": tech, "tech_bias": td,
            "fund": fund, "tf": rows, "setups": setups[:4], "shown": ({"dir": shown["dir"]} if shown else None),
            "plan": plan, "profile": prof, "ladder": lad, "zones": zl[:10],
            "dxy": {"state": dst[0], "text": dst[1], "smt": {"bull": smtf[1], "bear": smtf[-1]}},
            "flow": flow, "news": news[:7], "log": STATE["log"], "errors": err,
            "session": {"name": ("Marché fermé" if closed else {"asia": "Asie", "london": "Londres", "ny": "New York", "off": "Hors session"}[session(now_t)]), "kz": kz},
            "updated": datetime.now(timezone.utc).strftime("%H:%M:%S UTC")}


@app.route("/api/analysis")
def api():
    try:
        with LOCK:
            t, v = STATE["res"]
            if v and time.time() - t < 25:
                return jsonify(v)
            v = analyse()
            STATE["res"] = (time.time(), v)
            return jsonify(v)
    except Exception as e:
        return jsonify({"error": str(e)})


@app.route("/health")
def health():
    return "ok"


@app.route("/")
def index():
    return Response(PAGE, mimetype="text/html")


PAGE = r"""<!doctype html><html lang="fr"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1"><title>EURUSD Engine v3</title>
<style>
:root{--bg:#0e1116;--card:#171c24;--tx:#e6e9ee;--mut:#8b94a3;--g:#22c55e;--r:#ef4444;--y:#eab308;--b:#3b82f6;--c:#22d3ee}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--tx);font:15px system-ui,sans-serif;padding:12px;max-width:720px;margin:auto}
.card{background:var(--card);border-radius:12px;padding:14px;margin-bottom:12px}
h2{margin:0 0 8px;font-size:13px;color:var(--mut);text-transform:uppercase;letter-spacing:.05em}
.top{display:flex;justify-content:space-between;align-items:center;gap:8px}.price{font-size:26px;font-weight:700}
.badge{padding:6px 12px;border-radius:20px;font-weight:700;font-size:13px;text-align:center}
.row{display:flex;justify-content:space-between;padding:5px 0;border-bottom:1px solid #222a35;gap:8px}
.row:last-child{border:0}.s{font-weight:700;min-width:30px;text-align:right}
.mut{color:var(--mut);font-size:12px}button{background:var(--b);color:#fff;border:0;border-radius:8px;padding:8px 12px}
.gauge{height:10px;background:#222a35;border-radius:6px;position:relative;margin:8px 0}.gauge i{position:absolute;top:-3px;width:4px;height:16px;background:#fff;border-radius:2px}
.gauge b{position:absolute;top:0;height:10px;border-radius:6px}
table{width:100%;border-collapse:collapse;font-size:13px}td,th{padding:5px 3px;border-bottom:1px solid #222a35;text-align:left}th{color:var(--mut);font-weight:600;font-size:11px}
.pill{display:inline-block;padding:2px 8px;border-radius:10px;font-size:11px;font-weight:700;margin-right:4px;background:#222a35}
.set{border:1px solid #2a3340;border-radius:10px;padding:10px;margin-bottom:8px}.set.ok{border-color:var(--c)}
.tag{display:inline-block;background:#222a35;border-radius:6px;padding:1px 6px;margin:2px 3px 2px 0;font-size:11px}
.mark{background:#1d3a55;border-left:3px solid var(--c)}
</style></head><body>
<div class="card"><div class="top"><div><div class="mut">EUR/USD</div><div class="price" id="price">...</div></div>
<div class="badge" id="status">Chargement</div></div>
<p id="msg" style="margin:10px 0 0"></p><div class="mut" id="upd"></div></div>
<div id="app"></div>
<div class="card"><button onclick="Notification.requestPermission()">Activer les notifications</button>
<div class="mut" style="margin-top:6px">Mise à jour toutes les 60 s tant que la page est ouverte. Outil d&#39;aide, pas un conseil financier. Les scores sont des heuristiques non validées, pas des probabilités.</div></div>
<script>
var last=null;
function esc(s){return String(s==null?'':s).replace(/[&<>]/g,function(c){return {'&':'&amp;','<':'&lt;','>':'&gt;'}[c]})}
function col(s){return s>0?'var(--g)':s<0?'var(--r)':'var(--mut)'}
function f5(x){return x==null?'—':Number(x).toFixed(5)}
function sg(x,d){var v=Number(x).toFixed(d==null?0:d);return (x>0?'+':'')+v}
function badgeColor(s){if(s=='SETUP CONFIRMED'||s=='EN COURS'||s=='OBJECTIF ATTEINT')return '--g';if(s=='STOP')return '--r';if(s=='SURVEILLANCE'||s=='RÉACTION')return '--b';if(s=='ATTENTE'||s=='PAUSE NEWS')return '--y';return '--mut'}
function gauge(v){var p=Math.max(-100,Math.min(100,v)),w=Math.abs(p)/2;return '<div class="gauge"><b style="'+(p>=0?'left:50%':'right:50%')+';width:'+w+'%;background:'+col(p)+'"></b><i style="left:calc('+(50+p/2)+'% - 2px)"></i></div>'}
function planHtml(p,tag){if(!p)return '';return '<div class="row"><span>Entrée</span><b>'+f5(p.entry)+'</b></div><div class="mut">'+esc(p.mode)+'</div>'
 +'<div class="row"><span>Invalidation (SL)</span><b style="color:var(--r)">'+f5(p.sl)+' (-'+p.pips_sl+' pips)</b></div>'
 +'<div class="row"><span>TP1 ('+esc(p.tp_kind)+')</span><b style="color:var(--g)">'+f5(p.tp)+' (+'+p.pips_tp+' pips)</b></div><div class="row"><span>R:R TP1</span><b>'+p.rr+'</b></div>'
 +(p.tp2?'<div class="row"><span>TP2 ('+esc(p.tp2_kind)+')</span><b>'+f5(p.tp2)+' (R:R '+p.rr2+')</b></div>':'')}
function render(d){
 if(d.error){document.getElementById('msg').textContent='Erreur : '+d.error;return}
 var st=document.getElementById('status');st.textContent=d.status;st.style.background='var('+badgeColor(d.status)+')';st.style.color='#000';
 document.getElementById('price').textContent=f5(d.price);document.getElementById('msg').textContent=d.message;
 document.getElementById('upd').textContent=d.session.name+(d.session.kz?' · killzone '+d.session.kz:'')+' | '+d.source+' | MAJ '+d.updated;
 var F=d.fund,h='';
 var fb=F.bias>0?'HAUSSIER':F.bias<0?'BAISSIER':'NEUTRE';
 if(F.loading){h+='<div class="card"><h2>1. Fondamental quantitatif</h2><div class="mut">Chargement des données macro (FRED, BCE, CFTC)… réponse dans ~1 minute, la page se mettra à jour toute seule.</div></div>'}
 else h+='<div class="card"><h2>1. Fondamental quantitatif : '+fb+' ('+sg(F.score,0)+' / ±100)</h2>'+gauge(F.score)
  +'<div class="mut">Consensus pondéré : '+Math.round(F.breadth*100)+' % des facteurs vont dans le sens du score · couverture des données '+Math.round(F.coverage*100)+' %'+(F.dominant?' · facteur dominant : '+esc(F.dominant):'')+'</div>'
  +F.comps.map(function(c){return '<div class="row"><span>'+esc(c.label)+'<div class="mut">'+esc(c.note)+' · poids '+c.w+'</div></span><span class="s" style="color:'+col(c.score)+'">'+sg(c.score,1)+'</span></div>'}).join('')
  +(F.model?'<div class="mut" style="margin-top:6px">Modèle : EUR/USD théorique '+f5(F.model.fair)+' (écart '+sg(F.model.z,1)+' σ, R² '+F.model.r2+(F.model.r2<0.2?' : trop faible, ignoré':'')+')</div>':'')
  +(F.missing&&F.missing.length?'<div class="mut">Sources indisponibles : '+esc(F.missing.join(', '))+'</div>':'')+'</div>';
 h+='<div class="card"><h2>2. Structure multi-timeframe · technique '+sg(d.tech,0)+' · mode '+esc(d.mode)+'</h2>'+gauge(d.tech)
  +'<table><tr><th>TF</th><th>Structure</th><th>Dernier événement</th><th>Range</th><th>RSI</th><th>ATR</th></tr>'
  +d.tf.map(function(r){return '<tr><td><b>'+esc(r.tf)+'</b></td><td style="color:'+col(r.state)+'">'+esc(r.trend)+'</td><td>'+esc(r.ev)+'</td><td>'+(r.pos==null?'—':r.pos+'%')+'</td><td>'+r.rsi+(r.div?(r.div>0?' ↗':' ↘'):'')+'</td><td>'+r.atr+'</td></tr>'}).join('')+'</table>'
  +'<div class="mut">Range : 0 % = bas du dernier swing (discount), 100 % = haut (premium). ↗/↘ = divergence RSI.</div></div>';
 h+='<div class="card"><h2>3. Setups de confluence (OB, imbalance, breaker, liquidité, VWAP/POC...)</h2>';
 if(!d.setups.length)h+='<div class="mut">Aucune zone de confluence proche.</div>';
 d.setups.forEach(function(s){
  h+='<div class="set'+(s.allowed?' ok':'')+'"><div class="row" style="border:0"><span><span class="pill" style="background:'+(s.dir>0?'var(--g)':'var(--r)')+';color:#000">'+(s.dir>0?'ACHAT':'VENTE')+'</span><span class="pill">Grade '+s.grade+'</span><span class="pill">score '+s.score+'</span>'+(s.allowed?'<span class="pill" style="background:var(--c);color:#000">autorisé</span>':'<span class="pill">non autorisé</span>')+'</span><b>'+f5(s.lo)+' – '+f5(s.hi)+'</b></div>'
   +'<div class="mut">'+(s.inside?'Prix dans la zone':'À '+s.dist_pips+' pips')+' · DXY '+esc(s.dxy)+' · '+esc(s.fams.join(', '))+'</div>'
   +'<div>'+s.labels.map(function(l){return '<span class="tag">'+esc(l)+'</span>'}).join('')+'</div>'
   +(s.allowed&&s.rx?'<div style="margin-top:6px">'+s.rx.checks.map(function(k){return '<div class="row"><span>'+esc(k.label)+'</span><span class="s" style="color:'+(k.ok?'var(--g)':'var(--mut)')+'">'+(k.av?(k.ok?'OK':'-'):'n/d')+'</span></div>'}).join('')+'<div class="mut">'+s.rx.n_ok+'/'+s.rx.n_av+' confirmations (il en faut '+s.rx.need+')</div></div>':'')
   +(s.plan?'<div style="margin-top:6px">'+planHtml(s.plan)+'</div>':'<div class="mut">Pas de plan : stop trop large ou aucun objectif avec R:R suffisant.</div>')
   +'<details><summary class="mut">Pourquoi ce score ?</summary>'+s.why.map(function(w){return '<div class="mut">'+esc(w)+'</div>'}).join('')+'</details></div>'});
 h+='<div class="mut">Score de confluence = poids des éléments (timeframe, type) + contexte. Ce n&#39;est pas une probabilité de gain. Seuil requis : '+d.need_score+'.</div></div>';
 if(d.plan){h+='<div class="card"><h2>4. Plan actif : '+(d.plan.dir>0?'ACHAT':'VENTE')+' (grade '+esc(d.plan.grade)+')</h2>'+planHtml(d.plan)+'<div>'+d.plan.labels.map(function(l){return '<span class="tag">'+esc(l)+'</span>'}).join('')+'</div></div>'}
 var dc=d.dxy.state=='CONFIRME'?'var(--g)':d.dxy.state=='CONTRE'?'var(--r)':'var(--mut)';
 h+='<div class="card"><h2>5. DXY & SMT : <span style="color:'+dc+'">'+d.dxy.state+'</span></h2><div class="mut">'+esc(d.dxy.text)+'</div>'
  +'<div class="mut">Divergence SMT haussière : '+(d.dxy.smt.bull?'OUI':'non')+' · baissière : '+(d.dxy.smt.bear?'OUI':'non')+'</div></div>';
 h+='<div class="card"><h2>Order flow (proxys gratuits)</h2>'+d.flow.map(function(i){return '<div class="row"><span>'+esc(i.label)+'</span><span class="s" style="color:'+col(i.score)+'">'+sg(i.score,0)+'</span></div>'}).join('')+'<div class="mut">Pas de carnet d&#39;ordres gratuit en forex : delta estimé à partir des futures CME 6E (volume réel).</div></div>';
 var P=d.profile;
 if(P){function pr(n,v){return v?'<div class="row"><span>'+n+'</span><span><b>POC '+f5(v.poc)+'</b> · VAH '+f5(v.vah)+' · VAL '+f5(v.val)+'</span></div>':''}
  h+='<div class="card"><h2>Profil de volume & VWAP (futures 6E, ramenés en spot)</h2>'+pr('Jour',P.day)+pr('Veille',P.prev)+pr('Semaine',P.week)+pr('5 jours',P.comp)
   +(P.vwap?'<div class="row"><span>VWAP jour</span><span><b>'+f5(P.vwap.v)+'</b> · ±1σ '+f5(P.vwap.v-P.vwap.sd)+' / '+f5(P.vwap.v+P.vwap.sd)+'</span></div>':'')
   +(P.avwap?'<div class="row"><span>AVWAP depuis le dernier CHoCH H1</span><b>'+f5(P.avwap.v)+'</b></div>':'')
   +(P.naked&&P.naked.length?'<div class="row"><span>POC naked (non revisités)</span><b>'+P.naked.map(f5).join(' · ')+'</b></div>':'')+'</div>'}
 h+='<div class="card"><h2>Échelle de niveaux (prix actuel en surbrillance)</h2>';var placed=false;
 d.ladder.forEach(function(x){if(!placed&&x.p<d.price){placed=true;h+='<div class="row mark"><span><b>Prix actuel</b></span><b>'+f5(d.price)+'</b></div>'}
  h+='<div class="row"><span>'+esc(x.name)+'</span><span><b>'+f5(x.p)+'</b> <span class="mut">'+Math.round((x.p-d.price)/0.0001)+' p</span></span></div>'});
 if(!placed)h+='<div class="row mark"><span><b>Prix actuel</b></span><b>'+f5(d.price)+'</b></div>';
 h+='</div><div class="card"><h2>Zones non mitigées (imbalance, order blocks, breakers)</h2>'
  +(d.zones.length?d.zones.map(function(z){return '<div class="row"><span>'+esc(z.name)+'</span><span><b>'+f5(z.lo)+' – '+f5(z.hi)+'</b> <span class="mut">poids '+z.w+'</span></span></div>'}).join(''):'<div class="mut">Aucune</div>')+'</div>';
 h+='<div class="card"><h2>News USD/EUR (impact moyen/fort)</h2>'+(d.news.length?d.news.map(function(n){return '<div class="row"><span>'+(n.impact=='High'?'🔴 ':'🟠 ')+esc(n.country+' '+n.title)+'<div class="mut">prév. '+esc(n.forecast||'—')+' · préc. '+esc(n.previous||'—')+'</div></span><span class="mut">'+esc(n.time)+'</span></div>'}).join(''):'<div class="mut">Aucune dans les 36 h</div>')+'</div>';
 h+='<div class="card"><h2>Journal</h2>'+(d.log.length?d.log.map(function(l){return '<div class="row"><span>'+esc(l.msg)+'</span><span class="mut">'+esc(l.t)+'</span></div>'}).join(''):'<div class="mut">Rien pour le moment</div>')+'</div>';
 var ek=Object.keys(d.errors||{});if(ek.length)h+='<div class="card"><h2>Données optionnelles indisponibles</h2><div class="mut">'+ek.map(function(k){return esc(k+' : '+d.errors[k])}).join('<br>')+'</div></div>';
 document.getElementById('app').innerHTML=h;
 if(last&&last!==d.status&&window.Notification&&Notification.permission==='granted')new Notification('EURUSD : '+d.status,{body:d.message});
 last=d.status;
}
function load(){fetch('/api/analysis').then(function(r){return r.json()}).then(render).catch(function(){document.getElementById('msg').textContent='Connexion impossible'})}
load();setInterval(load,60000);
</script></body></html>"""

try:                                   # préchauffe les données macro dès le démarrage
    FUND_JOB["running"] = True
    threading.Thread(target=_fund_worker, daemon=True).start()
except Exception:
    FUND_JOB["running"] = False

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 5000)))
