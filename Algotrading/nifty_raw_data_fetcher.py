"""
NIFTY Raw Data Fetcher — Pure NSE
==================================
Fetches NIFTY 50 daily OHLC + F&O bhavcopy (CE & PE, ATM±5).
No yfinance. All data from NSE only.

HOW IT SOLVES THE AKAMAI PROBLEM
─────────────────────────────────
NSE uses Akamai Bot Manager which blocks plain requests.Session() calls
because it requires JavaScript execution to issue nsit/nseappid cookies.

This script uses Playwright (real Chromium) to:
  1. Visit NSE pages so Akamai's JS challenge runs in a real browser engine
  2. Harvest the resulting nsit + nseappid cookies
  3. Pass those cookies to requests for all subsequent data fetches

The browser stays open. Cookies are re-warmed every COOKIE_REWARM_EVERY
dates to prevent expiry mid-run.

INDEX DATA SOURCE
─────────────────
NSE's historical index API:
  https://www.nseindia.com/api/historical/indicesHistory
  ?indexType=NIFTY%2050&from=DD-MM-YYYY&to=DD-MM-YYYY

This returns OHLC for a date range. We fetch one date at a time to keep
error handling simple and honour holiday detection.

INSTALL
───────
  pip install playwright pandas requests
  python -m playwright install chromium

RUN
───
  python nifty_raw_data_fetcher.py
"""

from __future__ import annotations
import io, os, csv, time, zipfile, requests
import pandas as pd
from datetime import datetime, timedelta

try:
    from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout
    _PW_OK = True
except ImportError:
    _PW_OK = False
    raise SystemExit(
        "playwright not installed.\n"
        "  pip install playwright\n"
        "  python -m playwright install chromium"
    )

# ═══════════════════════════════════════════════════════════════════════════════
#  CONFIG
# ═══════════════════════════════════════════════════════════════════════════════
START_DATE          = "01-01-2024"   # DD-MM-YYYY
END_DATE            = "24-04-2026"   # DD-MM-YYYY
STRIKE_STEP         = 50
STRIKES_EACH_SIDE   = 5
OUTPUT_FILE         = "nifty_raw_data.csv"
RESUME              = True
COOKIE_REWARM_EVERY = 20   # re-open browser session every N trading dates
REQUEST_DELAY       = 0.6  # seconds between requests
REQUEST_TIMEOUT     = 20
# ═══════════════════════════════════════════════════════════════════════════════

FO_CUTOVER = datetime(2024, 7, 8)  # NSE switched to UDiFF format on this date
MON3 = ["JAN","FEB","MAR","APR","MAY","JUN",
         "JUL","AUG","SEP","OCT","NOV","DEC"]

# NSE URLs
_INDEX_HIST_URL = (
    "https://www.nseindia.com/api/historical/indicesHistory"
    "?indexType=NIFTY%2050&from={from_dt}&to={to_dt}"
)
FO_OLD_URL = (
    "https://archives.nseindia.com/content/historical/DERIVATIVES"
    "/{yyyy}/{mon}/fo{dd}{mon}{yyyy}bhav.csv.zip"
)
FO_NEW_URL = (
    "https://nsearchives.nseindia.com/content/fo"
    "/BhavCopy_NSE_FO_0_0_0_{yyyymmdd}_F_0000.csv.zip"
)

_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)
_HEADERS = {
    "User-Agent":      _UA,
    "Accept":          "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer":         "https://www.nseindia.com/",
    "Connection":      "keep-alive",
    "X-Requested-With":"XMLHttpRequest",
}


# ───────────────────────────────────────────────────────────────────────────────
#  Utilities
# ───────────────────────────────────────────────────────────────────────────────

def _parse_date(s: str) -> datetime:
    for fmt in ("%d-%m-%Y", "%Y-%m-%d"):
        try: return datetime.strptime(s, fmt)
        except ValueError: pass
    raise ValueError(f"Bad date {s!r}  — use DD-MM-YYYY")

def _to_num(series: pd.Series) -> pd.Series:
    return pd.to_numeric(
        series.astype(str).str.replace(",", "", regex=False), errors="coerce")

def _atm(close: float) -> int:
    return int(round(close / STRIKE_STEP) * STRIKE_STEP)

def _val(x) -> float | str:
    try:
        v = float(x); return "" if pd.isna(v) else v
    except Exception: return ""


# ───────────────────────────────────────────────────────────────────────────────
#  NSE Cookie Jar  (Playwright — real Chromium browser)
# ───────────────────────────────────────────────────────────────────────────────

class NSESession:
    """
    Uses a real Chromium browser to pass Akamai's JS challenge and
    harvest nsit/nseappid cookies.  Then provides authenticated
    requests.Session objects for all NSE API / archive calls.
    """

    # Pages visited in order — each one gets closer to the API cookies
    _WARM_SEQUENCE = [
        ("https://www.nseindia.com",               "domcontentloaded", 2500),
        ("https://www.nseindia.com/market-data/live-equity-market", "domcontentloaded", 2000),
        ("https://www.nseindia.com/option-chain",  "domcontentloaded", 2500),
        ("https://www.nseindia.com/api/allIndices", "commit",          1500),
    ]

    def __init__(self):
        self._pw = self._browser = self._ctx = None
        self._cookies: dict[str, str] = {}
        self._since_warm = 0

    def start(self):
        print("  [NSE] Launching Chromium …")
        self._pw      = sync_playwright().start()
        self._browser = self._pw.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--disable-blink-features=AutomationControlled",
                "--disable-web-security",
            ],
        )
        self._warm()

    def _warm(self):
        """Visit NSE pages in sequence; Akamai JS runs → sets nsit/nseappid."""
        if self._ctx:
            try: self._ctx.close()
            except: pass

        self._ctx = self._browser.new_context(
            user_agent=_UA,
            viewport={"width": 1366, "height": 768},
            locale="en-IN",
            timezone_id="Asia/Kolkata",
            extra_http_headers={"Accept-Language": "en-US,en;q=0.9"},
        )
        page = self._ctx.new_page()

        for url, wait_until, pause_ms in self._WARM_SEQUENCE:
            try:
                page.goto(url, timeout=25_000, wait_until=wait_until)
                page.wait_for_timeout(pause_ms)
            except PWTimeout:
                print(f"    [WARN] Timeout warming {url}")
            except Exception as e:
                print(f"    [WARN] {url}: {e}")

        try: page.close()
        except: pass

        self._cookies = {c["name"]: c["value"] for c in self._ctx.cookies()}
        have = [k for k in ("nsit", "nseappid") if k in self._cookies]
        all_names = list(self._cookies.keys())
        print(f"  [NSE] Cookies acquired: {all_names}")

        if have:
            print(f"  [NSE] ✓ Auth cookies ready: {have}")
        else:
            print("  [NSE] ✗ nsit/nseappid still missing — will retry on next warm")

        self._since_warm = 0

    def maybe_rewarm(self):
        self._since_warm += 1
        if self._since_warm >= COOKIE_REWARM_EVERY:
            print("  [NSE] Re-warming session cookies …")
            self._warm()

    def session(self) -> requests.Session:
        """Return a requests.Session pre-loaded with current NSE cookies."""
        s = requests.Session()
        s.headers.update(_HEADERS)
        for name, value in self._cookies.items():
            s.cookies.set(name, value, domain=".nseindia.com")
        return s

    def stop(self):
        for obj, method in [
            (self._ctx,     "close"),
            (self._browser, "close"),
            (self._pw,      "stop"),
        ]:
            try:
                if obj: getattr(obj, method)()
            except: pass


# ───────────────────────────────────────────────────────────────────────────────
#  Index OHLC  —  NSE historical index API
# ───────────────────────────────────────────────────────────────────────────────

def fetch_index_day(sess: requests.Session, dt: datetime) -> dict | None:
    """
    Calls NSE's /api/historical/indicesHistory for a single date.
    Returns {date, open, high, low, close, volume} or None (holiday/error).
    """
    date_str = dt.strftime("%d-%m-%Y")
    url = _INDEX_HIST_URL.format(from_dt=date_str, to_dt=date_str)

    for attempt in range(3):
        try:
            r = sess.get(url, timeout=REQUEST_TIMEOUT)
            if r.status_code == 401:
                # Session expired — signal caller to rewarm
                print(f"    [WARN] 401 on index API — session expired")
                return "REWARM"
            if r.status_code != 200:
                print(f"    [WARN] Index API status {r.status_code} for {date_str}")
                return None

            data = r.json()
            # Response: {"data": {"indexCloseOnlineRecords": [...], ...}}
            records = (data.get("data") or {}).get("indexCloseOnlineRecords", [])
            if not records:
                return None   # holiday or weekend

            rec = records[0]

            def _g(*keys):
                for k in keys:
                    if k in rec:
                        try: return float(str(rec[k]).replace(",", ""))
                        except: pass
                return None

            close = _g("EOD_CLOSE_INDEX_VAL", "CLOSE", "close")
            if not close:
                return None

            return {
                "date":   dt.strftime("%Y-%m-%d"),
                "open":   _g("EOD_OPEN_INDEX_VAL",  "OPEN",   "open"),
                "high":   _g("EOD_HIGH_INDEX_VAL",  "HIGH",   "high"),
                "low":    _g("EOD_LOW_INDEX_VAL",   "LOW",    "low"),
                "close":  close,
                "volume": _g("EOD_TRADED_VOL",      "VOLUME", "volume"),
            }

        except requests.RequestException as e:
            print(f"    [WARN] Index request error (attempt {attempt+1}): {e}")
            time.sleep(1.5 * (attempt + 1))

    return None


# ───────────────────────────────────────────────────────────────────────────────
#  F&O bhavcopy  —  NSE archives
# ───────────────────────────────────────────────────────────────────────────────

def _get_zip(sess: requests.Session, url: str) -> bytes | None:
    for attempt in range(3):
        try:
            r = sess.get(url, timeout=REQUEST_TIMEOUT)
            if r.status_code == 404:
                return None
            if r.status_code == 401:
                return "REWARM"
            if r.status_code == 200 and r.content[:2] == b"PK":
                return r.content
            if r.status_code == 403:
                print(f"    [WARN] 403 on ZIP (attempt {attempt+1}/3) — cookies may have expired")
        except requests.RequestException as e:
            print(f"    [WARN] ZIP request error: {e}")
        time.sleep(1.5 * (attempt + 1))
    return None


def _parse_fo_old(raw: pd.DataFrame) -> pd.DataFrame | None:
    raw = raw.reset_index(drop=True)
    raw.columns = raw.columns.str.strip().str.upper()
    inst = raw.get("INSTRUMENT", pd.Series([""]*len(raw))).astype(str).str.upper().str.contains("OPTIDX", na=False)
    sym  = raw.get("SYMBOL",     pd.Series([""]*len(raw))).astype(str).str.strip().str.upper() == "NIFTY"
    df   = raw[inst & sym].copy().reset_index(drop=True)
    if df.empty: return None

    rm = {"OPTION_TYP":"OPT_TYPE","STRIKE_PR":"STRIKE","SETTLE_PR":"SETTLE",
          "OPEN_INT":"OI","NO_OF_CONT":"VOLUME","CONTRACTS":"VOLUME",
          "NO_OF_CONTRACTS":"VOLUME","TRADED_QTY":"VOLUME","TTL_TRADG_VOL":"VOLUME"}
    df = df.rename(columns={k:v for k,v in rm.items() if k in df.columns})
    if "VOLUME" not in df.columns: df["VOLUME"] = float("nan")

    df["EXPIRY_DT"] = pd.to_datetime(df["EXPIRY_DT"], dayfirst=True, errors="coerce")
    df["OPT_TYPE"]  = df["OPT_TYPE"].astype(str).str.strip().str.upper()
    for c in ("STRIKE","OPEN","HIGH","LOW","CLOSE","SETTLE","OI","VOLUME"):
        if c in df.columns: df[c] = _to_num(df[c])

    keep = ["EXPIRY_DT","OPT_TYPE","STRIKE","OPEN","HIGH","LOW","CLOSE","SETTLE","OI","VOLUME"]
    return df[[c for c in keep if c in df.columns]]


def _parse_fo_new(raw: pd.DataFrame) -> pd.DataFrame | None:
    raw = raw.reset_index(drop=True)
    raw.columns = raw.columns.str.strip()

    ic = next((c for c in raw.columns if c.upper() in ("FININSTRMTP","FININSTRMTYPE","FIN_INSTRM_TP")), None)
    sc = next((c for c in raw.columns if c.upper() in ("TCKRSYMB","TCKR_SYMB","SYMBOL")), None)
    if not ic or not sc: return None

    mask = (raw[ic].astype(str).str.upper().isin(["IDO","OPTIDX"]) &
            raw[sc].astype(str).str.strip().str.upper().isin(["NIFTY","NIFTY 50"]))
    df = raw[mask].copy().reset_index(drop=True)
    if df.empty: return None

    cu = {c.upper(): c for c in df.columns}
    def gc(*opts):
        for o in opts:
            if o.upper() in cu: return cu[o.upper()]
        return None

    out = pd.DataFrame()
    ec = gc("XpryDt","EXPIRY_DT","ExpiryDate")
    out["EXPIRY_DT"] = pd.to_datetime(df[ec], errors="coerce") if ec else pd.NaT
    oc = gc("OptnTp","OPTION_TYP","OptionType","CE_PE")
    out["OPT_TYPE"]  = df[oc].astype(str).str.strip().str.upper() if oc else ""
    for dst, *srcs in [
        ("STRIKE",  "StrkPric","STRIKE_PR","StrikePrice"),
        ("OPEN",    "OpnPric","OPEN","OpenPrice"),
        ("HIGH",    "HghPric","HIGH","HighPrice"),
        ("LOW",     "LwPric","LOW","LowPrice"),
        ("CLOSE",   "ClsPric","CLOSE","ClosePrice"),
        ("SETTLE",  "SttlmPric","SETTLE_PR","SettlementPrice"),
        ("OI",      "OpnIntrst","OPEN_INT","OpenInterest"),
        ("VOLUME",  "TtlTradgVol","NO_OF_CONT","Volume","CONTRACTS"),
    ]:
        col = gc(*srcs)
        out[dst] = _to_num(df[col]) if col else float("nan")

    keep = ["EXPIRY_DT","OPT_TYPE","STRIKE","OPEN","HIGH","LOW","CLOSE","SETTLE","OI","VOLUME"]
    return out[[c for c in keep if c in out.columns]]


def fetch_fo_day(sess: requests.Session, dt: datetime) -> pd.DataFrame | None | str:
    if dt >= FO_CUTOVER:
        url = FO_NEW_URL.format(yyyymmdd=dt.strftime("%Y%m%d"))
    else:
        mon = MON3[dt.month - 1]
        url = FO_OLD_URL.format(yyyy=dt.strftime("%Y"), mon=mon, dd=dt.strftime("%d"))

    content = _get_zip(sess, url)
    if content == "REWARM": return "REWARM"
    if content is None:     return None

    try:
        zf  = zipfile.ZipFile(io.BytesIO(content))
        raw = pd.read_csv(io.BytesIO(zf.read(zf.namelist()[0])), low_memory=False)
    except Exception as e:
        print(f"    [WARN] ZIP parse error {dt.date()}: {e}")
        return None

    return _parse_fo_new(raw) if dt >= FO_CUTOVER else _parse_fo_old(raw)


# ───────────────────────────────────────────────────────────────────────────────
#  Row builder
# ───────────────────────────────────────────────────────────────────────────────

def _opt_fields(fo_df, expiry_dt, opt_type, strike) -> dict:
    empty = {"open":"","high":"","low":"","close":"","settle":"","oi":"","volume":""}
    if fo_df is None or fo_df.empty or expiry_dt is None: return empty
    sub = fo_df[
        (fo_df["EXPIRY_DT"].dt.date == expiry_dt) &
        (fo_df["OPT_TYPE"]          == opt_type.upper()) &
        (fo_df["STRIKE"]            == float(strike))
    ]
    if sub.empty: return empty
    r = sub.iloc[0]
    return {
        "open":   _val(r.get("OPEN")),
        "high":   _val(r.get("HIGH")),
        "low":    _val(r.get("LOW")),
        "close":  _val(r.get("CLOSE") if pd.notna(r.get("CLOSE")) else r.get("SETTLE")),
        "settle": _val(r.get("SETTLE")),
        "oi":     _val(r.get("OI")),
        "volume": _val(r.get("VOLUME")),
    }


def build_row(idx: dict, fo_df) -> dict:
    close = idx["close"]
    atm   = _atm(close)

    nearest_expiry = ""
    if fo_df is not None and not fo_df.empty:
        today  = datetime.strptime(idx["date"], "%Y-%m-%d").date()
        future = sorted([pd.Timestamp(e).date()
                         for e in fo_df["EXPIRY_DT"].dropna().unique()
                         if pd.Timestamp(e).date() >= today])
        if future: nearest_expiry = str(future[0])

    row = {
        "date":           idx["date"],
        "open":           _val(idx["open"]),
        "high":           _val(idx["high"]),
        "low":            _val(idx["low"]),
        "close":          _val(idx["close"]),
        "volume":         _val(idx["volume"]),
        "atm_strike":     atm,
        "nearest_expiry": nearest_expiry,
    }
    exp_date   = datetime.strptime(nearest_expiry, "%Y-%m-%d").date() if nearest_expiry else None
    _empty_opt = {"open":"","high":"","low":"","close":"","settle":"","oi":"","volume":""}

    for side in ("CE", "PE"):
        for offset in range(-STRIKES_EACH_SIDE, STRIKES_EACH_SIDE + 1):
            strike = atm + offset * STRIKE_STEP
            label  = f"{side}_ATM{offset:+d}" if offset != 0 else f"{side}_ATM+0"
            f = _opt_fields(fo_df, exp_date, side, strike) if exp_date else _empty_opt
            for field in ("strike","open","high","low","close","settle","oi","volume"):
                row[f"{label}_{field}"] = (strike if field == "strike" else f[field])
    return row


# ───────────────────────────────────────────────────────────────────────────────
#  CSV helpers
# ───────────────────────────────────────────────────────────────────────────────

def _build_header() -> list[str]:
    base = ["date","open","high","low","close","volume","atm_strike","nearest_expiry"]
    cols = []
    for side in ("CE","PE"):
        for offset in range(-STRIKES_EACH_SIDE, STRIKES_EACH_SIDE + 1):
            label = f"{side}_ATM{offset:+d}" if offset != 0 else f"{side}_ATM+0"
            for field in ("strike","open","high","low","close","settle","oi","volume"):
                cols.append(f"{label}_{field}")
    return base + cols


def _already_fetched(fp: str) -> set[str]:
    if not os.path.exists(fp): return set()
    try:
        df = pd.read_csv(fp, usecols=["date"], dtype=str)
        return set(df["date"].dropna().tolist())
    except: return set()


# ───────────────────────────────────────────────────────────────────────────────
#  Main
# ───────────────────────────────────────────────────────────────────────────────

def fetch_all():
    start = _parse_date(START_DATE)
    end   = _parse_date(END_DATE)

    done        = set()
    file_exists = os.path.exists(OUTPUT_FILE)
    if RESUME and file_exists:
        done = _already_fetched(OUTPUT_FILE)
        print(f"  [RESUME] {len(done)} dates already saved — skipping.")

    header   = _build_header()
    csv_file = open(OUTPUT_FILE, "a", newline="", encoding="utf-8")
    writer   = csv.DictWriter(csv_file, fieldnames=header, extrasaction="ignore")
    if not file_exists or len(done) == 0:
        writer.writeheader()
        csv_file.flush()

    print(f"\n{'='*65}")
    print(f"  NIFTY Raw Data Fetcher  [Pure NSE + Playwright]")
    print(f"  Range   : {start.date()} → {end.date()}")
    print(f"  Strikes : ATM±{STRIKES_EACH_SIDE}  (CE + PE)")
    print(f"  Output  : {OUTPUT_FILE}")
    print(f"{'='*65}\n")

    nse = NSESession()
    nse.start()

    current = start
    fetched = holidays = 0

    try:
        while current <= end:
            if current.weekday() >= 5:          # skip weekends
                current += timedelta(days=1); continue

            date_str = current.strftime("%Y-%m-%d")
            if date_str in done:
                current += timedelta(days=1); continue

            # ── Re-warm check ──────────────────────────────────────────────
            nse.maybe_rewarm()
            sess = nse.session()

            # ── Index OHLC ─────────────────────────────────────────────────
            idx = fetch_index_day(sess, current)
            time.sleep(REQUEST_DELAY)

            if idx == "REWARM":
                print(f"  {date_str}  [SESSION EXPIRED — re-warming]")
                nse._warm(); sess = nse.session()
                idx = fetch_index_day(sess, current)
                time.sleep(REQUEST_DELAY)

            if idx is None:
                holidays += 1
                print(f"  {date_str}  [HOLIDAY / NO DATA]")
                current += timedelta(days=1); continue

            # ── F&O bhavcopy ───────────────────────────────────────────────
            fo_df = fetch_fo_day(sess, current)
            time.sleep(REQUEST_DELAY)

            if fo_df == "REWARM":
                print(f"  {date_str}  [FO SESSION EXPIRED — re-warming]")
                nse._warm(); sess = nse.session()
                fo_df = fetch_fo_day(sess, current)
                time.sleep(REQUEST_DELAY)

            opt_status = "✓ options" if fo_df is not None and not fo_df.empty else "✗ no options"

            # ── Write row ──────────────────────────────────────────────────
            row = build_row(idx, fo_df)
            writer.writerow(row)
            csv_file.flush()
            fetched += 1

            print(f"  {date_str}  close={idx['close']:.2f}  "
                  f"ATM={_atm(idx['close'])}  "
                  f"expiry={row['nearest_expiry']}  "
                  f"{opt_status}  [row {fetched}]")

            current += timedelta(days=1)

    finally:
        nse.stop()
        csv_file.close()

    print(f"\n{'='*65}")
    print(f"  Done.  Fetched: {fetched}  Holidays/no-data: {holidays}")
    print(f"  Output: {OUTPUT_FILE}")
    print(f"{'='*65}\n")

    try:
        df = pd.read_csv(OUTPUT_FILE)
        print(f"  Rows    : {len(df)}")
        print(f"  Dates   : {df['date'].min()} → {df['date'].max()}")
        print(f"  Columns : {len(df.columns)}")
        print("\n  Sample (first row):"); print(df.head(1).T.to_string())
    except Exception as e:
        print(f"  [WARN] Summary: {e}")


if __name__ == "__main__":
    fetch_all()
