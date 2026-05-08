"""
NIFTY 50 — Historical + Live Data Fetcher (NSE Only)
=====================================================
Historical output : nifty50_combined.csv  (1 row/day, index OHLCV + options)
Intraday output   : nifty50_15min_candles.csv  (1 row per 15-min candle)
Live output       : terminal snapshot + optional CSV save

Usage:
  python nifty_data.py                            # fetch historical data
  python nifty_data.py --live                     # live snapshot (index + options)
  python nifty_data.py --live --watch 30          # auto-refresh every 30 seconds
  python nifty_data.py --intraday                 # 15-min candle mode (market hours only)
  python nifty_data.py --intraday --otm 7         # ATM ± 7 option strikes per candle
  python nifty_data.py --intraday --expiry 1      # use next expiry instead of nearest
  python nifty_data.py --intraday --save          # save every candle close to CSV
  python nifty_data.py --live --expiry 1          # next expiry instead of nearest
  python nifty_data.py --live --otm 7             # show ATM ± 7 strikes
  python nifty_data.py --live --save              # also save snapshot to CSV
  python nifty_data.py --start 01-01-2024 --end 01-01-2025  # custom date range

Intraday candle trigger grid (IST):
  09:15 → 09:30 → 09:45 → 10:00 → … → 15:15 → 15:30
  Trigger fires exactly at each boundary after the candle closes.
  A 30-second warm-up window polls NSE every 5s before each close
  to accumulate fresh OHLCV ticks.

Install: pip install requests pandas
"""

import io
import os
import sys
import time
import argparse
import zipfile
import threading
import requests
import pandas as pd
from datetime import datetime, timedelta
from collections import deque


# ═══════════════════════════════════════════════════════════════════════════════
#  CONFIG — edit only these lines
# ═══════════════════════════════════════════════════════════════════════════════
START_DATE        = "01-01-2020"
END_DATE          = "24-04-2026"
OUTPUT_FILE       = "nifty50_combined.csv"
INTRADAY_FILE     = "nifty50_15min_candles.csv"
NIFTY_STRIKE_STEP = 50
CANDLE_MINUTES    = 15         # candle interval in minutes  ← 15-min grid
MARKET_OPEN_H     = 9
MARKET_OPEN_M     = 15
MARKET_CLOSE_H    = 15
MARKET_CLOSE_M    = 30
# ═══════════════════════════════════════════════════════════════════════════════

# ── Historical fetch constants ────────────────────────────────────────────────
HIST_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept":          "*/*",
    "Accept-Language": "en-IN,en-GB;q=0.9,en-US;q=0.8,en;q=0.7",
    "Referer":         "https://www.nseindia.com/",
    "DNT"            : "1",
}
RETRY_DELAY     = 0.35
REQUEST_TIMEOUT = 15
FO_CUTOVER      = datetime(2024, 7, 8)
MON3            = ["JAN","FEB","MAR","APR","MAY","JUN",
                   "JUL","AUG","SEP","OCT","NOV","DEC"]
INDEX_URL  = "https://archives.nseindia.com/content/indices/ind_close_all_{date}.csv"
FO_OLD_URL = ("https://archives.nseindia.com/content/historical/DERIVATIVES/"
              "{yyyy}/{mon}/fo{dd}{mon}{yyyy}bhav.csv.zip")
FO_NEW_URL = ("https://nsearchives.nseindia.com/content/fo/"
              "BhavCopy_NSE_FO_0_0_0_{yyyymmdd}_F_0000.csv.zip")

# ── Live fetch constants ───────────────────────────────────────────────────────
_NSE_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

# Headers for HTML page loads (warm-up steps)
_HEADERS_PAGE = {
    "User-Agent"               : _NSE_UA,
    "Accept"                   : ("text/html,application/xhtml+xml,application/xml;"
                                  "q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,"
                                  "application/signed-exchange;v=b3;q=0.7"),
    "Accept-Language"          : "en-IN,en-GB;q=0.9,en-US;q=0.8,en;q=0.7",
    "Accept-Encoding"          : "gzip, deflate, br",
    "Connection"               : "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Ch-Ua"                : '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
    "Sec-Ch-Ua-Mobile"         : "?0",
    "Sec-Ch-Ua-Platform"       : '"Windows"',
    "Sec-Fetch-Dest"           : "document",
    "Sec-Fetch-Mode"           : "navigate",
    "Sec-Fetch-Site"           : "none",
    "Sec-Fetch-User"           : "?1",
    "Cache-Control"            : "max-age=0",
    "DNT"                      : "1",
}

# Headers for XHR / JSON API calls
LIVE_HEADERS = {
    "User-Agent"      : _NSE_UA,
    "Accept"          : "application/json, text/plain, */*",
    "Accept-Language" : "en-IN,en-GB;q=0.9,en-US;q=0.8,en;q=0.7",
    "Accept-Encoding" : "gzip, deflate, br",
    "Referer"         : "https://www.nseindia.com/option-chain",
    "X-Requested-With": "XMLHttpRequest",
    "Connection"      : "keep-alive",
    "Sec-Ch-Ua"       : '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
    "Sec-Ch-Ua-Mobile": "?0",
    "Sec-Fetch-Dest"  : "empty",
    "Sec-Fetch-Mode"  : "cors",
    "Sec-Fetch-Site"  : "same-origin",
    "DNT"             : "1",
}

NSE_BASE          = "https://www.nseindia.com"
URL_MARKET_STATUS = f"{NSE_BASE}/api/marketStatus"
URL_ALL_INDICES   = f"{NSE_BASE}/api/allIndices"
URL_OPTION_CHAIN  = f"{NSE_BASE}/api/option-chain-indices?symbol=NIFTY"
LIVE_OTM_RANGE    = 5
LIVE_TIMEOUT      = 15


# ═══════════════════════════════════════════════════════════════════════════════
#  SHARED HELPERS
# ═══════════════════════════════════════════════════════════════════════════════

def _parse_date(s):
    for fmt in ("%d-%m-%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    raise ValueError(f"Bad date: {s!r} — use DD-MM-YYYY")


def _atm(close, step):
    return int(round(close / step) * step)


def _to_num(series):
    return pd.to_numeric(
        series.astype(str).str.replace(",", ""), errors="coerce"
    )


# ═══════════════════════════════════════════════════════════════════════════════
#  HISTORICAL DATA FUNCTIONS
# ═══════════════════════════════════════════════════════════════════════════════

def _hist_get(session, url):
    for attempt in range(3):
        try:
            r = session.get(url, headers=HIST_HEADERS, timeout=REQUEST_TIMEOUT)
            if r.status_code == 404:
                return None
            if r.status_code == 200:
                return r
            time.sleep(1.5 * (attempt + 1))
        except requests.RequestException:
            time.sleep(1.5 * (attempt + 1))
    return None


def _fetch_index_day(session, dt):
    url  = INDEX_URL.format(date=dt.strftime("%d%m%Y"))
    resp = _hist_get(session, url)
    if resp is None:
        return None
    try:
        df = pd.read_csv(io.StringIO(resp.text), thousands=",")
    except Exception:
        return None
    df.columns = df.columns.str.strip()
    name_col = next((c for c in df.columns
                     if "index" in c.lower() and "name" in c.lower()), None)
    if name_col is None:
        return None
    row = df[df[name_col].str.strip().str.upper() == "NIFTY 50"]
    if row.empty:
        return None
    row = row.iloc[0]
    col = {c.strip().upper(): c for c in df.columns}
    def g(*keys):
        for k in keys:
            if k.upper() in col:
                try:
                    return float(str(row[col[k.upper()]]).replace(",", ""))
                except Exception:
                    return None
        return None
    return {
        "date":   dt.date(),
        "open":   g("Open Index Value",    "OPEN"),
        "high":   g("High Index Value",    "HIGH"),
        "low":    g("Low Index Value",     "LOW"),
        "close":  g("Closing Index Value", "CLOSE"),
        "volume": g("Volume",              "VOL"),
    }


def _parse_fo_old(raw):
    raw = raw.reset_index(drop=True)
    raw.columns = raw.columns.str.strip().str.upper()
    inst_mask = (raw.get("INSTRUMENT", pd.Series([""] * len(raw)))
                    .astype(str).str.upper().str.contains("OPTIDX", na=False))
    sym_mask  = (raw.get("SYMBOL", pd.Series([""] * len(raw)))
                    .astype(str).str.strip().str.upper() == "NIFTY")
    df = raw[inst_mask & sym_mask].copy().reset_index(drop=True)
    if df.empty:
        return None
    rename_map = {
        "OPTION_TYP": "OPT_TYPE", "STRIKE_PR": "STRIKE",
        "SETTLE_PR": "SETTLE",    "OPEN_INT":  "OI",
        "NO_OF_CONT": "VOLUME",   "CONTRACTS": "VOLUME",
        "NO_OF_CONTRACTS": "VOLUME", "TRADED_QTY": "VOLUME",
        "TTL_TRADG_VOL": "VOLUME",
    }
    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})
    if "VOLUME" not in df.columns:
        df["VOLUME"] = float("nan")
    df["EXPIRY_DT"] = pd.to_datetime(df["EXPIRY_DT"], dayfirst=True, errors="coerce")
    df["OPT_TYPE"]  = df["OPT_TYPE"].astype(str).str.strip().str.upper()
    for c in ("STRIKE","OPEN","HIGH","LOW","CLOSE","SETTLE","OI","VOLUME"):
        if c in df.columns:
            df[c] = _to_num(df[c])
    desired = ["EXPIRY_DT","OPT_TYPE","STRIKE","OPEN","HIGH","LOW","CLOSE","SETTLE","OI","VOLUME"]
    return df[[c for c in desired if c in df.columns]]


def _parse_fo_new(raw):
    raw = raw.reset_index(drop=True)
    raw.columns = raw.columns.str.strip()
    inst_col = next((c for c in raw.columns
                     if c.upper() in ("FININSTRMTP","FININSTRMTYPE","FIN_INSTRM_TP")), None)
    sym_col  = next((c for c in raw.columns
                     if c.upper() in ("TCKRSYMB","TCKR_SYMB","SYMBOL")), None)
    if inst_col is None or sym_col is None:
        return None
    inst_vals = raw[inst_col].astype(str).str.strip().str.upper()
    sym_vals  = raw[sym_col].astype(str).str.strip().str.upper()
    inst_mask = inst_vals.isin(["IDO", "IO", "OPTIDX"])
    sym_mask  = sym_vals == "NIFTY"
    df = raw[inst_mask & sym_mask].copy().reset_index(drop=True)
    if df.empty:
        return None
    col_upper = {c.upper(): c for c in df.columns}
    def gc(options):
        for o in options:
            if o.upper() in col_upper:
                return col_upper[o.upper()]
        return None
    expiry_col = gc(["XpryDt","EXPIRY_DT","ExpiryDate"])
    optn_col   = gc(["OptnTp","OPTION_TYP","OptionType","CE_PE"])
    strike_col = gc(["StrkPric","STRIKE_PR","StrikePrice"])
    open_col   = gc(["OpnPric","OPEN","OpenPrice"])
    high_col   = gc(["HghPric","HIGH","HighPrice"])
    low_col    = gc(["LwPric","LOW","LowPrice"])
    close_col  = gc(["ClsPric","CLOSE","ClosePrice"])
    settle_col = gc(["SttlmPric","SETTLE_PR","SettlementPrice"])
    oi_col     = gc(["OpnIntrst","OPEN_INT","OpenInterest"])
    vol_col    = gc(["TtlTradgVol","NO_OF_CONT","Volume","CONTRACTS"])
    result = pd.DataFrame()
    result["EXPIRY_DT"] = pd.to_datetime(df[expiry_col], errors="coerce") if expiry_col else pd.NaT
    result["OPT_TYPE"]  = df[optn_col].astype(str).str.strip().str.upper() if optn_col else ""
    result["STRIKE"]    = _to_num(df[strike_col]) if strike_col else float("nan")
    result["OPEN"]      = _to_num(df[open_col])   if open_col   else float("nan")
    result["HIGH"]      = _to_num(df[high_col])   if high_col   else float("nan")
    result["LOW"]       = _to_num(df[low_col])    if low_col    else float("nan")
    result["CLOSE"]     = _to_num(df[close_col])  if close_col  else float("nan")
    result["SETTLE"]    = _to_num(df[settle_col]) if settle_col else float("nan")
    result["OI"]        = _to_num(df[oi_col])     if oi_col     else float("nan")
    result["VOLUME"]    = _to_num(df[vol_col])    if vol_col    else float("nan")
    desired = ["EXPIRY_DT","OPT_TYPE","STRIKE","OPEN","HIGH","LOW","CLOSE","SETTLE","OI","VOLUME"]
    return result[[c for c in desired if c in result.columns]].reset_index(drop=True)


def _fetch_fo_day(session, dt):
    if dt >= FO_CUTOVER:
        url = FO_NEW_URL.format(yyyymmdd=dt.strftime("%Y%m%d"))
    else:
        mon = MON3[dt.month - 1]
        url = FO_OLD_URL.format(yyyy=dt.strftime("%Y"), mon=mon, dd=dt.strftime("%d"))
    resp = _hist_get(session, url)
    if resp is None:
        return None
    try:
        zf  = zipfile.ZipFile(io.BytesIO(resp.content))
        csv = zf.read(zf.namelist()[0])
        raw = pd.read_csv(io.BytesIO(csv), low_memory=False)
    except Exception as e:
        print(f"  [WARN] Could not parse F&O zip for {dt.date()}: {e}")
        return None
    return _parse_fo_new(raw) if dt >= FO_CUTOVER else _parse_fo_old(raw)


def _build_row(idx, fo_df, step):
    row         = dict(idx)
    close_price = idx["close"]
    atm         = _atm(close_price, step)
    row["atm_strike"]  = atm
    row["nifty_close"] = close_price
    row["expiry"]      = None
    for i in range(6):
        row[f"CE_ATM+{i}_strike"] = atm + i * step
        row[f"CE_ATM+{i}_close"]  = None
        row[f"CE_ATM+{i}_oi"]     = None
        row[f"PE_ATM-{i}_strike"] = atm - i * step
        row[f"PE_ATM-{i}_close"]  = None
        row[f"PE_ATM-{i}_oi"]     = None
    if fo_df is None or fo_df.empty:
        return row
    expiries = fo_df["EXPIRY_DT"].dropna().unique()
    future   = sorted([e for e in expiries if pd.Timestamp(e).date() >= idx["date"]])
    if not future:
        return row
    nearest       = future[0]
    row["expiry"] = pd.Timestamp(nearest).date()
    day_df        = fo_df[fo_df["EXPIRY_DT"] == nearest].copy()
    def _opt(opt_type, strike):
        sub = day_df[(day_df["OPT_TYPE"] == opt_type) & (day_df["STRIKE"] == float(strike))]
        if sub.empty:
            return None, None
        r     = sub.iloc[0]
        close = r["CLOSE"] if pd.notna(r.get("CLOSE")) else r.get("SETTLE")
        oi    = r.get("OI")
        return (float(close) if pd.notna(close) else None,
                float(oi)    if pd.notna(oi)    else None)
    for i in range(6):
        ce_close, ce_oi = _opt("CE", atm + i * step)
        pe_close, pe_oi = _opt("PE", atm - i * step)
        row[f"CE_ATM+{i}_close"] = ce_close
        row[f"CE_ATM+{i}_oi"]    = ce_oi
        row[f"PE_ATM-{i}_close"] = pe_close
        row[f"PE_ATM-{i}_oi"]    = pe_oi
    return row


def fetch_all(start_date=START_DATE, end_date=END_DATE):
    start = _parse_date(start_date)
    end   = _parse_date(end_date)
    print(f"\n{'='*60}")
    print(f"  NIFTY 50 Index + Options Fetcher")
    print(f"  Range  : {start.date()} → {end.date()}")
    print(f"  Output : {OUTPUT_FILE}")
    print(f"  Format : 1 row/day | CE ATM..ATM+5 | PE ATM..ATM-5")
    print(f"{'='*60}\n")
    session = requests.Session()
    rows, fetched, skipped = [], 0, 0
    current = start
    while current <= end:
        if current.weekday() >= 5:
            current += timedelta(days=1)
            continue
        idx = _fetch_index_day(session, current)
        time.sleep(RETRY_DELAY)
        if idx is None:
            skipped += 1
            current += timedelta(days=1)
            continue
        fo_df = _fetch_fo_day(session, current)
        time.sleep(RETRY_DELAY)
        row = _build_row(idx, fo_df, NIFTY_STRIKE_STEP)
        rows.append(row)
        fetched += 1
        if fetched % 25 == 0 or fetched == 1:
            opt_ok = "✓ options" if fo_df is not None else "✗ no options"
            print(f"  [{current.date()}]  rows={fetched}  {opt_ok}")
        current += timedelta(days=1)
    print(f"\n  Done — {fetched} trading days fetched | {skipped} holidays skipped.\n")
    ce_cols, pe_cols = [], []
    for i in range(6):
        ce_cols += [f"CE_ATM+{i}_strike", f"CE_ATM+{i}_close", f"CE_ATM+{i}_oi"]
        pe_cols += [f"PE_ATM-{i}_strike", f"PE_ATM-{i}_close", f"PE_ATM-{i}_oi"]
    col_order = (["date","open","high","low","close","volume","atm_strike","nifty_close","expiry"]
                 + ce_cols + pe_cols)
    df        = pd.DataFrame(rows)
    col_order = [c for c in col_order if c in df.columns]
    df        = df[col_order]
    df["date"] = pd.to_datetime(df["date"])
    if "expiry" in df.columns:
        df["expiry"] = pd.to_datetime(df["expiry"])
    return df.sort_values("date").reset_index(drop=True)


# ═══════════════════════════════════════════════════════════════════════════════
#  LIVE DATA FUNCTIONS  (unchanged from original)
# ═══════════════════════════════════════════════════════════════════════════════

def _live_session(verbose=True):
    """
    Build a requests.Session that reliably passes NSE's Akamai bot-check.

    Cookie warm-up sequence (ALL steps required):
      Step 1 → GET /                          sets: bm_sz  (Akamai bot-score cookie)
      Step 2 → GET /market-data-pre-open/…    navigates to a market data sub-page
      Step 3 → GET /option-chain              sets: nsit   (NSE session token)
      Step 4 → GET /get-quotes/derivatives/…  secondary page visit
      Step 5 → XHR GET /api/marketStatus      sets: nseappid (NSE app token)
      Step 6 → XHR GET /api/allIndices        validation check

    Retries up to 4 times with increasing delays on failure.
    Works both during market hours and post-close.
    """
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    def _attempt(attempt_num):
        s        = requests.Session()
        s.verify = False
        # Use a realistic Chrome-like header set
        s.headers.update({
            "User-Agent"               : _NSE_UA,
            "Accept"                   : ("text/html,application/xhtml+xml,application/xml;"
                                          "q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,"
                                          "application/signed-exchange;v=b3;q=0.7"),
            "Accept-Language"          : "en-IN,en-GB;q=0.9,en-US;q=0.8,en;q=0.7",
            "Accept-Encoding"          : "gzip, deflate, br",
            "Connection"               : "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Ch-Ua"                : '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
            "Sec-Ch-Ua-Mobile"         : "?0",
            "Sec-Ch-Ua-Platform"       : '"Windows"',
            "Sec-Fetch-Dest"           : "document",
            "Sec-Fetch-Mode"           : "navigate",
            "Sec-Fetch-Site"           : "none",
            "Sec-Fetch-User"           : "?1",
            "Cache-Control"            : "max-age=0",
            "DNT"                      : "1",
        })

        def _get(url, timeout=LIVE_TIMEOUT, is_xhr=False):
            try:
                if is_xhr:
                    s.headers.update({
                        "Accept"          : "application/json, text/plain, */*",
                        "Sec-Fetch-Dest"  : "empty",
                        "Sec-Fetch-Mode"  : "cors",
                        "Sec-Fetch-Site"  : "same-origin",
                        "X-Requested-With": "XMLHttpRequest",
                    })
                else:
                    s.headers.update({
                        "Accept"         : ("text/html,application/xhtml+xml,application/xml;"
                                            "q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8"),
                        "Sec-Fetch-Dest" : "document",
                        "Sec-Fetch-Mode" : "navigate",
                        "Sec-Fetch-Site" : "none",
                    })
                r = s.get(url, timeout=timeout, verify=False,
                          allow_redirects=True)
                return r
            except Exception:
                return None

        # Step 1: NSE homepage — Akamai sets bm_sz / ak_bmsc cookies
        r1 = _get(NSE_BASE)
        if r1 is None:
            return None, "homepage unreachable"
        time.sleep(2.2 + attempt_num * 0.5)

        # Step 2: Market-data pre-open page (simulates real user navigation)
        _get(f"{NSE_BASE}/market-data-pre-open/nse/equities-market")
        time.sleep(1.5)

        # Step 3: Option-chain HTML page — sets nsit session token
        r3 = _get(f"{NSE_BASE}/option-chain")
        if r3 is None:
            return None, "option-chain page unreachable"
        # Verify nsit cookie was set
        nsit = s.cookies.get("nsit", "")
        if not nsit:
            # Try the derivatives quotes page as alternative nsit setter
            _get(f"{NSE_BASE}/get-quotes/derivatives/future")
            time.sleep(1.5)
            nsit = s.cookies.get("nsit", "")
        time.sleep(2.0)

        # Step 4: Switch to XHR mode and hit marketStatus — sets nseappid
        r4 = _get(URL_MARKET_STATUS, is_xhr=True)
        if r4 is not None and r4.status_code in (401, 403):
            # Akamai rejected — do a full second warm-up pass
            s.headers.update({
                "Accept"         : ("text/html,application/xhtml+xml,application/xml;"
                                    "q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8"),
                "Sec-Fetch-Dest" : "document",
                "Sec-Fetch-Mode" : "navigate",
                "Sec-Fetch-Site" : "none",
            })
            _get(NSE_BASE)
            time.sleep(3.0)
            _get(f"{NSE_BASE}/option-chain")
            time.sleep(2.5)
            r4 = _get(URL_MARKET_STATUS, is_xhr=True)

        time.sleep(1.2)

        # Step 5: Validate — allIndices JSON endpoint
        r_test = _get(URL_ALL_INDICES, is_xhr=True)
        if r_test is not None and r_test.status_code == 200:
            ct = r_test.headers.get("Content-Type", "")
            if "json" in ct or r_test.text.strip().startswith("{"):
                return s, "ok"

        # allIndices can return HTML post-market; try option-chain API as fallback
        r_oc = _get(URL_OPTION_CHAIN, is_xhr=True)
        if r_oc is not None and r_oc.status_code == 200:
            ct = r_oc.headers.get("Content-Type", "")
            if "json" in ct or r_oc.text.strip().startswith("{"):
                return s, "ok-via-oc"

        sc = getattr(r_test, "status_code", "?")
        return s, f"session-weak (nsit={'set' if nsit else 'missing'}, indices={sc})"

    MAX_ATTEMPTS = 4
    for attempt in range(1, MAX_ATTEMPTS + 1):
        if verbose:
            print(f"    [session] Warm-up attempt {attempt}/{MAX_ATTEMPTS} …", flush=True)
        session, status = _attempt(attempt)
        if session is not None and status.startswith("ok"):
            if verbose:
                print(f"    [session] Ready  ({status})", flush=True)
            return session
        if verbose:
            print(f"    [session] {status} — retrying in {attempt * 4}s …", flush=True)
        if attempt < MAX_ATTEMPTS:
            time.sleep(attempt * 4)

    # Return whatever session we have — caller will handle None data gracefully
    if verbose:
        print("    [session] Warning: session may be degraded; proceeding anyway.", flush=True)
    return session


def _safe_json(r):
    if r is None:
        return None
    ct = r.headers.get("Content-Type", "")
    if "text/html" in ct and "json" not in ct:
        return None
    try:
        return r.json()
    except Exception:
        try:
            import json as _json
            return _json.loads(r.text)
        except Exception:
            return None


def _live_get(session, url, retries=4):
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    def _rewarm(s):
        """Full multi-step rewarm to re-establish NSE Akamai cookies."""
        try:
            # Restore page-load headers
            s.headers.update({
                "Accept"         : ("text/html,application/xhtml+xml,application/xml;"
                                    "q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8"),
                "Sec-Fetch-Dest" : "document",
                "Sec-Fetch-Mode" : "navigate",
                "Sec-Fetch-Site" : "none",
            })
            s.get(NSE_BASE, timeout=LIVE_TIMEOUT, verify=False)
            time.sleep(2.5)
            s.get(f"{NSE_BASE}/market-data-pre-open/nse/equities-market",
                  timeout=LIVE_TIMEOUT, verify=False)
            time.sleep(1.5)
            s.get(f"{NSE_BASE}/option-chain", timeout=LIVE_TIMEOUT, verify=False)
            time.sleep(2.0)
            # Switch back to XHR headers
            s.headers.update({
                "Accept"          : "application/json, text/plain, */*",
                "Sec-Fetch-Dest"  : "empty",
                "Sec-Fetch-Mode"  : "cors",
                "Sec-Fetch-Site"  : "same-origin",
                "X-Requested-With": "XMLHttpRequest",
            })
            s.get(URL_MARKET_STATUS, timeout=LIVE_TIMEOUT, verify=False)
            time.sleep(1.0)
        except Exception:
            pass

    # Ensure XHR headers are set before the first attempt
    session.headers.update({
        "Accept"          : "application/json, text/plain, */*",
        "Referer"         : "https://www.nseindia.com/option-chain",
        "X-Requested-With": "XMLHttpRequest",
        "Sec-Fetch-Dest"  : "empty",
        "Sec-Fetch-Mode"  : "cors",
        "Sec-Fetch-Site"  : "same-origin",
    })

    for attempt in range(retries):
        try:
            r = session.get(url, timeout=LIVE_TIMEOUT, verify=False)
            if r.status_code == 200:
                data = _safe_json(r)
                if data is not None:
                    return data
                # Got 200 but HTML (Akamai challenge page) — rewarm
                _rewarm(session)
            elif r.status_code in (401, 403):
                _rewarm(session)
            elif r.status_code == 429:
                wait = 8.0 + attempt * 4
                time.sleep(wait)
                continue
            elif r.status_code == 503:
                time.sleep(5.0)
        except requests.exceptions.SSLError:
            session.verify = False
        except (requests.exceptions.Timeout,
                requests.exceptions.ConnectionError):
            pass
        except Exception:
            pass
        time.sleep(3.0 * (attempt + 1))
    return None


def fetch_market_status(session=None):
    s    = session or _live_session()
    data = _live_get(s, URL_MARKET_STATUS)
    if not data:
        return {"status": "unknown", "error": "NSE API unreachable"}
    markets = data.get("marketState", [])
    if not markets:
        return {"status": "unknown", "error": "Empty marketState"}
    def _extract(m):
        return {
            "market"    : m.get("market",              ""),
            "status"    : m.get("marketStatus",        "unknown"),
            "trade_date": m.get("tradeDate",           ""),
            "index"     : m.get("index",               ""),
            "last"      : m.get("last",                0),
            "variation" : m.get("variation",           0),
            "pct_change": m.get("percentChange",       0),
            "message"   : m.get("marketStatusMessage", ""),
        }
    for m in markets:
        if "capital" in m.get("market", "").lower():
            return _extract(m)
    for m in markets:
        if "normal" in m.get("marketType", "").lower():
            return _extract(m)
    return _extract(markets[0])


def fetch_live_index(session=None):
    s    = session or _live_session()
    data = _live_get(s, URL_ALL_INDICES)
    if not data:
        return None
    nifty = None
    for x in data.get("data", []):
        name = (x.get("index") or x.get("indexSymbol") or
                x.get("indexName") or "").upper().strip()
        if name == "NIFTY 50":
            nifty = x
            break
    if nifty is None:
        return None
    def _f(v, default=0.0):
        try:
            return float(str(v).replace(",", "")) if v not in (None, "", "-", "N/A") else default
        except (ValueError, TypeError):
            return default
    def _i(v, default=0):
        try:
            return int(str(v).replace(",", "")) if v not in (None, "", "-", "N/A") else default
        except (ValueError, TypeError):
            return default
    ltp  = _f(nifty.get("last") or nifty.get("lastPrice"))
    prev = _f(nifty.get("previousClose") or nifty.get("prevClose"))
    chg  = _f(nifty.get("variation") or nifty.get("change") or (ltp - prev))
    pct  = _f(nifty.get("percentChange") or nifty.get("pChange"))
    return {
        "timestamp" : data.get("timestamp", datetime.now().strftime("%d-%b-%Y %H:%M:%S")),
        "ltp"       : ltp,
        "open"      : _f(nifty.get("open")),
        "high"      : _f(nifty.get("high")),
        "low"       : _f(nifty.get("low")),
        "prev_close": prev,
        "change"    : chg,
        "pct_change": pct,
        "year_high" : _f(nifty.get("yearHigh")),
        "year_low"  : _f(nifty.get("yearLow")),
        "pe"        : nifty.get("pe",  "N/A"),
        "pb"        : nifty.get("pb",  "N/A"),
        "advance"   : _i(nifty.get("advances")),
        "decline"   : _i(nifty.get("declines")),
    }


def fetch_live_options(session=None, otm_range=LIVE_OTM_RANGE, expiry_index=0):
    s    = session or _live_session()
    data = _live_get(s, URL_OPTION_CHAIN)
    if not data:
        return None
    records      = data.get("records", {})
    all_expiries = records.get("expiryDates", [])
    underlying   = float(records.get("underlyingValue", 0) or 0)
    timestamp    = records.get("timestamp", "")
    if not all_expiries:
        return None
    expiry_index  = min(max(expiry_index, 0), len(all_expiries) - 1)
    chosen_expiry = all_expiries[expiry_index]
    atm           = _atm(underlying, NIFTY_STRIKE_STEP)
    strike_min    = atm - otm_range * NIFTY_STRIKE_STEP
    strike_max    = atm + otm_range * NIFTY_STRIKE_STEP
    def _f(v, default=0.0):
        try:
            return float(str(v).replace(",", "")) if v not in (None, "", "-") else default
        except (ValueError, TypeError):
            return default
    def _side(d):
        if not d or not isinstance(d, dict):
            return {}
        return {
            "ltp"       : _f(d.get("lastPrice")),
            "open"      : _f(d.get("openPrice")),
            "high"      : _f(d.get("highPrice")),
            "low"       : _f(d.get("lowPrice")),
            "close"     : _f(d.get("closePrice")),
            "prev_close": _f(d.get("prevClose")),
            "change"    : _f(d.get("change")),
            "pct_change": _f(d.get("pChange")),
            "iv"        : _f(d.get("impliedVolatility")),
            "oi"        : _f(d.get("openInterest")),
            "oi_change" : _f(d.get("changeinOpenInterest")),
            "volume"    : _f(d.get("totalTradedVolume")),
            "bid"       : _f(d.get("bidPrice")),
            "ask"       : _f(d.get("askPrice")),
        }
    rows = []
    for item in records.get("data", []):
        if item.get("expiryDate") != chosen_expiry:
            continue
        strike = float(item.get("strikePrice", 0) or 0)
        if not (strike_min <= strike <= strike_max):
            continue
        diff  = int((strike - atm) // NIFTY_STRIKE_STEP)
        label = "ATM" if diff == 0 else f"ATM{'+' if diff > 0 else ''}{diff}"
        row   = {"strike": strike, "atm_label": label}
        for k, v in _side(item.get("CE")).items():
            row[f"CE_{k}"] = v
        for k, v in _side(item.get("PE")).items():
            row[f"PE_{k}"] = v
        rows.append(row)
    if not rows:
        return None
    chain = pd.DataFrame(rows).sort_values("strike").reset_index(drop=True)
    return {
        "expiry"      : chosen_expiry,
        "all_expiries": all_expiries,
        "atm_strike"  : atm,
        "underlying"  : underlying,
        "timestamp"   : timestamp,
        "chain"       : chain,
    }


def fetch_live_snapshot(otm_range=LIVE_OTM_RANGE, expiry_index=0):
    print("Connecting to NSE ...")
    session = _live_session(verbose=False)
    print("  Fetching market status ...")
    mkt = fetch_market_status(session)
    print("  Fetching NIFTY 50 index quote ...")
    idx = fetch_live_index(session)
    print("  Fetching NIFTY option chain ...")
    opt = fetch_live_options(session, otm_range=otm_range, expiry_index=expiry_index)
    return {"market_status": mkt, "index": idx, "options": opt}


def print_live_snapshot(snap):
    SEP   = "═" * 72
    sep   = "─" * 72
    GREEN = "\033[92m"; YELLOW= "\033[93m"; RED   = "\033[91m"
    RESET = "\033[0m";  BOLD  = "\033[1m"

    mkt = snap.get("market_status") or {}
    idx = snap.get("index")         or {}
    opt = snap.get("options")       or {}

    print(f"\n{SEP}")
    print("  NIFTY 50  —  LIVE SNAPSHOT")
    if idx.get("timestamp"):
        print(f"  As of : {idx['timestamp']}")
    print(SEP)

    raw_status = str(mkt.get("status", "unknown"))
    status_up  = raw_status.upper()
    sc = GREEN if status_up == "OPEN" else (YELLOW if "PRE" in status_up else RED)
    td  = f"   |  Trade Date : {mkt['trade_date']}" if mkt.get("trade_date") else ""
    msg = f"   |  {mkt['message']}"                 if mkt.get("message")    else ""
    print(f"\n  Market : {sc}{status_up}{RESET}{td}{msg}")

    if idx:
        def _f(k, d=0.0):
            try: return float(idx.get(k) or d)
            except: return d
        ltp = _f("ltp"); chg = _f("change"); pct = _f("pct_change")
        arrow = "▲" if chg >= 0 else "▼"
        nc    = GREEN if chg >= 0 else RED
        print(f"\n{sep}")
        print("  INDEX QUOTE")
        print(sep)
        print(f"  LTP        : {nc}{BOLD}{ltp:>10,.2f}{RESET}  "
              f"{nc}{arrow} {chg:+.2f}  ({pct:+.2f}%){RESET}")
        print(f"  Open       : {_f('open'):>10,.2f}    "
              f"High : {_f('high'):>10,.2f}    "
              f"Low  : {_f('low'):>10,.2f}")
        print(f"  Prev Close : {_f('prev_close'):>10,.2f}    "
              f"52W High : {_f('year_high'):>10,.2f}    "
              f"52W Low  : {_f('year_low'):>10,.2f}")
        print(f"  P/E        : {str(idx.get('pe','N/A')):>10}    "
              f"P/B  : {str(idx.get('pb','N/A')):>10}")
        print(f"  Advances   : {str(idx.get('advance',0)):>10}    "
              f"Declines : {str(idx.get('decline',0)):>10}")

    chain_df = opt.get("chain") if isinstance(opt, dict) else None
    if isinstance(chain_df, pd.DataFrame) and not chain_df.empty:
        expiry = opt.get("expiry", "")
        atm    = opt.get("atm_strike", 0)
        ul     = float(opt.get("underlying", 0) or 0)
        print(f"\n{sep}")
        print(f"  OPTION CHAIN  |  Expiry : {expiry}  |  ATM : {atm}  |  Spot : {ul:,.2f}")
        print(sep)
        print(f"  {'CE LTP':>9} {'CE OI':>11} {'CE IV%':>7}  "
              f"{'STRIKE':^9}  "
              f"{'PE LTP':>9} {'PE OI':>11} {'PE IV%':>7}  LABEL")
        print(f"  {'-'*9} {'-'*11} {'-'*7}  {'-'*9}  "
              f"{'-'*9} {'-'*11} {'-'*7}  {'-'*9}")
        for _, row in chain_df.iterrows():
            strike = int(float(row.get("strike", 0) or 0))
            is_atm = (strike == atm)
            pfx    = f"\033[1m►" if is_atm else " "
            sfx    = "\033[0m"   if is_atm else ""
            def _rv(col, d=0.0):
                try: return float(row.get(col) or d)
                except: return d
            print(f" {pfx}"
                  f"{_rv('CE_ltp'):>9,.2f} "
                  f"{_rv('CE_oi'):>11,.0f} "
                  f"{_rv('CE_iv'):>6.1f}%  "
                  f"{strike:^9}  "
                  f"{_rv('PE_ltp'):>9,.2f} "
                  f"{_rv('PE_oi'):>11,.0f} "
                  f"{_rv('PE_iv'):>6.1f}%  "
                  f"{row.get('atm_label','')}{sfx}")
        ce_oi = float(chain_df.get("CE_oi", pd.Series(dtype=float)).fillna(0).sum())
        pe_oi = float(chain_df.get("PE_oi", pd.Series(dtype=float)).fillna(0).sum())
        pcr   = pe_oi / ce_oi if ce_oi > 0 else 0.0
        print(f"\n  PCR (OI)     : {pcr:.3f}    "
              f"Total CE OI : {ce_oi:>12,.0f}    "
              f"Total PE OI : {pe_oi:>12,.0f}")
        all_exp = opt.get("all_expiries", [])
        if all_exp:
            print(f"  All Expiries : {', '.join(all_exp[:8])}")
    elif not idx:
        print(f"\n\033[91m  [!] Could not fetch data from NSE.\033[0m")
        print(  "      Possible reasons:")
        print(  "        • Market is closed / outside trading hours")
        print(  "        • NSE website is down or under maintenance")
        print(  "        • Your IP is rate-limited by NSE (wait 30s and retry)")
        print(  "        • Network/firewall blocking nseindia.com")
    print(f"\n{SEP}\n")


def save_live_snapshot(snap, filename="nifty_live_snapshot.csv"):
    opt = snap.get("options")
    idx = snap.get("index", {})
    if not opt or not isinstance(opt.get("chain"), pd.DataFrame) or opt["chain"].empty:
        print("  No option data to save.")
        return
    chain = opt["chain"].copy()
    chain.insert(0, "timestamp",  idx.get("timestamp", ""))
    chain.insert(1, "nifty_ltp",  idx.get("ltp",  0))
    chain.insert(2, "nifty_open", idx.get("open", 0))
    chain.insert(3, "nifty_high", idx.get("high", 0))
    chain.insert(4, "nifty_low",  idx.get("low",  0))
    chain.insert(5, "expiry",     opt.get("expiry", ""))
    chain.insert(6, "atm_strike", opt.get("atm_strike", 0))
    chain.insert(7, "underlying", opt.get("underlying", 0))
    chain.to_csv(filename, index=False)
    print(f"  Saved {len(chain)} rows → {filename}")


def _run_live_cli(args):
    iteration = 0
    while True:
        iteration += 1
        if args.watch and iteration > 1:
            os.system("cls" if os.name == "nt" else "clear")
        snap = fetch_live_snapshot(otm_range=args.otm, expiry_index=args.expiry)
        print_live_snapshot(snap)
        if args.save:
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            save_live_snapshot(snap, f"nifty_live_{ts}.csv")
        if not args.watch:
            break
        print(f"  Refreshing in {args.watch}s ...  (Ctrl+C to stop)\n")
        try:
            time.sleep(args.watch)
        except KeyboardInterrupt:
            print("\n  Stopped.")
            break


# ═══════════════════════════════════════════════════════════════════════════════
#  5-MINUTE INTRADAY CANDLE ENGINE
# ═══════════════════════════════════════════════════════════════════════════════

class CandleBuilder:
    """
    Accumulates tick-level LTP samples into OHLCV candles aligned to
    candle boundaries (09:15, 09:20, 09:25 ... 15:25, 15:30).

    A candle is considered "closed" the first time a tick arrives whose
    wall-clock time falls in the NEXT candle bucket.  At that moment the
    closed candle is returned and a fresh one started.
    """

    def __init__(self, interval_minutes=CANDLE_MINUTES):
        self.interval   = interval_minutes
        self._candle    = None           # dict with open/high/low/close/volume
        self._bucket    = None           # datetime of current candle open

    # ── public ─────────────────────────────────────────────────────────────

    def push(self, ltp: float, ts: datetime, volume: float = 0.0):
        """
        Feed one tick.  Returns a closed candle dict if the previous
        candle just closed, otherwise returns None.
        """
        bucket = self._align(ts)
        closed = None

        if self._bucket is None:
            # first tick ever — open the first candle
            self._open_candle(bucket, ltp, volume)

        elif bucket != self._bucket:
            # we've moved to a new candle bucket → close the old one
            closed = self._close_candle()
            self._open_candle(bucket, ltp, volume)

        else:
            # same candle — update OHLCV
            self._candle["high"]   = max(self._candle["high"],  ltp)
            self._candle["low"]    = min(self._candle["low"],   ltp)
            self._candle["close"]  = ltp
            self._candle["volume"] += volume
            self._candle["ticks"]  += 1

        return closed

    def current(self):
        """Return a *copy* of the in-progress (open) candle, or None."""
        return dict(self._candle) if self._candle else None

    def force_close(self):
        """Force-close and return the current open candle (e.g. at 15:30)."""
        if self._candle:
            return self._close_candle()
        return None

    # ── private ────────────────────────────────────────────────────────────

    def _align(self, ts: datetime) -> datetime:
        """Round down ts to the nearest candle boundary."""
        base    = ts.replace(hour=MARKET_OPEN_H, minute=MARKET_OPEN_M,
                             second=0, microsecond=0)
        elapsed = int((ts - base).total_seconds() // 60)
        bucket_offset = (elapsed // self.interval) * self.interval
        return base + timedelta(minutes=bucket_offset)

    def _open_candle(self, bucket, ltp, volume):
        self._bucket = bucket
        self._candle = {
            "candle_open_time"  : bucket,
            "candle_close_time" : bucket + timedelta(minutes=self.interval),
            "open"  : ltp,
            "high"  : ltp,
            "low"   : ltp,
            "close" : ltp,
            "volume": volume,
            "ticks" : 1,
        }

    def _close_candle(self):
        c = dict(self._candle)
        self._candle = None
        self._bucket = None
        return c


# ─── Flat CSV row builder ──────────────────────────────────────────────────────

def _flatten_candle_with_options(candle: dict, idx: dict, opt: dict) -> dict:
    """
    Merge a closed candle dict + live index quote + option chain into
    one flat dict suitable for a CSV row.
    """
    row = {
        # ── candle columns ──────────────────────────────────────────────
        "candle_open_time"  : candle.get("candle_open_time", ""),
        "candle_close_time" : candle.get("candle_close_time", ""),
        "candle_open"       : candle.get("open",   0),
        "candle_high"       : candle.get("high",   0),
        "candle_low"        : candle.get("low",    0),
        "candle_close"      : candle.get("close",  0),
        "candle_volume"     : candle.get("volume", 0),
        "candle_ticks"      : candle.get("ticks",  0),
        # ── live index snapshot at candle close ──────────────────────────
        "nse_timestamp"     : (idx or {}).get("timestamp", ""),
        "nifty_ltp"         : (idx or {}).get("ltp",        0),
        "nifty_day_open"    : (idx or {}).get("open",       0),
        "nifty_day_high"    : (idx or {}).get("high",       0),
        "nifty_day_low"     : (idx or {}).get("low",        0),
        "nifty_prev_close"  : (idx or {}).get("prev_close", 0),
        "nifty_change"      : (idx or {}).get("change",     0),
        "nifty_pct_change"  : (idx or {}).get("pct_change", 0),
        # ── options meta ─────────────────────────────────────────────────
        "expiry"            : (opt or {}).get("expiry",      ""),
        "atm_strike"        : (opt or {}).get("atm_strike",  0),
        "underlying"        : (opt or {}).get("underlying",  0),
    }

    # ── option chain: one set of CE/PE columns per strike ─────────────────
    chain_df = (opt or {}).get("chain")
    if isinstance(chain_df, pd.DataFrame) and not chain_df.empty:
        for _, r in chain_df.iterrows():
            strike = int(float(r.get("strike", 0) or 0))
            label  = str(r.get("atm_label", strike)).replace("+", "p").replace("-", "m")
            for side in ("CE", "PE"):
                for field in ("ltp", "open", "high", "low", "close",
                              "oi", "oi_change", "volume", "iv", "bid", "ask",
                              "change", "pct_change"):
                    col = f"{side}_{label}_{field}"
                    row[col] = float(r.get(f"{side}_{field}", 0) or 0)

    return row


# ─── Pretty printer for a candle close ───────────────────────────────────────

def _print_candle(candle: dict, opt: dict):
    SEP   = "═" * 78
    sep   = "─" * 78
    GREEN = "\033[92m"; RED = "\033[91m"; BOLD = "\033[1m"; RESET = "\033[0m"
    CYAN  = "\033[96m"; YELLOW = "\033[93m"

    ot = candle.get("candle_open_time",  "")
    ct = candle.get("candle_close_time", "")
    if isinstance(ot, datetime): ot = ot.strftime("%H:%M")
    if isinstance(ct, datetime): ct = ct.strftime("%H:%M")

    o = candle.get("open",  0)
    h = candle.get("high",  0)
    l = candle.get("low",   0)
    c = candle.get("close", 0)
    v = candle.get("volume", 0)

    chg   = c - o
    color = GREEN if chg >= 0 else RED
    arrow = "▲" if chg >= 0 else "▼"

    print(f"\n{SEP}")
    print(f"  {BOLD}{CYAN}5-MIN CANDLE CLOSED{RESET}   "
          f"{BOLD}{ot} → {ct}{RESET}")
    print(SEP)
    print(f"  O:{o:>9,.2f}  H:{h:>9,.2f}  L:{l:>9,.2f}  "
          f"C:{color}{BOLD}{c:>9,.2f}{RESET}  "
          f"{color}{arrow} {chg:+.2f}{RESET}")
    if v:
        print(f"  Vol: {v:,.0f}  |  Ticks in candle: {candle.get('ticks',0)}")

    chain_df = (opt or {}).get("chain") if isinstance(opt, dict) else None
    if isinstance(chain_df, pd.DataFrame) and not chain_df.empty:
        expiry = (opt or {}).get("expiry", "")
        atm    = (opt or {}).get("atm_strike", 0)
        ul     = float((opt or {}).get("underlying", 0) or 0)
        print(f"\n{sep}")
        print(f"  {YELLOW}OPTION CHAIN at CANDLE CLOSE{RESET}  "
              f"|  Expiry: {expiry}  |  ATM: {atm}  |  Spot: {ul:,.2f}")
        print(sep)
        print(f"  {'CE LTP':>9} {'CE OI':>11} {'CE IV%':>7}  "
              f"{'STRIKE':^9}  "
              f"{'PE LTP':>9} {'PE OI':>11} {'PE IV%':>7}  LABEL")
        print(f"  {'-'*9} {'-'*11} {'-'*7}  {'-'*9}  "
              f"{'-'*9} {'-'*11} {'-'*7}  {'-'*9}")
        for _, row in chain_df.iterrows():
            strike = int(float(row.get("strike", 0) or 0))
            is_atm = (strike == atm)
            pfx    = f"{BOLD}►" if is_atm else " "
            sfx    = RESET      if is_atm else ""
            def _rv(col, d=0.0):
                try: return float(row.get(col) or d)
                except: return d
            print(f" {pfx}"
                  f"{_rv('CE_ltp'):>9,.2f} "
                  f"{_rv('CE_oi'):>11,.0f} "
                  f"{_rv('CE_iv'):>6.1f}%  "
                  f"{strike:^9}  "
                  f"{_rv('PE_ltp'):>9,.2f} "
                  f"{_rv('PE_oi'):>11,.0f} "
                  f"{_rv('PE_iv'):>6.1f}%  "
                  f"{row.get('atm_label','')}{sfx}")
        ce_oi = float(chain_df.get("CE_oi", pd.Series(dtype=float)).fillna(0).sum())
        pe_oi = float(chain_df.get("PE_oi", pd.Series(dtype=float)).fillna(0).sum())
        pcr   = pe_oi / ce_oi if ce_oi > 0 else 0.0
        print(f"\n  PCR(OI): {pcr:.3f}   "
              f"CE OI: {ce_oi:>12,.0f}   PE OI: {pe_oi:>12,.0f}")
    print(f"{SEP}\n")


# ─── Save / append a candle row to CSV ────────────────────────────────────────

def _save_candle_row(row: dict, filename: str):
    df     = pd.DataFrame([row])
    exists = os.path.isfile(filename)
    df.to_csv(filename, mode="a", header=not exists, index=False)
    print(f"  [CSV] Appended candle → {filename}  (total rows: "
          f"{sum(1 for _ in open(filename)) - 1})")


# ─── Market-hours guard ───────────────────────────────────────────────────────

def _market_open_now() -> bool:
    """
    True if current IST wall-clock time is within NSE equity market hours
    (09:15:00 inclusive → 15:30:00 inclusive).
    Weekends (Sat/Sun) always return False.
    """
    now = datetime.now()
    if now.weekday() >= 5:          # 0=Mon … 4=Fri, 5=Sat, 6=Sun
        return False
    open_t  = now.replace(hour=MARKET_OPEN_H,  minute=MARKET_OPEN_M,
                          second=0, microsecond=0)
    close_t = now.replace(hour=MARKET_CLOSE_H, minute=MARKET_CLOSE_M,
                          second=0, microsecond=0)
    return open_t <= now <= close_t


def _seconds_until_market_open() -> float:
    """
    Seconds from now until next market open (09:15 IST).
    Correctly handles:
      • Same-day pre-open  (now < today 09:15 and it is a weekday)
      • Post-close / weekend -> finds the next weekday 09:15
    """
    now        = datetime.now()
    today_open = now.replace(hour=MARKET_OPEN_H, minute=MARKET_OPEN_M,
                             second=0, microsecond=0)
    # same-day pre-open on a weekday
    if now.weekday() < 5 and now < today_open:
        return (today_open - now).total_seconds()
    # post-close or weekend: find the next weekday
    days_ahead = 1
    while True:
        candidate = (now + timedelta(days=days_ahead)).replace(
            hour=MARKET_OPEN_H, minute=MARKET_OPEN_M, second=0, microsecond=0)
        if candidate.weekday() < 5:
            return (candidate - now).total_seconds()
        days_ahead += 1


# ─── Clock-aligned scheduler helpers ─────────────────────────────────────────

def _next_15min_boundary(now: datetime) -> datetime:
    """
    Return the next strict 15-min candle close aligned to the 09:15 grid.

    Grid: 09:15, 09:30, 09:45, 10:00, 10:15 … 15:15, 15:30
    Always returns a time STRICTLY AFTER `now`.
    Capped at 15:30 — never returns 15:45 or later.

    Examples (CANDLE_MINUTES = 15):
      09:14:59  →  09:30:00
      09:15:00  →  09:30:00
      09:30:00  →  09:45:00
      12:00:00  →  12:15:00
      15:14:59  →  15:30:00
      15:29:59  →  15:30:00
      15:30:00  →  15:30:00   (already at final boundary)
    """
    base     = now.replace(hour=MARKET_OPEN_H, minute=MARKET_OPEN_M,
                            second=0, microsecond=0)
    close_t  = now.replace(hour=MARKET_CLOSE_H, minute=MARKET_CLOSE_M,
                            second=0, microsecond=0)
    interval = CANDLE_MINUTES * 60
    elapsed  = (now - base).total_seconds()          # may be negative pre-open

    if elapsed < 0:
        # before market open — first boundary is base + interval
        return base + timedelta(seconds=interval)

    # how many complete intervals fit in elapsed?
    n_complete = int(elapsed // interval)
    # boundary that ends the current (or most recent) interval
    current_boundary = base + timedelta(seconds=(n_complete + 1) * interval)

    # if now is exactly ON a boundary we still want the NEXT one
    if now >= current_boundary:
        current_boundary += timedelta(seconds=interval)

    # cap: never go past 15:30
    return min(current_boundary, close_t)


def _sleep_until(target: datetime):
    """
    Sleep precisely until `target` wall-clock time.
    Wakes up in small chunks so KeyboardInterrupt is still responsive.
    """
    while True:
        remaining = (target - datetime.now()).total_seconds()
        if remaining <= 0:
            return
        time.sleep(min(remaining, 5.0))   # wake every ≤5 s to stay responsive


# ─── Pretty printer updated for 15-min label ─────────────────────────────────

def _print_candle_15(candle: dict, opt: dict):
    """Same as _print_candle but labels say '15-MIN CANDLE'."""
    SEP   = "═" * 78
    sep   = "─" * 78
    GREEN = "\033[92m"; RED = "\033[91m"; BOLD = "\033[1m"; RESET = "\033[0m"
    CYAN  = "\033[96m"; YELLOW = "\033[93m"

    ot = candle.get("candle_open_time",  "")
    ct = candle.get("candle_close_time", "")
    if isinstance(ot, datetime): ot = ot.strftime("%H:%M")
    if isinstance(ct, datetime): ct = ct.strftime("%H:%M")

    o = candle.get("open",  0);  h = candle.get("high",  0)
    l = candle.get("low",   0);  c = candle.get("close", 0)
    v = candle.get("volume", 0)

    chg   = c - o
    color = GREEN if chg >= 0 else RED
    arrow = "▲" if chg >= 0 else "▼"

    print(f"\n{SEP}")
    print(f"  {BOLD}{CYAN}15-MIN CANDLE CLOSED{RESET}   "
          f"{BOLD}{ot} → {ct}{RESET}")
    print(SEP)
    print(f"  O:{o:>9,.2f}  H:{h:>9,.2f}  L:{l:>9,.2f}  "
          f"C:{color}{BOLD}{c:>9,.2f}{RESET}  "
          f"{color}{arrow} {chg:+.2f}{RESET}")
    if v:
        print(f"  Vol: {v:,.0f}  |  Ticks in candle: {candle.get('ticks', 0)}")

    chain_df = (opt or {}).get("chain") if isinstance(opt, dict) else None
    if isinstance(chain_df, pd.DataFrame) and not chain_df.empty:
        expiry = (opt or {}).get("expiry", "")
        atm    = (opt or {}).get("atm_strike", 0)
        ul     = float((opt or {}).get("underlying", 0) or 0)
        print(f"\n{sep}")
        print(f"  {YELLOW}OPTION CHAIN at 15-MIN CANDLE CLOSE{RESET}  "
              f"|  Expiry: {expiry}  |  ATM: {atm}  |  Spot: {ul:,.2f}")
        print(sep)
        print(f"  {'CE LTP':>9} {'CE OI':>11} {'CE IV%':>7}  "
              f"{'STRIKE':^9}  "
              f"{'PE LTP':>9} {'PE OI':>11} {'PE IV%':>7}  LABEL")
        print(f"  {'-'*9} {'-'*11} {'-'*7}  {'-'*9}  "
              f"{'-'*9} {'-'*11} {'-'*7}  {'-'*9}")
        for _, row in chain_df.iterrows():
            strike = int(float(row.get("strike", 0) or 0))
            is_atm = (strike == atm)
            pfx    = f"\033[1m►" if is_atm else " "
            sfx    = "\033[0m"   if is_atm else ""
            def _rv(col, d=0.0):
                try:    return float(row.get(col) or d)
                except: return d
            print(f" {pfx}"
                  f"{_rv('CE_ltp'):>9,.2f} "
                  f"{_rv('CE_oi'):>11,.0f} "
                  f"{_rv('CE_iv'):>6.1f}%  "
                  f"{strike:^9}  "
                  f"{_rv('PE_ltp'):>9,.2f} "
                  f"{_rv('PE_oi'):>11,.0f} "
                  f"{_rv('PE_iv'):>6.1f}%  "
                  f"{row.get('atm_label', '')}{sfx}")
        ce_oi = float(chain_df.get("CE_oi", pd.Series(dtype=float)).fillna(0).sum())
        pe_oi = float(chain_df.get("PE_oi", pd.Series(dtype=float)).fillna(0).sum())
        pcr   = pe_oi / ce_oi if ce_oi > 0 else 0.0
        print(f"\n  PCR(OI): {pcr:.3f}   "
              f"CE OI: {ce_oi:>12,.0f}   PE OI: {pe_oi:>12,.0f}")
    print(f"{SEP}\n")


# ─── Main intraday loop ───────────────────────────────────────────────────────

def _run_intraday_cli(args):
    """
    15-minute candle engine — clock-aligned trigger.

    How it works
    ────────────
    1. Waits until the next 15-min boundary aligned to the 09:15 NSE grid
       (09:15, 09:30, 09:45 … 15:30).
    2. For the 30 seconds BEFORE each boundary, polls NSE every 5 s to
       accumulate fresh OHLCV ticks for the closing candle.
    3. At the boundary: force-closes the candle, fetches the option chain,
       and prints/saves the combined row.
    4. If called when the market is already CLOSED:
       - Fetches the last available 15-min candle + option chain ONCE.
       - Prints the data with a clear "⚠ MARKET CLOSED" banner.
       - Exits immediately (does NOT wait for next open).
    5. Runs until 15:30 candle is emitted, then exits cleanly.

    Trigger grid examples:
      12:00 start → waits to 12:15, fires, waits to 12:30, fires …
      09:14 start → waits to 09:30 (first real close), fires …
    """
    # ── tuning constants ──────────────────────────────────────────────────
    WARMUP_SECONDS = 30    # start polling this many seconds before boundary
    TICK_POLL_S    = 5     # poll interval during warm-up window (seconds)
    SESSION_REWARM = 10    # re-warm NSE session every N candles

    GREEN  = "\033[92m"; RED    = "\033[91m"; RESET  = "\033[0m"
    BOLD   = "\033[1m";  CYAN   = "\033[96m"; YELLOW = "\033[93m"

    otm_range    = args.otm
    expiry_index = args.expiry
    save_csv     = args.save
    csv_file     = INTRADAY_FILE

    print(f"\n{'═'*72}")
    print(f"  NIFTY 50 — 15-MIN INTRADAY CANDLE MODE  (clock-aligned)")
    print(f"  Grid    : 09:15 → 09:30 → 09:45 … 15:30  (every {CANDLE_MINUTES} min)")
    print(f"  OTM     : ATM ± {otm_range} strikes")
    print(f"  Save    : {'YES → ' + csv_file if save_csv else 'NO (pass --save to enable)'}")
    print(f"{'═'*72}\n")

    # ── ── ── ── ── ── ── ── ── ── ── ── ── ── ── ── ── ── ── ── ── ──
    # CASE A: Market is already CLOSED (or weekend)
    #   → fetch last snapshot once, print with "MARKET CLOSED" note, exit.
    # ── ── ── ── ── ── ── ── ── ── ── ── ── ── ── ── ── ── ── ── ── ──
    now       = datetime.now()
    close_t   = now.replace(hour=MARKET_CLOSE_H, minute=MARKET_CLOSE_M,
                            second=0, microsecond=0)
    is_weekend = now.weekday() >= 5
    is_closed  = (now > close_t) or is_weekend

    if is_closed:
        print(f"  {YELLOW}⚠  Market is currently CLOSED"
              f"{' (weekend)' if is_weekend else ' (post-market)'}.{RESET}")

        if is_weekend:
            print(f"  NSE APIs are offline on weekends — cannot fetch live data.")
            print(f"\n{'═'*72}")
            print(f"  {YELLOW}⚠  MARKET CLOSED (weekend) — No data available.  "
                  f"Loop ended.{RESET}")
            print(f"{'═'*72}\n")
            return

        print(f"  Fetching last available 15-min candle + option chain …")
        print(f"  (warming up NSE session — this may take ~15s post-market)\n")

        # ── Build session with retries ────────────────────────────────────
        session = _live_session(verbose=True)

        # ── Strategy: try OPTION CHAIN first (most reliable post-close) ──
        # The option chain API keeps data populated for ~30 min after close.
        # It also returns the underlying value, which we use as the LTP.
        print(f"  Trying option chain endpoint …", flush=True)
        opt = None
        underlying_ltp = 0.0

        for _retry in range(3):
            opt = fetch_live_options(session,
                                     otm_range=otm_range,
                                     expiry_index=expiry_index)
            if opt is not None:
                underlying_ltp = float(opt.get("underlying", 0) or 0)
                break
            print(f"  Option chain attempt {_retry+1} failed — retrying in 5s …")
            time.sleep(5)
            # Re-warm session on each retry
            session = _live_session(verbose=False)

        # ── Fallback: try allIndices for the LTP ─────────────────────────
        idx_snap = None
        if underlying_ltp <= 0:
            print(f"  Option chain unavailable — trying allIndices …", flush=True)
            for _retry in range(3):
                idx_snap = fetch_live_index(session)
                if idx_snap and float(idx_snap.get("ltp", 0) or 0) > 0:
                    underlying_ltp = float(idx_snap["ltp"])
                    break
                print(f"  allIndices attempt {_retry+1} failed — retrying in 5s …")
                time.sleep(5)
                session = _live_session(verbose=False)

        # ── Build synthetic last-candle from whatever LTP we got ─────────
        if underlying_ltp > 0:
            # Use the option chain's underlying value OR the allIndices LTP.
            # The last 15-min candle of the day is 15:15 → 15:30.
            t_open  = now.replace(hour=MARKET_CLOSE_H,
                                  minute=MARKET_CLOSE_M - CANDLE_MINUTES,
                                  second=0, microsecond=0)
            t_close = close_t

            # If the opt chain has the underlying, extract day OHLC for richer candle
            # (allIndices gives day OHLC; option chain gives underlying only)
            candle_o = underlying_ltp
            candle_h = underlying_ltp
            candle_l = underlying_ltp
            candle_c = underlying_ltp

            if idx_snap:
                # allIndices day OHLC — use as proxy for last-candle range
                candle_o = float(idx_snap.get("open",  underlying_ltp) or underlying_ltp)
                candle_h = float(idx_snap.get("high",  underlying_ltp) or underlying_ltp)
                candle_l = float(idx_snap.get("low",   underlying_ltp) or underlying_ltp)
                candle_c = float(idx_snap.get("ltp",   underlying_ltp) or underlying_ltp)

            last_candle = {
                "candle_open_time" : t_open,
                "candle_close_time": t_close,
                "open"  : candle_o,
                "high"  : candle_h,
                "low"   : candle_l,
                "close" : candle_c,
                "volume": 0,
                "ticks" : 0,
            }

            # If we only got LTP (not full OHLC), add a note
            if not idx_snap:
                print(f"  {YELLOW}Note: Day OHLC unavailable post-market. "
                      f"Candle O/H/L/C all set to last underlying: "
                      f"{underlying_ltp:,.2f}{RESET}")

            _print_candle_15(last_candle, opt)

            if save_csv:
                row = _flatten_candle_with_options(
                    last_candle, idx_snap or {}, opt)
                _save_candle_row(row, csv_file)

        else:
            # Both endpoints failed
            print(f"\n  {RED}[!] Could not retrieve any data from NSE.{RESET}")
            print(f"  Possible reasons:")
            print(f"    • NSE website is down or under maintenance")
            print(f"    • Your IP is rate-limited (wait 60s and retry)")
            print(f"    • Network / firewall blocking nseindia.com")
            print(f"    • NSE clears API data more than ~30 min after close")
            print(f"  Try running again or open https://www.nseindia.com "
                  f"in a browser first to unblock your IP.")

        print(f"\n{'═'*72}")
        print(f"  {YELLOW}⚠  MARKET CLOSED  —  loop ended.  "
              f"No further triggers scheduled.{RESET}")
        print(f"{'═'*72}\n")
        return   # ← clean exit, no waiting for next open

    # ── ── ── ── ── ── ── ── ── ── ── ── ── ── ── ── ── ── ── ── ── ──
    # CASE B: Pre-open (market hasn't opened yet today)
    #   → wait silently until 09:15.
    # ── ── ── ── ── ── ── ── ── ── ── ── ── ── ── ── ── ── ── ── ── ──
    if not _market_open_now():
        wait = _seconds_until_market_open()
        mm_t, ss_t = divmod(int(wait), 60)
        hh_t, mm_t = divmod(mm_t, 60)
        print(f"  Market not yet open.  Waiting {hh_t:02d}h {mm_t:02d}m {ss_t:02d}s "
              f"until 09:15 …  (Ctrl+C to abort)\n")
        try:
            open_dt = datetime.now() + timedelta(seconds=wait)
            _sleep_until(open_dt)
        except KeyboardInterrupt:
            print("\n  Aborted.")
            return

    print("  Connecting to NSE and warming up session …")
    session = _live_session(verbose=True)
    print("  Session ready.\n")

    builder      = CandleBuilder(interval_minutes=CANDLE_MINUTES)
    candles_done = 0
    last_opt     = None

    try:
        while True:
            now     = datetime.now()
            close_t = now.replace(hour=MARKET_CLOSE_H, minute=MARKET_CLOSE_M,
                                  second=0, microsecond=0)

            # ── Has the market closed since our last candle? ───────────────
            if now > close_t or now.weekday() >= 5:
                # Emit any partial candle that was in progress
                final = builder.force_close()
                if final:
                    print(f"\n  [{now.strftime('%H:%M:%S')}]  "
                          f"[END-OF-DAY] Emitting last partial candle …")
                    # Try one last fresh option fetch
                    opt_final = fetch_live_options(session,
                                                   otm_range=otm_range,
                                                   expiry_index=expiry_index)
                    last_opt = opt_final or last_opt
                    _print_candle_15(final, last_opt)
                    if save_csv:
                        idx_snap = fetch_live_index(session) or {}
                        row = _flatten_candle_with_options(final, idx_snap, last_opt)
                        _save_candle_row(row, csv_file)

                print(f"\n{'═'*72}")
                print(f"  {YELLOW}⚠  MARKET CLOSED at "
                      f"{MARKET_CLOSE_H:02d}:{MARKET_CLOSE_M:02d}.  "
                      f"Total 15-min candles emitted: {candles_done}.  "
                      f"Loop ended.{RESET}")
                print(f"{'═'*72}\n")
                break   # ← exit the loop; no waiting for next day

            # ── Compute next candle-close boundary ────────────────────────
            boundary     = _next_15min_boundary(now)
            warmup_t     = boundary - timedelta(seconds=WARMUP_SECONDS)

            # Safety: if boundary is somehow past 15:30, cap and exit next iter
            if boundary > close_t:
                boundary = close_t

            # ── IDLE PHASE: sleep until warm-up window opens ──────────────
            secs_to_warmup = (warmup_t - datetime.now()).total_seconds()
            if secs_to_warmup > 2:
                mm_left, ss_left = divmod(int(secs_to_warmup + WARMUP_SECONDS), 60)
                print(f"  [{datetime.now().strftime('%H:%M:%S')}]  "
                      f"Next candle close : {boundary.strftime('%H:%M:%S')}  "
                      f"({mm_left}m {ss_left}s away)  — sleeping …")
                try:
                    _sleep_until(warmup_t)
                except KeyboardInterrupt:
                    raise

            # ── WARM-UP PHASE: poll ticks aggressively ────────────────────
            print(f"\n  [{datetime.now().strftime('%H:%M:%S')}]  "
                  f"{CYAN}Warm-up phase — polling every {TICK_POLL_S}s "
                  f"until {boundary.strftime('%H:%M:%S')}{RESET}")

            while datetime.now() < boundary:
                t0  = datetime.now()
                idx = fetch_live_index(session)
                if idx:
                    ltp = float(idx.get("ltp", 0) or 0)
                    if ltp > 0:
                        builder.push(ltp=ltp, ts=t0)
                        cur = builder.current()
                        if cur:
                            ot  = cur["candle_open_time"]
                            if isinstance(ot, datetime): ot = ot.strftime("%H:%M")
                            o   = cur["open"]; h = cur["high"]
                            l   = cur["low"]
                            chg = ltp - o
                            col = GREEN if chg >= 0 else RED
                            print(f"    tick {t0.strftime('%H:%M:%S')}  "
                                  f"O:{o:,.2f} H:{h:,.2f} L:{l:,.2f} "
                                  f"C:{col}{ltp:,.2f}{RESET}  "
                                  f"({chg:+.2f})  ticks={cur['ticks']}")
                elapsed   = (datetime.now() - t0).total_seconds()
                remaining = (boundary - datetime.now()).total_seconds()
                if remaining <= 0:
                    break
                time.sleep(min(TICK_POLL_S - elapsed, remaining, TICK_POLL_S))

            # ── CANDLE CLOSE: boundary has arrived ────────────────────────
            trigger_ts = boundary + timedelta(seconds=1)
            idx_final  = fetch_live_index(session)
            ltp_final  = float((idx_final or {}).get("ltp", 0) or 0)

            if ltp_final > 0:
                builder.push(ltp=ltp_final, ts=trigger_ts)

            closed = builder.force_close()

            if closed is None:
                print(f"  [{datetime.now().strftime('%H:%M:%S')}]  "
                      f"[WARN] No candle data to emit — skipping.")
                time.sleep(2)
                continue

            candles_done += 1
            print(f"\n  [{datetime.now().strftime('%H:%M:%S')}]  "
                  f"Candle #{candles_done} closed → fetching option chain …")

            # Re-warm session periodically to keep cookies fresh
            if candles_done % SESSION_REWARM == 0:
                print("  [SESSION] Re-warming NSE session …")
                session = _live_session(verbose=False)

            opt = fetch_live_options(session,
                                     otm_range=otm_range,
                                     expiry_index=expiry_index)
            if opt is None:
                print("  [WARN] Option fetch failed — reusing last successful data")
                opt = last_opt
            else:
                last_opt = opt

            # Print the closed candle + option chain
            _print_candle_15(closed, opt)

            # Save to CSV if requested
            if save_csv:
                row = _flatten_candle_with_options(closed, idx_final or {}, opt)
                _save_candle_row(row, csv_file)

            # If this was the 15:30 candle (final candle of the day) → exit
            ct_str = closed.get("candle_close_time")
            if isinstance(ct_str, datetime):
                if (ct_str.hour == MARKET_CLOSE_H and
                        ct_str.minute == MARKET_CLOSE_M):
                    print(f"\n{'═'*72}")
                    print(f"  {YELLOW}⚠  MARKET CLOSED at "
                          f"{MARKET_CLOSE_H:02d}:{MARKET_CLOSE_M:02d}.  "
                          f"Total 15-min candles emitted: {candles_done}.  "
                          f"Loop ended.{RESET}")
                    print(f"{'═'*72}\n")
                    break

            # Short pause then loop back to compute next boundary
            time.sleep(2)

    except KeyboardInterrupt:
        print(f"\n\n  Interrupted by user.  Candles completed: {candles_done}\n")
        final = builder.force_close()
        if final:
            print("  Flushing partial candle on exit …")
            _print_candle_15(final, last_opt)
            if save_csv:
                try:
                    idx_snap = fetch_live_index(session) or {}
                    row = _flatten_candle_with_options(final, idx_snap, last_opt)
                    _save_candle_row(row, csv_file)
                except Exception:
                    pass


# ═══════════════════════════════════════════════════════════════════════════════
#  ENTRY POINT
# ═══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="NIFTY 50 Fetcher — historical | live snapshot | 15-min intraday candles"
    )
    parser.add_argument("--live",     action="store_true",
                        help="Fetch LIVE snapshot (index quote + option chain).")
    parser.add_argument("--intraday", action="store_true",
                        help="15-MIN CANDLE MODE: clock-aligned trigger every 15 min + option chain at each close.")
    parser.add_argument("--watch",    type=int, default=None, metavar="SECONDS",
                        help="Auto-refresh live snapshot every N seconds (use with --live)")
    parser.add_argument("--otm",      type=int, default=LIVE_OTM_RANGE, metavar="N",
                        help=f"Strikes above/below ATM to display (default {LIVE_OTM_RANGE})")
    parser.add_argument("--expiry",   type=int, default=0, metavar="N",
                        help="Expiry index: 0=nearest (default), 1=next week, 2=far month ...")
    parser.add_argument("--save",     action="store_true",
                        help="Save candles/snapshots to CSV")
    parser.add_argument("--start",    type=str, default=START_DATE,
                        help=f"Historical start date DD-MM-YYYY (default {START_DATE})")
    parser.add_argument("--end",      type=str, default=END_DATE,
                        help=f"Historical end date DD-MM-YYYY (default {END_DATE})")
    args = parser.parse_args()

    if args.intraday:
        # ── 5-MIN CANDLE MODE ──────────────────────────────────────────────
        _run_intraday_cli(args)

    elif args.live:
        # ── LIVE SNAPSHOT MODE ─────────────────────────────────────────────
        _run_live_cli(args)

    else:
        # ── HISTORICAL MODE ────────────────────────────────────────────────
        df = fetch_all(args.start, args.end)
        df.to_csv(OUTPUT_FILE, index=False, date_format="%Y-%m-%d")
        print(f"Saved {len(df)} rows  →  {OUTPUT_FILE}")
        print(f"Shape : {df.shape}")
        print(f"Range : {df['date'].min().date()} → {df['date'].max().date()}")
        print(f"\nSample (first day):")
        print(df.head(1).T.to_string())