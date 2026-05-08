"""
NIFTY 50 — Index + Options Data Fetcher (NSE Only)
====================================================
Output: ONE ROW PER DAY in nifty50_combined.csv
  Columns: date, open, high, low, close, volume,
           atm_strike, nifty_close, expiry,
           CE_ATM+0_strike, CE_ATM+0_close, CE_ATM+0_oi,
           CE_ATM+1_strike, CE_ATM+1_close, CE_ATM+1_oi, ... CE_ATM+5
           PE_ATM-0_strike, PE_ATM-0_close, PE_ATM-0_oi, ... PE_ATM-5

NSE Archive Sources:
  Index  : https://archives.nseindia.com/content/indices/ind_close_all_DDMMYYYY.csv
  F&O old: https://archives.nseindia.com/content/historical/DERIVATIVES/YYYY/MON/foDDMONYYYYbhav.csv.zip
  F&O new: https://nsearchives.nseindia.com/content/fo/BhavCopy_NSE_FO_0_0_0_YYYYMMDD_F_0000.csv.zip
  (New UDiFF format applies from 08-Jul-2024 per NSE Circular No. 62424)

Install: pip install requests pandas
"""

import io
import time
import zipfile
import requests
import pandas as pd
from datetime import datetime, timedelta


# ═══════════════════════════════════════════════════════════════════════════════
#  CONFIG  — only edit these lines
# ═══════════════════════════════════════════════════════════════════════════════
START_DATE        = "01-01-2020"
END_DATE          = "24-04-2026"
OUTPUT_FILE       = "nifty50_combined.csv"
NIFTY_STRIKE_STEP = 50
# ═══════════════════════════════════════════════════════════════════════════════

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept":          "*/*",
    "Accept-Language": "en-US,en;q=0.5",
    "Referer":         "https://www.nseindia.com/",
}

RETRY_DELAY     = 0.35
REQUEST_TIMEOUT = 15
FO_CUTOVER      = datetime(2024, 7, 8)   # NSE Circular No. 62424
MON3            = ["JAN","FEB","MAR","APR","MAY","JUN",
                   "JUL","AUG","SEP","OCT","NOV","DEC"]

INDEX_URL  = "https://archives.nseindia.com/content/indices/ind_close_all_{date}.csv"
FO_OLD_URL = ("https://archives.nseindia.com/content/historical/DERIVATIVES/"
              "{yyyy}/{mon}/fo{dd}{mon}{yyyy}bhav.csv.zip")
FO_NEW_URL = ("https://nsearchives.nseindia.com/content/fo/"
              "BhavCopy_NSE_FO_0_0_0_{yyyymmdd}_F_0000.csv.zip")


# ───────────────────────────────────────────────────────────────────────────────
#  Helpers
# ───────────────────────────────────────────────────────────────────────────────

def _parse_date(s):
    for fmt in ("%d-%m-%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    raise ValueError(f"Bad date: {s!r} — use DD-MM-YYYY")


def _get(session, url):
    for attempt in range(3):
        try:
            r = session.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
            if r.status_code == 404:
                return None
            if r.status_code == 200:
                return r
            time.sleep(1.5 * (attempt + 1))
        except requests.RequestException:
            time.sleep(1.5 * (attempt + 1))
    return None


def _atm(close, step):
    return int(round(close / step) * step)


def _to_num(series):
    return pd.to_numeric(
        series.astype(str).str.replace(",", ""), errors="coerce"
    )


# ───────────────────────────────────────────────────────────────────────────────
#  Index fetcher (unchanged — always worked)
# ───────────────────────────────────────────────────────────────────────────────

def _fetch_index_day(session, dt):
    url  = INDEX_URL.format(date=dt.strftime("%d%m%Y"))
    resp = _get(session, url)
    if resp is None:
        return None

    try:
        df = pd.read_csv(io.StringIO(resp.text), thousands=",")
    except Exception:
        return None

    df.columns = df.columns.str.strip()
    name_col   = next((c for c in df.columns
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


# ───────────────────────────────────────────────────────────────────────────────
#  F&O bhavcopy — OLD format (pre Jul-8-2024)
#  Columns: INSTRUMENT, SYMBOL, EXPIRY_DT, OPTION_TYP, STRIKE_PR,
#           OPEN, HIGH, LOW, CLOSE, SETTLE_PR, OPEN_INT, NO_OF_CONT
# ───────────────────────────────────────────────────────────────────────────────

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

    # Rename known columns — covers slight naming variations across years
    rename_map = {
        "OPTION_TYP":  "OPT_TYPE",
        "STRIKE_PR":   "STRIKE",
        "SETTLE_PR":   "SETTLE",
        "OPEN_INT":    "OI",
        # VOLUME: NSE used different names in different years
        "NO_OF_CONT":  "VOLUME",
        "CONTRACTS":   "VOLUME",
        "NO_OF_CONTRACTS": "VOLUME",
        "TRADED_QTY":  "VOLUME",
        "TTL_TRADG_VOL": "VOLUME",
    }
    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})

    # If VOLUME is still missing, create it as NaN so downstream code doesn't crash
    if "VOLUME" not in df.columns:
        df["VOLUME"] = float("nan")

    df["EXPIRY_DT"] = pd.to_datetime(df["EXPIRY_DT"], dayfirst=True, errors="coerce")
    df["OPT_TYPE"]  = df["OPT_TYPE"].astype(str).str.strip().str.upper()
    for c in ("STRIKE","OPEN","HIGH","LOW","CLOSE","SETTLE","OI","VOLUME"):
        if c in df.columns:
            df[c] = _to_num(df[c])

    # Only select columns that actually exist (guards against any future schema change)
    desired = ["EXPIRY_DT","OPT_TYPE","STRIKE","OPEN","HIGH","LOW",
               "CLOSE","SETTLE","OI","VOLUME"]
    return df[[c for c in desired if c in df.columns]]


# ───────────────────────────────────────────────────────────────────────────────
#  F&O bhavcopy — NEW UDiFF format (post Jul-8-2024)
#  Key columns: TckrSymb, XpryDt, StrkPric, OptnTp,
#               OpnPric, HghPric, LwPric, ClsPric,
#               SttlmPric, OpnIntrst, TtlTradgVol, FinInstrmTp
# ───────────────────────────────────────────────────────────────────────────────

def _parse_fo_new(raw):
    raw = raw.reset_index(drop=True)
    raw.columns = raw.columns.str.strip()

    # UDiFF uses FinInstrmTp for instrument type (OPTIDX = index option)
    # and TckrSymb for ticker symbol
    inst_col = next((c for c in raw.columns
                     if c.upper() in ("FININSTRMTP","FININSTRMTYPE",
                                      "FIN_INSTRM_TP")), None)
    sym_col  = next((c for c in raw.columns
                     if c.upper() in ("TCKRSYMB","TCKR_SYMB","SYMBOL")), None)

    if inst_col is None or sym_col is None:
        return None

    # Confirmed UDiFF FinInstrmTp values from live NSE FO files:
    #   'IDO' = Index Derivative Option  ← NIFTY/BANKNIFTY options
    #   'IDF' = Index Derivative Future
    #   'STO' = Stock Option
    #   'STF' = Stock Future
    inst_vals = raw[inst_col].astype(str).str.strip().str.upper()
    sym_vals  = raw[sym_col].astype(str).str.strip().str.upper()
    inst_mask = inst_vals.isin(["IDO", "IO", "OPTIDX"])  # all known variants
    sym_mask  = sym_vals == "NIFTY"
    df = raw[inst_mask & sym_mask].copy().reset_index(drop=True)

    if df.empty:
        return None

    # Map UDiFF column names → internal names
    # UDiFF spec: XpryDt=expiry, StrkPric=strike, OptnTp=CE/PE,
    #             OpnPric=open, HghPric=high, LwPric=low, ClsPric=close,
    #             SttlmPric=settle, OpnIntrst=OI, TtlTradgVol=volume
    col_upper = {c.upper(): c for c in df.columns}

    def gc(options):
        """Get first matching column name (case-insensitive)."""
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

    # Only return columns that are actually populated
    desired = ["EXPIRY_DT","OPT_TYPE","STRIKE","OPEN","HIGH","LOW",
               "CLOSE","SETTLE","OI","VOLUME"]
    return result[[c for c in desired if c in result.columns]]


# ───────────────────────────────────────────────────────────────────────────────
#  Unified F&O fetcher — picks old/new parser automatically
# ───────────────────────────────────────────────────────────────────────────────

def _fetch_fo_day(session, dt):
    if dt >= FO_CUTOVER:
        url = FO_NEW_URL.format(yyyymmdd=dt.strftime("%Y%m%d"))
    else:
        mon = MON3[dt.month - 1]
        url = FO_OLD_URL.format(yyyy=dt.strftime("%Y"),
                                mon=mon, dd=dt.strftime("%d"))

    resp = _get(session, url)
    if resp is None:
        return None

    try:
        zf  = zipfile.ZipFile(io.BytesIO(resp.content))
        csv = zf.read(zf.namelist()[0])
        raw = pd.read_csv(io.BytesIO(csv), low_memory=False)
    except Exception as e:
        print(f"  [WARN] Could not parse F&O zip for {dt.date()}: {e}")
        return None

    if dt >= FO_CUTOVER:
        return _parse_fo_new(raw)
    else:
        return _parse_fo_old(raw)


# ───────────────────────────────────────────────────────────────────────────────
#  Build one flat row per day
# ───────────────────────────────────────────────────────────────────────────────

def _build_row(idx, fo_df, step):
    row         = dict(idx)
    close_price = idx["close"]
    atm         = _atm(close_price, step)

    row["atm_strike"]  = atm
    row["nifty_close"] = close_price
    row["expiry"]      = None

    # Pre-fill all option columns with None
    for i in range(6):
        row[f"CE_ATM+{i}_strike"] = atm + i * step
        row[f"CE_ATM+{i}_close"]  = None
        row[f"CE_ATM+{i}_oi"]     = None
        row[f"PE_ATM-{i}_strike"] = atm - i * step
        row[f"PE_ATM-{i}_close"]  = None
        row[f"PE_ATM-{i}_oi"]     = None

    if fo_df is None or fo_df.empty:
        return row

    # Nearest expiry >= today
    expiries = fo_df["EXPIRY_DT"].dropna().unique()
    future   = sorted([e for e in expiries
                       if pd.Timestamp(e).date() >= idx["date"]])
    if not future:
        return row

    nearest       = future[0]
    row["expiry"] = pd.Timestamp(nearest).date()
    day_df        = fo_df[fo_df["EXPIRY_DT"] == nearest].copy()

    def _opt(opt_type, strike):
        sub = day_df[
            (day_df["OPT_TYPE"] == opt_type) &
            (day_df["STRIKE"]   == float(strike))
        ]
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


# ───────────────────────────────────────────────────────────────────────────────
#  Main loop
# ───────────────────────────────────────────────────────────────────────────────

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
    rows    = []
    fetched = 0
    skipped = 0

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

    # ── Assemble final DataFrame ──────────────────────────────────
    ce_cols, pe_cols = [], []
    for i in range(6):
        ce_cols += [f"CE_ATM+{i}_strike", f"CE_ATM+{i}_close", f"CE_ATM+{i}_oi"]
        pe_cols += [f"PE_ATM-{i}_strike", f"PE_ATM-{i}_close", f"PE_ATM-{i}_oi"]

    col_order = (["date","open","high","low","close","volume",
                  "atm_strike","nifty_close","expiry"]
                 + ce_cols + pe_cols)

    df        = pd.DataFrame(rows)
    col_order = [c for c in col_order if c in df.columns]
    df        = df[col_order]
    df["date"] = pd.to_datetime(df["date"])
    if "expiry" in df.columns:
        df["expiry"] = pd.to_datetime(df["expiry"])

    return df.sort_values("date").reset_index(drop=True)


# ───────────────────────────────────────────────────────────────────────────────
#  Entry point
# ───────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    df = fetch_all(START_DATE, END_DATE)

    df.to_csv(OUTPUT_FILE, index=False, date_format="%Y-%m-%d")
    print(f"Saved {len(df)} rows  →  {OUTPUT_FILE}")
    print(f"Shape : {df.shape}")
    print(f"Range : {df['date'].min().date()} → {df['date'].max().date()}")
    print(f"\nSample (first day):")
    print(df.head(1).T.to_string())
