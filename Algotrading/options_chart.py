#!/usr/bin/env python3
# =============================================================================
# options_chart.py  —  Options Price Chart Generator using Dhan API
# =============================================================================
#
# Generates an interactive HTML chart for any option contract.
# Uses the same credentials and instrument registry as dhan_trading_system.py.
#
# INSTALL:
#   pip install requests pandas  (no extra charting lib needed — pure HTML/JS)
#
# USAGE EXAMPLES:
# ─── By expiry date ──────────────────────────────────────────────────────────
#   python options_chart.py --symbol NIFTY --type PE --expiry 2022-01-27
#   python options_chart.py --symbol BANKNIFTY --type PE --expiry 2025-05-29 --strike ATM+1
#   python options_chart.py --symbol RELIANCE --type CE --expiry 2025-05-29
#
# ─── By day (picks expiry automatically) ────────────────────────────────────
#   python options_chart.py --symbol NIFTY --type CE --day 2025-05-12
#   python options_chart.py --symbol HDFCBANK --type PE --day 2025-04-10
#
# ─── With date range ────────────────────────────────────────────────────────
#   python options_chart.py --symbol NIFTY --type CE --from 2025-04-01 --to 2025-04-30
#
# ─── Both CE and PE on same chart ───────────────────────────────────────────
#   python options_chart.py --symbol NIFTY --type BOTH --expiry 2025-06-05
#   python options_chart.py --symbol BANKNIFTY --type BOTH --day 2025-05-07
#
# ─── With interval (default 15m) ─────────────────────────────────────────────
#   python options_chart.py --symbol NIFTY --type CE --expiry 2025-06-05 --interval 5
#   python options_chart.py --symbol NIFTY --type BOTH --day 2025-05-07 --interval 60
#
# OUTPUT:
#   Opens an interactive HTML file in your browser automatically.
#   Also saves it as  nifty_ce_chart.html  (or similar) in current directory.
#
# CHART FEATURES:
#   • Candlestick chart of option premium (OHLC)
#   • Underlying spot price line (secondary axis)
#   • Volume bars
#   • OI (Open Interest) change indicators
#   • IV (Implied Volatility) line
#   • Key levels: entry SL (−40%), target (+80%), trail (−20% from ATM)
#   • Expiry date marker
#   • Hover tooltips on every candle
#   • Fully interactive (zoom, pan, reset)
#   • CE and PE side-by-side when --type BOTH
# =============================================================================

import os, sys, json, time, argparse, webbrowser, tempfile
from datetime import datetime, timedelta, date
from typing import Optional, List, Dict, Tuple

import requests
import pandas as pd

# ─── CREDENTIALS  (copy from dhan_trading_system.py or set env vars) ──────────
DHAN_CLIENT_ID    = os.getenv("DHAN_CLIENT_ID",   "1111077247")
DHAN_ACCESS_TOKEN = os.getenv("DHAN_ACCESS_TOKEN", "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJpc3MiOiJkaGFuIiwicGFydG5lcklkIjoiIiwiZXhwIjoxNzc2NjA4MjUxLCJpYXQiOjE3NzY1MjE4NTEsInRva2VuQ29uc3VtZXJUeXBlIjoiU0VMRiIsIndlYmhvb2tVcmwiOiIiLCJkaGFuQ2xpZW50SWQiOiIxMTExMDc3MjQ3In0.OLOT1QVqd06rKDrfsLkDf8dqVklwkCdWzMxLZ_fJxfYag8XColEgcr9FY1POVu3VWsfIBjWUhxntXIfAyq0xfQ")

BASE_URL         = "https://api.dhan.co/v2"
MAX_DAYS_PER_REQ = 29   # rollingoption API chunk limit


# =============================================================================
# INSTRUMENT REGISTRY  (same as dhan_trading_system.py)
# =============================================================================

INSTRUMENTS: Dict[str, dict] = {
    # ── NSE Indices (weekly) ─────────────────────────────────────────────────
    "NIFTY":      {"opt_sid":13,    "eq_sid":"13",  "eq_seg":"IDX_I",   "eq_instr":"INDEX",
                   "opt_seg":"NSE_FNO","opt_instr":"OPTIDX","lot_size":75,   "strike_int":50,
                   "expiry_flag":"WEEK"},
    "BANKNIFTY":  {"opt_sid":25,    "eq_sid":"25",  "eq_seg":"IDX_I",   "eq_instr":"INDEX",
                   "opt_seg":"NSE_FNO","opt_instr":"OPTIDX","lot_size":30,   "strike_int":100,
                   "expiry_flag":"WEEK"},
    "SENSEX":     {"opt_sid":1,     "eq_sid":"1",   "eq_seg":"BSE_I",   "eq_instr":"INDEX",
                   "opt_seg":"NSE_FNO","opt_instr":"OPTIDX","lot_size":10,   "strike_int":200,
                   "expiry_flag":"WEEK"},
    "MIDCPNIFTY": {"opt_sid":27,    "eq_sid":"27",  "eq_seg":"IDX_I",   "eq_instr":"INDEX",
                   "opt_seg":"NSE_FNO","opt_instr":"OPTIDX","lot_size":75,   "strike_int":25,
                   "expiry_flag":"WEEK"},
    "FINNIFTY":   {"opt_sid":26,    "eq_sid":"26",  "eq_seg":"IDX_I",   "eq_instr":"INDEX",
                   "opt_seg":"NSE_FNO","opt_instr":"OPTIDX","lot_size":40,   "strike_int":50,
                   "expiry_flag":"WEEK"},
    # ── NSE Stocks (monthly) ─────────────────────────────────────────────────
    "HDFCBANK":   {"opt_sid":13,    "eq_sid":"1333","eq_seg":"NSE_EQ",  "eq_instr":"EQUITY",
                   "opt_seg":"NSE_FNO","opt_instr":"OPTSTK","lot_size":550,  "strike_int":10,
                   "expiry_flag":"MONTH"},
    "RELIANCE":   {"opt_sid":2885,  "eq_sid":"2885","eq_seg":"NSE_EQ",  "eq_instr":"EQUITY",
                   "opt_seg":"NSE_FNO","opt_instr":"OPTSTK","lot_size":250,  "strike_int":20,
                   "expiry_flag":"MONTH"},
    "INFY":       {"opt_sid":10604, "eq_sid":"10604","eq_seg":"NSE_EQ", "eq_instr":"EQUITY",
                   "opt_seg":"NSE_FNO","opt_instr":"OPTSTK","lot_size":300,  "strike_int":20,
                   "expiry_flag":"MONTH"},
    "TCS":        {"opt_sid":11536, "eq_sid":"11536","eq_seg":"NSE_EQ", "eq_instr":"EQUITY",
                   "opt_seg":"NSE_FNO","opt_instr":"OPTSTK","lot_size":150,  "strike_int":50,
                   "expiry_flag":"MONTH"},
    "ICICIBANK":  {"opt_sid":4963,  "eq_sid":"4963","eq_seg":"NSE_EQ",  "eq_instr":"EQUITY",
                   "opt_seg":"NSE_FNO","opt_instr":"OPTSTK","lot_size":700,  "strike_int":5,
                   "expiry_flag":"MONTH"},
    "SBIN":       {"opt_sid":3045,  "eq_sid":"3045","eq_seg":"NSE_EQ",  "eq_instr":"EQUITY",
                   "opt_seg":"NSE_FNO","opt_instr":"OPTSTK","lot_size":1500, "strike_int":5,
                   "expiry_flag":"MONTH"},
    "BAJFINANCE": {"opt_sid":317,   "eq_sid":"317", "eq_seg":"NSE_EQ",  "eq_instr":"EQUITY",
                   "opt_seg":"NSE_FNO","opt_instr":"OPTSTK","lot_size":125,  "strike_int":50,
                   "expiry_flag":"MONTH"},
    "AXISBANK":   {"opt_sid":5900,  "eq_sid":"5900","eq_seg":"NSE_EQ",  "eq_instr":"EQUITY",
                   "opt_seg":"NSE_FNO","opt_instr":"OPTSTK","lot_size":625,  "strike_int":5,
                   "expiry_flag":"MONTH"},
    "WIPRO":      {"opt_sid":3787,  "eq_sid":"3787","eq_seg":"NSE_EQ",  "eq_instr":"EQUITY",
                   "opt_seg":"NSE_FNO","opt_instr":"OPTSTK","lot_size":1500, "strike_int":5,
                   "expiry_flag":"MONTH"},
    "TITAN":      {"opt_sid":3506,  "eq_sid":"3506","eq_seg":"NSE_EQ",  "eq_instr":"EQUITY",
                   "opt_seg":"NSE_FNO","opt_instr":"OPTSTK","lot_size":375,  "strike_int":10,
                   "expiry_flag":"MONTH"},
    "TATAMOTORS": {"opt_sid":3456,  "eq_sid":"3456","eq_seg":"NSE_EQ",  "eq_instr":"EQUITY",
                   "opt_seg":"NSE_FNO","opt_instr":"OPTSTK","lot_size":1425, "strike_int":5,
                   "expiry_flag":"MONTH"},
    "HCLTECH":    {"opt_sid":10666, "eq_sid":"10666","eq_seg":"NSE_EQ", "eq_instr":"EQUITY",
                   "opt_seg":"NSE_FNO","opt_instr":"OPTSTK","lot_size":700,  "strike_int":10,
                   "expiry_flag":"MONTH"},
    "DRREDDY":    {"opt_sid":881,   "eq_sid":"881", "eq_seg":"NSE_EQ",  "eq_instr":"EQUITY",
                   "opt_seg":"NSE_FNO","opt_instr":"OPTSTK","lot_size":125,  "strike_int":50,
                   "expiry_flag":"MONTH"},
    "MARUTI":     {"opt_sid":10999, "eq_sid":"10999","eq_seg":"NSE_EQ", "eq_instr":"EQUITY",
                   "opt_seg":"NSE_FNO","opt_instr":"OPTSTK","lot_size":100,  "strike_int":100,
                   "expiry_flag":"MONTH"},
    # ── Currency (monthly) ───────────────────────────────────────────────────
    "USDINR":     {"opt_sid":10093, "eq_sid":"10093","eq_seg":"NSE_CUR","eq_instr":"FUTCUR",
                   "opt_seg":"NSE_CUR","opt_instr":"OPTCUR","lot_size":1000, "strike_int":0.25,
                   "expiry_flag":"MONTH"},
    # ── Commodity (monthly) ──────────────────────────────────────────────────
    "GOLD":       {"opt_sid":10080, "eq_sid":"10080","eq_seg":"MCX_COMM","eq_instr":"FUTCOM",
                   "opt_seg":"MCX_COMM","opt_instr":"OPTFUT","lot_size":100, "strike_int":100,
                   "expiry_flag":"MONTH"},
    "CRUDEOIL":   {"opt_sid":10082, "eq_sid":"10082","eq_seg":"MCX_COMM","eq_instr":"FUTCOM",
                   "opt_seg":"MCX_COMM","opt_instr":"OPTFUT","lot_size":100, "strike_int":50,
                   "expiry_flag":"MONTH"},
}

# Strike offset map
STRIKE_OFFSETS = {
    "ATM": "ATM", "ATM+1": "ATM+1", "ATM+2": "ATM+2",
    "ATM-1": "ATM-1", "ATM-2": "ATM-2",
    "OTM1": "ATM+1", "OTM2": "ATM+2",
    "ITM1": "ATM-1", "ITM2": "ATM-2",
}


# =============================================================================
# EXPIRY DATE HELPERS
# =============================================================================

def nearest_thursday(d: date) -> date:
    """Nearest Thursday on or after date d."""
    days = (3 - d.weekday()) % 7
    return d + timedelta(days=days)


def last_thursday_of_month(d: date) -> date:
    """Last Thursday of the month containing date d."""
    if d.month == 12:
        last = date(d.year + 1, 1, 1) - timedelta(days=1)
    else:
        last = date(d.year, d.month + 1, 1) - timedelta(days=1)
    offset = (last.weekday() - 3) % 7
    return last - timedelta(days=offset)


def derive_expiry(for_date: date, expiry_flag: str) -> date:
    """
    Given a reference date and expiry_flag ("WEEK" or "MONTH"),
    return the most likely front-contract expiry date.

    WEEKLY:
        If for_date is a Thursday, that day IS the expiry.
        If it's before Thursday, the next Thursday is the expiry.
        If the nearest Thursday is passed (data from that expiry is done),
        return the Thursday of next week.

    MONTHLY:
        Return the last Thursday of for_date's month.
        If that date has already passed, return next month's last Thursday.
    """
    flag = expiry_flag.upper()
    if flag in ("WEEK", "WEEKLY"):
        exp = nearest_thursday(for_date)
        return exp
    else:
        exp = last_thursday_of_month(for_date)
        if exp < for_date:
            # Use next month
            nm = date(for_date.year + (1 if for_date.month == 12 else 0),
                      (for_date.month % 12) + 1, 1)
            exp = last_thursday_of_month(nm)
        return exp


def expiry_code_for_date(target_expiry: date, ref_date: date,
                          expiry_flag: str) -> int:
    """
    Dhan rollingoption expiryCode:
      1 = front (nearest) expiry
      2 = next expiry
      3 = far expiry

    If the target expiry matches the front expiry of ref_date, return 1.
    If it matches the next one, return 2. Etc.
    Defaults to 1 if we can't determine.
    """
    flag = expiry_flag.upper()
    for code in range(1, 4):
        if flag in ("WEEK", "WEEKLY"):
            candidate = nearest_thursday(ref_date + timedelta(weeks=code - 1))
        else:
            # Front month + (code-1) months
            y, m = ref_date.year, ref_date.month
            m += (code - 1)
            while m > 12:
                m -= 12; y += 1
            candidate = last_thursday_of_month(date(y, m, 1))
        if candidate == target_expiry:
            return code
    return 1   # fallback


# =============================================================================
# HTTP SESSION
# =============================================================================

def make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "Content-Type": "application/json",
        "Accept":       "application/json",
        "access-token": DHAN_ACCESS_TOKEN,
        "client-id":    DHAN_CLIENT_ID,
    })
    return s

SESSION = make_session()


# =============================================================================
# DATA FETCHING
# =============================================================================

def _post(endpoint: str, payload: dict, timeout: int = 30) -> Optional[dict]:
    try:
        r = SESSION.post(f"{BASE_URL}{endpoint}", json=payload, timeout=timeout)
        r.raise_for_status()
        return r.json()
    except requests.exceptions.HTTPError as e:
        print(f"  HTTP error {r.status_code} on {endpoint}: {r.text[:300]}")
    except Exception as e:
        print(f"  Request error {endpoint}: {e}")
    return None


def fetch_option_data(
    opt_sid: int,
    option_type: str,        # "CALL" or "PUT"
    strike_offset: str,      # "ATM", "ATM+1", etc.
    from_date: str,          # "YYYY-MM-DD"
    to_date: str,
    opt_instrument: str,     # "OPTIDX", "OPTSTK", etc.
    expiry_flag: str,        # "WEEK" or "MONTH"
    expiry_code: int = 1,
    interval: str = "15",
) -> pd.DataFrame:
    """
    Fetch option OHLCV + spot + OI + IV from Dhan rollingoption API.
    Auto-chunks at 29-day windows.
    Returns DataFrame with columns:
      datetime(index), open, high, low, close, volume, oi, iv, spot, strike
    """
    fmt   = "%Y-%m-%d"
    start = datetime.strptime(from_date, fmt)
    end   = datetime.strptime(to_date,   fmt)
    chunk = timedelta(days=MAX_DAYS_PER_REQ)
    frames = []
    cursor = start
    side   = "ce" if option_type == "CALL" else "pe"

    while cursor <= end:
        chunk_end = min(cursor + chunk, end)
        payload = {
            "exchangeSegment": "NSE_FNO",
            "interval":        interval,
            "securityId":      opt_sid,
            "instrument":      opt_instrument,
            "expiryFlag":      expiry_flag,
            "expiryCode":      expiry_code,
            "strike":          strike_offset,
            "drvOptionType":   option_type,
            "requiredData":    ["open","high","low","close","volume","oi","iv","spot","strike"],
            "fromDate":        cursor.strftime(fmt),
            "toDate":          chunk_end.strftime(fmt),
        }
        raw = _post("/charts/rollingoption", payload)
        if raw:
            inner = raw.get("data", {})
            sd    = inner.get(side) or inner.get(side.upper()) or {}
            ts_l  = sd.get("timestamp", [])
            if ts_l:
                cols = {}
                for fld in ["open","high","low","close","volume","oi","iv","spot","strike"]:
                    v = sd.get(fld, [])
                    if v and len(v) == len(ts_l):
                        cols[fld] = v
                if "close" in cols:
                    df = pd.DataFrame(
                        cols,
                        index=pd.to_datetime(ts_l, unit="s", utc=True)
                              .tz_convert("Asia/Kolkata")
                    )
                    df.index.name = "datetime"
                    for c in df.columns:
                        df[c] = pd.to_numeric(df[c], errors="coerce")
                    frames.append(df.dropna(subset=["close"]))
        cursor = chunk_end + timedelta(days=1)
        time.sleep(0.35)

    if not frames:
        return pd.DataFrame()
    out = pd.concat(frames).sort_index()
    return out[~out.index.duplicated(keep="first")]


def fetch_underlying_data(
    eq_sid: str,
    from_date: str,
    to_date: str,
    eq_segment: str = "NSE_EQ",
    eq_instrument: str = "EQUITY",
    interval: str = "15",
) -> pd.DataFrame:
    """
    Fetch underlying OHLCV from Dhan intraday API.
    Used to overlay spot price on the options chart.
    """
    fmt   = "%Y-%m-%d"
    start = datetime.strptime(from_date, fmt)
    end   = datetime.strptime(to_date,   fmt)
    frames, cursor = [], start

    while cursor <= end:
        chunk_end = min(cursor + timedelta(days=89), end)
        payload = {
            "securityId":      eq_sid,
            "exchangeSegment": eq_segment,
            "instrument":      eq_instrument,
            "interval":        interval,
            "oi":              False,
            "fromDate":        cursor.strftime(fmt),
            "toDate":          chunk_end.strftime(fmt),
        }
        raw = _post("/charts/intraday", payload)
        if raw and "open" in raw:
            try:
                df = pd.DataFrame(
                    {"open":raw["open"],"high":raw["high"],
                     "low":raw["low"],"close":raw["close"],"volume":raw["volume"]},
                    index=pd.to_datetime(raw["timestamp"], unit="s", utc=True)
                          .tz_convert("Asia/Kolkata")
                )
                df.index.name = "datetime"
                frames.append(df)
            except Exception as e:
                print(f"  Underlying parse error: {e}")
        cursor = chunk_end + timedelta(days=1)
        time.sleep(0.35)

    if not frames:
        return pd.DataFrame()
    out = pd.concat(frames).sort_index()
    return out[~out.index.duplicated(keep="first")]


# =============================================================================
# FILTER BY EXPIRY DATE AND DAY
# =============================================================================

def filter_by_day(df: pd.DataFrame, day: str) -> pd.DataFrame:
    """Keep only rows where the date matches the given day (YYYY-MM-DD)."""
    if df.empty:
        return df
    target = pd.Timestamp(day).date()
    return df[df.index.date == target]


def filter_by_expiry_range(df: pd.DataFrame, from_date: str, to_date: str) -> pd.DataFrame:
    """Keep rows within [from_date, to_date] inclusive."""
    if df.empty:
        return df
    start = pd.Timestamp(from_date, tz="Asia/Kolkata")
    end   = pd.Timestamp(to_date,   tz="Asia/Kolkata") + pd.Timedelta(hours=23, minutes=59)
    return df[(df.index >= start) & (df.index <= end)]


# =============================================================================
# COMPUTE PRICE LEVELS
# =============================================================================

def compute_levels(df: pd.DataFrame) -> dict:
    """
    Compute key option price levels based on entry premium.
    Entry = first close in the dataset.
    """
    if df.empty or "close" not in df.columns:
        return {}
    entry = float(df["close"].iloc[0])
    return {
        "entry":         entry,
        "sl_40pct":      round(entry * 0.60, 2),    # −40% SL
        "half_exit":     round(entry * 1.30, 2),    # +30% first target (half-exit zone)
        "target_80pct":  round(entry * 1.80, 2),    # +80% full target
        "trail_20pct":   round(entry * 0.80, 2),    # −20% trailing stop marker
    }


# =============================================================================
# HTML CHART GENERATOR
# =============================================================================

def _df_to_js_arrays(df: pd.DataFrame, cols: List[str]) -> dict:
    """Convert DataFrame columns to JavaScript array strings for the chart."""
    out = {}
    for col in cols:
        if col == "datetime":
            vals = [str(ts)[:19].replace("+05:30","") for ts in df.index]
        elif col in df.columns:
            vals = [round(float(v), 4) if pd.notna(v) else None for v in df[col]]
        else:
            vals = []
        out[col] = json.dumps(vals)
    return out


def generate_html_chart(
    ce_df:       Optional[pd.DataFrame],
    pe_df:       Optional[pd.DataFrame],
    spot_df:     Optional[pd.DataFrame],
    symbol:      str,
    opt_type:    str,              # "CE", "PE", or "BOTH"
    strike_str:  str,              # e.g. "ATM"
    expiry_date: str,
    expiry_kind: str,              # "WEEKLY" or "MONTHLY"
    interval:    str,
    levels_ce:   dict,
    levels_pe:   dict,
    day_filter:  Optional[str] = None,
) -> str:
    """
    Build a complete self-contained HTML page with interactive options charts.
    Uses lightweight, CDN-loaded Chart.js — no server required.
    Returns the full HTML string.
    """
    title = f"{symbol} {opt_type} Options Chart — {strike_str} | Expiry: {expiry_date} ({expiry_kind})"
    if day_filter:
        title += f" | Day: {day_filter}"

    def _safe(df):
        return df if df is not None and not df.empty else pd.DataFrame()

    def _ohlc_arrays(df):
        if df.empty:
            return "[]", "[]", "[]", "[]", "[]", "[]", "[]", "[]", "[]", "[]"
        lbl  = json.dumps([str(ts)[:19].replace("+05:30","") for ts in df.index])
        o    = json.dumps([round(float(v),2) if pd.notna(v) else None for v in df.get("open",  [])])
        h    = json.dumps([round(float(v),2) if pd.notna(v) else None for v in df.get("high",  [])])
        lo   = json.dumps([round(float(v),2) if pd.notna(v) else None for v in df.get("low",   [])])
        cl   = json.dumps([round(float(v),2) if pd.notna(v) else None for v in df.get("close", [])])
        vol  = json.dumps([int(v) if pd.notna(v) else 0 for v in df.get("volume", [])])
        iv   = json.dumps([round(float(v),2) if pd.notna(v) else None for v in df.get("iv",    df.get("close")*0)])
        oi   = json.dumps([round(float(v),2) if pd.notna(v) else None for v in df.get("oi",    [])])
        spot = json.dumps([round(float(v),2) if pd.notna(v) else None for v in df.get("spot",  [])])
        sk   = json.dumps([round(float(v),2) if pd.notna(v) else None for v in df.get("strike",[])])
        return lbl, o, h, lo, cl, vol, iv, oi, spot, sk

    ce  = _safe(ce_df)
    pe  = _safe(pe_df)
    sp  = _safe(spot_df)

    ce_lbl, ce_o, ce_h, ce_l, ce_c, ce_vol, ce_iv, ce_oi, ce_spot, ce_sk = _ohlc_arrays(ce)
    pe_lbl, pe_o, pe_h, pe_l, pe_c, pe_vol, pe_iv, pe_oi, pe_spot, pe_sk = _ohlc_arrays(pe)

    sp_lbl  = json.dumps([str(ts)[:19].replace("+05:30","") for ts in sp.index]) if not sp.empty else "[]"
    sp_cl   = json.dumps([round(float(v),2) for v in sp["close"]]) if not sp.empty and "close" in sp.columns else "[]"

    lv_ce = json.dumps(levels_ce)
    lv_pe = json.dumps(levels_pe)

    chart_cols  = 2 if opt_type == "BOTH" else 1
    ce_display  = "block" if opt_type in ("CE","BOTH") else "none"
    pe_display  = "block" if opt_type in ("PE","BOTH") else "none"

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>{title}</title>
<script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.js"></script>
<script src="https://cdnjs.cloudflare.com/ajax/libs/chartjs-plugin-zoom/2.0.1/chartjs-plugin-zoom.min.js"></script>
<script src="https://cdnjs.cloudflare.com/ajax/libs/hammer.js/2.0.8/hammer.min.js"></script>
<style>
  * {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{ background: #0d1117; color: #e6edf3; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", monospace; font-size: 13px; }}
  .header {{ background: #161b22; border-bottom: 1px solid #30363d; padding: 14px 20px; }}
  .header h1 {{ font-size: 16px; font-weight: 600; color: #58a6ff; margin-bottom: 4px; }}
  .header .meta {{ color: #8b949e; font-size: 12px; }}
  .meta span {{ margin-right: 18px; }}
  .meta .we {{ color: #f0883e; }}
  .meta .mo {{ color: #56d364; }}
  .controls {{ background: #161b22; border-bottom: 1px solid #30363d; padding: 10px 20px;
               display: flex; gap: 10px; flex-wrap: wrap; align-items: center; }}
  .btn {{ padding: 5px 14px; background: #21262d; border: 1px solid #30363d;
          border-radius: 6px; color: #c9d1d9; cursor: pointer; font-size: 12px; }}
  .btn:hover {{ background: #30363d; color: #58a6ff; }}
  .btn.active {{ background: #1f6feb; border-color: #388bfd; color: #fff; }}
  .grid {{ display: grid; grid-template-columns: repeat({chart_cols}, 1fr);
           gap: 12px; padding: 14px; }}
  .panel {{ background: #161b22; border: 1px solid #30363d; border-radius: 8px;
            padding: 12px; }}
  .panel-title {{ font-size: 13px; font-weight: 600; margin-bottom: 10px; }}
  .panel-title .ce-label {{ color: #3fb950; }}
  .panel-title .pe-label {{ color: #f85149; }}
  .panel-title .sp-label {{ color: #58a6ff; }}
  .chart-area {{ position: relative; height: 320px; }}
  .vol-area  {{ position: relative; height: 100px; margin-top: 6px; }}
  .oi-area   {{ position: relative; height: 90px;  margin-top: 6px; }}
  .iv-area   {{ position: relative; height: 90px;  margin-top: 6px; }}
  .levels-box {{ margin-top: 10px; padding: 8px 12px; background: #0d1117;
                 border: 1px solid #30363d; border-radius: 6px; font-size: 11px;
                 display: flex; flex-wrap: wrap; gap: 14px; }}
  .lv {{ display: flex; flex-direction: column; }}
  .lv span:first-child {{ color: #8b949e; margin-bottom: 2px; }}
  .lv span:last-child  {{ font-weight: 600; font-variant-numeric: tabular-nums; }}
  .lv .entry  {{ color: #f0883e; }}
  .lv .sl     {{ color: #f85149; }}
  .lv .half   {{ color: #e3b341; }}
  .lv .target {{ color: #3fb950; }}
  .lv .trail  {{ color: #a5d6ff; }}
  .info-row {{ margin-top: 10px; color: #8b949e; font-size: 11px; }}
  .stats-grid {{ display: grid; grid-template-columns: repeat({chart_cols}, 1fr);
                 gap: 12px; padding: 0 14px 14px; }}
  .stat-box {{ background: #161b22; border: 1px solid #30363d; border-radius: 8px;
               padding: 10px 14px; display: flex; flex-wrap: wrap; gap: 14px; }}
</style>
</head>
<body>

<div class="header">
  <h1>⚡ {title}</h1>
  <div class="meta">
    <span>📅 Expiry: <strong>{expiry_date}</strong></span>
    <span class="{'we' if expiry_kind=='WEEKLY' else 'mo'}">{'🗓 WEEKLY' if expiry_kind=='WEEKLY' else '📆 MONTHLY'}</span>
    <span>⏱ Interval: {interval}m</span>
    <span>🎯 Strike: {strike_str}</span>
    {'<span>📌 Day filter: <strong>' + day_filter + '</strong></span>' if day_filter else ''}
  </div>
</div>

<div class="controls">
  <span style="color:#8b949e;font-size:12px;">Zoom/Pan:</span>
  <button class="btn" onclick="resetAll()">↺ Reset zoom</button>
  <button class="btn" onclick="zoomAll('x')">📐 Zoom X only</button>
  <button class="btn" onclick="zoomAll('xy')">🔍 Zoom XY</button>
  <span style="margin-left:10px;color:#8b949e;font-size:11px;">
    Scroll to zoom • Drag to pan • Double-click to reset
  </span>
</div>

<div class="grid">

  <!-- CE PANEL -->
  <div class="panel" style="display:{ce_display}" id="ce-panel">
    <div class="panel-title"><span class="ce-label">▲ CALL (CE)</span> — {symbol} {strike_str}</div>
    <div class="chart-area"><canvas id="ce-price"></canvas></div>
    <div class="vol-area"><canvas id="ce-vol"></canvas></div>
    <div class="oi-area"><canvas id="ce-oi"></canvas></div>
    <div class="iv-area"><canvas id="ce-iv"></canvas></div>
    <div class="levels-box" id="ce-levels"></div>
    <div class="info-row" id="ce-info"></div>
  </div>

  <!-- PE PANEL -->
  <div class="panel" style="display:{pe_display}" id="pe-panel">
    <div class="panel-title"><span class="pe-label">▼ PUT (PE)</span> — {symbol} {strike_str}</div>
    <div class="chart-area"><canvas id="pe-price"></canvas></div>
    <div class="vol-area"><canvas id="pe-vol"></canvas></div>
    <div class="oi-area"><canvas id="pe-oi"></canvas></div>
    <div class="iv-area"><canvas id="pe-iv"></canvas></div>
    <div class="levels-box" id="pe-levels"></div>
    <div class="info-row" id="pe-info"></div>
  </div>

</div>

<!-- Underlying spot row -->
<div class="stats-grid" id="spot-row">
  <div class="stat-box" style="grid-column: 1 / -1">
    <div style="color:#58a6ff;font-weight:600;width:100%;margin-bottom:4px;">
      📈 {symbol} Underlying Spot Price
    </div>
    <div style="position:relative;width:100%;height:130px">
      <canvas id="spot-chart"></canvas>
    </div>
  </div>
</div>

<script>
// =============================================================================
//  DATA
// =============================================================================
const CE = {{
  labels: {ce_lbl}, open: {ce_o}, high: {ce_h}, low: {ce_l}, close: {ce_c},
  volume: {ce_vol}, iv: {ce_iv}, oi: {ce_oi}, spot: {ce_spot}, strike: {ce_sk}
}};
const PE = {{
  labels: {pe_lbl}, open: {pe_o}, high: {pe_h}, low: {pe_l}, close: {pe_c},
  volume: {pe_vol}, iv: {pe_iv}, oi: {pe_oi}, spot: {pe_spot}, strike: {pe_sk}
}};
const SPOT = {{ labels: {sp_lbl}, close: {sp_cl} }};
const LV_CE = {lv_ce};
const LV_PE = {lv_pe};

const CHARTS = [];

// =============================================================================
//  COLOUR HELPERS
// =============================================================================
function bullishColor(o, c)  {{ return c >= o ? '#3fb950' : '#f85149'; }}
function alphaColor(hex, a)  {{ return hex + Math.round(a*255).toString(16).padStart(2,'0'); }}

// =============================================================================
//  CANDLESTICK PLUGIN  (inline — no extra dependency)
// =============================================================================
const CandlestickPlugin = {{
  id: 'candlestick',
  beforeDatasetsDraw(chart) {{
    const {{ctx, scales: {{x, y}}}} = chart;
    const meta = chart.getDatasetMeta(0);
    if (!meta || !meta.data.length) return;
    const ds = chart.data.datasets[0];
    ctx.save();
    meta.data.forEach((bar, i) => {{
      const o = ds.data[i]?.o ?? 0, h = ds.data[i]?.h ?? 0;
      const l = ds.data[i]?.l ?? 0, c = ds.data[i]?.c ?? 0;
      const xPos = bar.x;
      const yO = y.getPixelForValue(o), yH = y.getPixelForValue(h);
      const yL = y.getPixelForValue(l), yC = y.getPixelForValue(c);
      const w  = Math.max(2, (x.getPixelForValue(i+1) - x.getPixelForValue(i)) * 0.65);
      const col = c >= o ? '#3fb950' : '#f85149';
      ctx.strokeStyle = col; ctx.fillStyle = col; ctx.lineWidth = 1;
      // Wick
      ctx.beginPath(); ctx.moveTo(xPos, yH); ctx.lineTo(xPos, yL); ctx.stroke();
      // Body
      const bodyTop = Math.min(yO, yC), bodyH = Math.max(1, Math.abs(yO - yC));
      ctx.fillRect(xPos - w/2, bodyTop, w, bodyH);
    }});
    ctx.restore();
  }}
}};
Chart.register(CandlestickPlugin);

// =============================================================================
//  LEVEL ANNOTATIONS
// =============================================================================
function levelDatasets(lv, color_entry='#f0883e') {{
  if (!lv || !lv.entry) return [];
  const n = CE.labels.length || PE.labels.length || 1;
  const line = (val, lbl, col, dash=[]) => ({{
    label: lbl, data: Array(n).fill(val),
    borderColor: col, borderWidth: 1.2, borderDash: dash,
    pointRadius: 0, fill: false, tension: 0,
  }});
  return [
    line(lv.entry,       `Entry ₹${{lv.entry}}`,       '#f0883e', []),
    line(lv.sl_40pct,    `SL −40% ₹${{lv.sl_40pct}}`,  '#f85149', [6,3]),
    line(lv.half_exit,   `Half-exit ₹${{lv.half_exit}}`,'#e3b341', [4,2]),
    line(lv.target_80pct,`Target ₹${{lv.target_80pct}}`,'#3fb950', [6,3]),
    line(lv.trail_20pct, `Trail ₹${{lv.trail_20pct}}`,  '#a5d6ff', [3,3]),
  ];
}}

// =============================================================================
//  BUILD PRICE CHART (candlestick + levels + spot overlay)
// =============================================================================
function buildPriceChart(canvasId, d, lv, spotLabels, spotClose) {{
  if (!d.labels || d.labels.length === 0) return null;

  const ohlc = d.labels.map((t, i) => ({{ x: t, o: d.open[i], h: d.high[i], l: d.low[i], c: d.close[i] }}));
  const datasets = [
    {{
      type: 'line', label: 'OHLC', data: d.close.map((c,i)=>c),
      borderColor: 'transparent', pointRadius: 0, fill: false,
      _ohlc: ohlc,   // hidden — used by plugin
    }},
    ...levelDatasets(lv),
  ];
  // Override first dataset for candlestick rendering
  datasets[0].data = ohlc.map(o => o.c);

  // Spot overlay if data available
  if (spotClose && spotClose.length) {{
    // Resample spot to match option labels (forward-fill)
    const spotMap = {{}};
    spotLabels.forEach((t,i) => {{ spotMap[t] = spotClose[i]; }});
    const spotData = d.labels.map(t => {{
      // find nearest spot
      if (spotMap[t] !== undefined) return spotMap[t];
      return null;
    }});
    datasets.push({{
      label: 'Spot', data: spotData, borderColor: '#58a6ff88',
      borderWidth: 1, pointRadius: 0, fill: false, tension: 0.3,
      yAxisID: 'y2',
    }});
  }}

  const ch = new Chart(document.getElementById(canvasId), {{
    type: 'line',
    data: {{ labels: d.labels, datasets }},
    options: {{
      responsive: true, maintainAspectRatio: false, animation: false,
      interaction: {{ mode: 'index', intersect: false }},
      plugins: {{
        legend: {{ display: true, position: 'top',
                   labels: {{ color:'#8b949e', font:{{size:10}}, boxWidth:12 }} }},
        tooltip: {{
          backgroundColor: '#1c2128', titleColor: '#e6edf3',
          bodyColor: '#8b949e', borderColor:'#30363d', borderWidth:1,
          callbacks: {{
            label(ctx) {{
              const ds = ctx.dataset;
              if (ctx.datasetIndex === 0) {{
                const oc = d.labels.map((t,i)=>({{t,o:d.open[i],h:d.high[i],l:d.low[i],c:d.close[i]}}))[ctx.dataIndex];
                return oc ? [`O:₹${{oc.o}} H:₹${{oc.h}} L:₹${{oc.l}} C:₹${{oc.c}}`] : [];
              }}
              return `${{ds.label}}: ₹${{ctx.parsed.y?.toFixed(2)}}`;
            }},
            title(items) {{
              const sk = d.strike?.[items[0]?.dataIndex];
              return [items[0]?.label, sk ? `Strike: ₹${{sk}}` : ''];
            }}
          }}
        }},
        zoom: {{
          pan: {{ enabled:true, mode:'x' }},
          zoom: {{ wheel:{{enabled:true}}, pinch:{{enabled:true}}, mode:'x',
                   onZoomComplete({{chart}}){{ chart.update('none'); }} }},
        }},
      }},
      scales: {{
        x: {{ ticks:{{ color:'#8b949e',font:{{size:10}},maxTicksLimit:12,autoSkip:true }},
               grid:{{ color:'#21262d' }} }},
        y: {{ ticks:{{ color:'#8b949e',font:{{size:10}},
                       callback:v=>`₹${{v?.toFixed(1)}}` }},
               grid:{{ color:'#21262d' }}, position:'left' }},
        y2: {{ ticks:{{ color:'#58a6ff88',font:{{size:9}},
                        callback:v=>`${{v?.toFixed(0)}}` }},
               grid:{{ display:false }}, position:'right', display: spotClose && spotClose.length > 0 }},
      }},
    }},
  }});
  CHARTS.push(ch);
  return ch;
}}

// =============================================================================
//  VOLUME BARS
// =============================================================================
function buildVolChart(canvasId, d) {{
  if (!d.labels || !d.labels.length) return;
  const colors = d.close.map((c,i) => c >= (d.open[i]||c) ? '#3fb95066' : '#f8514966');
  const ch = new Chart(document.getElementById(canvasId), {{
    type: 'bar',
    data: {{ labels: d.labels,
             datasets: [{{ label:'Volume', data:d.volume, backgroundColor:colors,
                           borderWidth:0 }}] }},
    options: {{
      responsive:true, maintainAspectRatio:false, animation:false,
      plugins:{{ legend:{{display:false}},
                 tooltip:{{ backgroundColor:'#1c2128', bodyColor:'#8b949e',
                            callbacks:{{ label:ctx=>`Vol: ${{ctx.parsed.y?.toLocaleString()}}` }} }},
                 zoom:{{ pan:{{enabled:true,mode:'x'}},
                         zoom:{{wheel:{{enabled:true}},pinch:{{enabled:true}},mode:'x'}} }} }},
      scales:{{
        x:{{display:false}},
        y:{{ticks:{{color:'#8b949e',font:{{size:9}},
                   callback:v=>v>=1e6?`${{(v/1e6).toFixed(1)}}M`:v>=1e3?`${{(v/1e3).toFixed(0)}}K`:v}},
            grid:{{color:'#21262d'}}}},
      }},
    }},
  }});
  CHARTS.push(ch);
}}

// =============================================================================
//  OI BARS
// =============================================================================
function buildOIChart(canvasId, d) {{
  if (!d.labels || !d.oi || !d.oi.length) return;
  const oiChange = d.oi.map((v,i) => i===0 ? 0 : (v||0) - (d.oi[i-1]||0));
  const colors   = oiChange.map(v => v >= 0 ? '#388bfd66' : '#f0883e66');
  const ch = new Chart(document.getElementById(canvasId), {{
    type: 'bar',
    data: {{ labels: d.labels,
             datasets: [{{ label:'OI Change', data:oiChange, backgroundColor:colors,
                           borderWidth:0 }}] }},
    options: {{
      responsive:true, maintainAspectRatio:false, animation:false,
      plugins:{{ legend:{{display:true,labels:{{color:'#8b949e',font:{{size:9}},boxWidth:8}}}},
                 tooltip:{{ backgroundColor:'#1c2128', bodyColor:'#8b949e',
                            callbacks:{{ label:ctx=>`OI Chg: ${{ctx.parsed.y?.toLocaleString()}}` }} }},
                 zoom:{{ pan:{{enabled:true,mode:'x'}},
                         zoom:{{wheel:{{enabled:true}},pinch:{{enabled:true}},mode:'x'}} }} }},
      scales:{{
        x:{{display:false}},
        y:{{ticks:{{color:'#8b949e',font:{{size:9}}}}, grid:{{color:'#21262d'}}}},
      }},
    }},
  }});
  CHARTS.push(ch);
}}

// =============================================================================
//  IV LINE
// =============================================================================
function buildIVChart(canvasId, d, color) {{
  if (!d.labels || !d.iv || !d.iv.length || d.iv.every(v=>v===null)) return;
  const ch = new Chart(document.getElementById(canvasId), {{
    type: 'line',
    data: {{ labels: d.labels,
             datasets: [{{ label:'IV %', data:d.iv, borderColor:color, borderWidth:1.5,
                           pointRadius:0, fill:false, tension:0.3, spanGaps:true }}] }},
    options: {{
      responsive:true, maintainAspectRatio:false, animation:false,
      plugins:{{ legend:{{display:true,labels:{{color:'#8b949e',font:{{size:9}},boxWidth:8}}}},
                 tooltip:{{ backgroundColor:'#1c2128', bodyColor:'#8b949e',
                            callbacks:{{ label:ctx=>`IV: ${{ctx.parsed.y?.toFixed(2)}}%` }} }},
                 zoom:{{ pan:{{enabled:true,mode:'x'}},
                         zoom:{{wheel:{{enabled:true}},pinch:{{enabled:true}},mode:'x'}} }} }},
      scales:{{
        x:{{ticks:{{color:'#8b949e',font:{{size:9}},maxTicksLimit:8,autoSkip:true}},
             grid:{{color:'#21262d'}}}},
        y:{{ticks:{{color:'#8b949e',font:{{size:9}},callback:v=>`${{v?.toFixed(1)}}%`}},
             grid:{{color:'#21262d'}}}},
      }},
    }},
  }});
  CHARTS.push(ch);
}}

// =============================================================================
//  SPOT CHART
// =============================================================================
function buildSpotChart() {{
  if (!SPOT.labels || !SPOT.labels.length) {{
    document.getElementById('spot-row').style.display = 'none';
    return;
  }}
  const ch = new Chart(document.getElementById('spot-chart'), {{
    type: 'line',
    data: {{ labels: SPOT.labels,
             datasets: [{{ label:'{symbol} Spot', data: SPOT.close,
                           borderColor:'#58a6ff', borderWidth:1.5,
                           pointRadius:0, fill:false, tension:0.3 }}] }},
    options: {{
      responsive:true, maintainAspectRatio:false, animation:false,
      plugins:{{ legend:{{display:false}},
                 tooltip:{{ backgroundColor:'#1c2128', bodyColor:'#8b949e',
                            callbacks:{{ label:ctx=>`Spot: ₹${{ctx.parsed.y?.toFixed(2)}}` }} }},
                 zoom:{{ pan:{{enabled:true,mode:'x'}},
                         zoom:{{wheel:{{enabled:true}},pinch:{{enabled:true}},mode:'x'}} }} }},
      scales:{{
        x:{{ticks:{{color:'#8b949e',font:{{size:9}},maxTicksLimit:14,autoSkip:true}},
             grid:{{color:'#21262d'}}}},
        y:{{ticks:{{color:'#8b949e',font:{{size:9}},callback:v=>`₹${{v?.toFixed(0)}}`}},
             grid:{{color:'#21262d'}}}},
      }},
    }},
  }});
  CHARTS.push(ch);
}}

// =============================================================================
//  LEVEL BOXES
// =============================================================================
function renderLevels(boxId, lv, infoId, d) {{
  const box = document.getElementById(boxId);
  if (!lv || !lv.entry || !box) return;
  box.innerHTML = `
    <div class="lv"><span>Entry</span><span class="entry">₹${{lv.entry}}</span></div>
    <div class="lv"><span>SL −40%</span><span class="sl">₹${{lv.sl_40pct}}</span></div>
    <div class="lv"><span>Half-exit +30%</span><span class="half">₹${{lv.half_exit}}</span></div>
    <div class="lv"><span>Target +80%</span><span class="target">₹${{lv.target_80pct}}</span></div>
    <div class="lv"><span>Trail −20%</span><span class="trail">₹${{lv.trail_20pct}}</span></div>
  `;
  const info = document.getElementById(infoId);
  if (info && d.labels) {{
    const high = Math.max(...d.high.filter(v=>v));
    const low  = Math.min(...d.low.filter(v=>v));
    const chg  = d.close.length ? (d.close.at(-1) - d.close[0]).toFixed(2) : 0;
    const chgP = d.close.length && d.close[0] ? ((chg/d.close[0])*100).toFixed(1) : 0;
    const chgColor = chg >= 0 ? '#3fb950' : '#f85149';
    info.innerHTML = `Bars: ${{d.labels.length}} &nbsp;|&nbsp; High: ₹${{high?.toFixed(2)}} &nbsp;|&nbsp;
      Low: ₹${{low?.toFixed(2)}} &nbsp;|&nbsp;
      <span style="color:${{chgColor}}">Δ ₹${{chg}} (${{chgP}}%)</span>`;
  }}
}}

// =============================================================================
//  RESET / ZOOM CONTROLS
// =============================================================================
function resetAll() {{ CHARTS.forEach(c => c.resetZoom && c.resetZoom()); }}
function zoomAll(mode) {{
  CHARTS.forEach(c => {{
    if (c.options.plugins.zoom) {{
      c.options.plugins.zoom.zoom.mode = mode;
      c.update('none');
    }}
  }});
}}

// =============================================================================
//  INIT
// =============================================================================
window.addEventListener('DOMContentLoaded', () => {{
  // CE charts
  if ({str(opt_type in ('CE','BOTH')).lower()}) {{
    buildPriceChart('ce-price', CE, LV_CE, SPOT.labels, SPOT.close);
    buildVolChart('ce-vol', CE);
    buildOIChart('ce-oi', CE);
    buildIVChart('ce-iv', CE, '#3fb950');
    renderLevels('ce-levels', LV_CE, 'ce-info', CE);
  }}
  // PE charts
  if ({str(opt_type in ('PE','BOTH')).lower()}) {{
    buildPriceChart('pe-price', PE, LV_PE, SPOT.labels, SPOT.close);
    buildVolChart('pe-vol', PE);
    buildOIChart('pe-oi', PE);
    buildIVChart('pe-iv', PE, '#f85149');
    renderLevels('pe-levels', LV_PE, 'pe-info', PE);
  }}
  // Spot
  buildSpotChart();
}});
</script>
</body>
</html>"""
    return html


# =============================================================================
# SAVE AND OPEN
# =============================================================================

def save_and_open(html: str, filename: str) -> str:
    """Save HTML to file and open in the default browser."""
    with open(filename, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"\n  ✔ Chart saved: {filename}")
    try:
        webbrowser.open(f"file://{os.path.abspath(filename)}")
        print(f"  ✔ Opened in browser")
    except Exception as e:
        print(f"  ⚠ Could not auto-open browser: {e}")
        print(f"  → Open manually: {os.path.abspath(filename)}")
    return filename


# =============================================================================
# MAIN ORCHESTRATOR
# =============================================================================

def run_chart(
    symbol:       str,
    opt_type:     str,           # "CE", "PE", or "BOTH"
    strike:       str = "ATM",
    expiry:       Optional[str] = None,   # "YYYY-MM-DD" — if given, derives date range
    day:          Optional[str] = None,   # "YYYY-MM-DD" — show just this day
    from_date:    Optional[str] = None,
    to_date:      Optional[str] = None,
    interval:     str = "15",
    output_file:  Optional[str] = None,
):
    symbol = symbol.upper()
    if symbol not in INSTRUMENTS:
        print(f"\n  ERROR: '{symbol}' not in instrument registry.")
        print(f"  Supported: {', '.join(sorted(INSTRUMENTS.keys()))}")
        sys.exit(1)

    info        = INSTRUMENTS[symbol]
    eflag       = info["expiry_flag"]
    expiry_kind = "WEEKLY" if eflag.upper() in ("WEEK","WEEKLY") else "MONTHLY"
    strike_api  = STRIKE_OFFSETS.get(strike, "ATM")

    # ── Resolve date range ────────────────────────────────────────────────────
    today = date.today()

    if expiry:
        # --expiry given: fetch data around that expiry
        exp_date    = datetime.strptime(expiry, "%Y-%m-%d").date()
        expiry_str  = expiry
        # Determine expiry code by comparing to front expiry of a ref date
        # Use one week/month before expiry as reference
        if expiry_kind == "WEEKLY":
            ref_date  = exp_date - timedelta(days=7)
            from_d    = (exp_date - timedelta(days=7)).strftime("%Y-%m-%d")
        else:
            ref_date  = date(exp_date.year, exp_date.month, 1)
            from_d    = ref_date.strftime("%Y-%m-%d")
        to_d         = expiry_str
        exp_code     = expiry_code_for_date(exp_date, ref_date, eflag)

    elif day:
        # --day given: show only that specific day, auto-detect expiry
        day_date    = datetime.strptime(day, "%Y-%m-%d").date()
        exp_date    = derive_expiry(day_date, eflag)
        expiry_str  = exp_date.strftime("%Y-%m-%d")
        from_d      = day
        to_d        = day
        exp_code    = expiry_code_for_date(exp_date, day_date, eflag)

    elif from_date and to_date:
        # --from/--to given explicitly
        from_d      = from_date
        to_d        = to_date
        ref         = datetime.strptime(from_date, "%Y-%m-%d").date()
        exp_date    = derive_expiry(ref, eflag)
        expiry_str  = exp_date.strftime("%Y-%m-%d")
        exp_code    = expiry_code_for_date(exp_date, ref, eflag)

    else:
        # Default: today's expiry, today's data
        exp_date    = derive_expiry(today, eflag)
        expiry_str  = exp_date.strftime("%Y-%m-%d")
        from_d      = today.strftime("%Y-%m-%d")
        to_d        = today.strftime("%Y-%m-%d")
        exp_code    = 1

    print(f"\n{'─'*55}")
    print(f"  Symbol      : {symbol}")
    print(f"  Option type : {opt_type}")
    print(f"  Strike      : {strike} ({strike_api})")
    print(f"  Expiry      : {expiry_str} ({expiry_kind}, code={exp_code})")
    print(f"  Date range  : {from_d} → {to_d}")
    print(f"  Interval    : {interval}m")
    print(f"{'─'*55}\n")

    # ── Fetch data ────────────────────────────────────────────────────────────
    ce_df = pe_df = pd.DataFrame()

    if opt_type in ("CE", "BOTH"):
        print(f"  Fetching CE data ...")
        ce_df = fetch_option_data(
            info["opt_sid"], "CALL", strike_api, from_d, to_d,
            opt_instrument=info["opt_instr"],
            expiry_flag=eflag, expiry_code=exp_code, interval=interval,
        )
        if ce_df.empty:
            print(f"  ⚠ No CE data returned.")
        else:
            print(f"  ✔ CE: {len(ce_df)} bars  |  "
                  f"price range ₹{ce_df['close'].min():.2f}–₹{ce_df['close'].max():.2f}")

    if opt_type in ("PE", "BOTH"):
        time.sleep(0.5)
        print(f"  Fetching PE data ...")
        # PE strike is on the opposite side
        pe_strike = PE_OFFSETS.get(strike, "ATM") if opt_type == "BOTH" else strike_api
        pe_df = fetch_option_data(
            info["opt_sid"], "PUT", pe_strike, from_d, to_d,
            opt_instrument=info["opt_instr"],
            expiry_flag=eflag, expiry_code=exp_code, interval=interval,
        )
        if pe_df.empty:
            print(f"  ⚠ No PE data returned.")
        else:
            print(f"  ✔ PE: {len(pe_df)} bars  |  "
                  f"price range ₹{pe_df['close'].min():.2f}–₹{pe_df['close'].max():.2f}")

    # ── Fetch underlying spot ─────────────────────────────────────────────────
    time.sleep(0.4)
    print(f"  Fetching underlying spot data ...")
    spot_df = fetch_underlying_data(
        info["eq_sid"], from_d, to_d,
        eq_segment=info["eq_seg"], eq_instrument=info["eq_instr"],
        interval=interval,
    )
    if spot_df.empty:
        print(f"  ⚠ No spot data returned.")
    else:
        print(f"  ✔ Spot: {len(spot_df)} bars  |  "
              f"range ₹{spot_df['close'].min():.2f}–₹{spot_df['close'].max():.2f}")

    # ── Apply day filter ──────────────────────────────────────────────────────
    if day:
        ce_df   = filter_by_day(ce_df,   day)
        pe_df   = filter_by_day(pe_df,   day)
        spot_df = filter_by_day(spot_df, day)
        print(f"  After day filter ({day}): CE={len(ce_df)} PE={len(pe_df)} Spot={len(spot_df)}")

    if ce_df.empty and pe_df.empty:
        print("\n  ERROR: No data to chart after filtering.")
        print("  Possible reasons:")
        print("    • Market was closed on the selected day")
        print("    • Expiry code mismatch — try --expiry instead of --day")
        print("    • Token expired — update DHAN_ACCESS_TOKEN")
        sys.exit(1)

    # ── Compute price levels ──────────────────────────────────────────────────
    levels_ce = compute_levels(ce_df)
    levels_pe = compute_levels(pe_df)

    # ── Generate HTML ─────────────────────────────────────────────────────────
    print(f"\n  Building chart ...")
    html = generate_html_chart(
        ce_df=ce_df, pe_df=pe_df, spot_df=spot_df,
        symbol=symbol, opt_type=opt_type,
        strike_str=strike, expiry_date=expiry_str,
        expiry_kind=expiry_kind, interval=interval,
        levels_ce=levels_ce, levels_pe=levels_pe,
        day_filter=day,
    )

    # ── Save + open ────────────────────────────────────────────────────────────
    if not output_file:
        tag = day or f"{from_d}_to_{to_d}"
        output_file = f"{symbol.lower()}_{opt_type.lower()}_{tag}.html"
    save_and_open(html, output_file)


# =============================================================================
# CLI ENTRY POINT
# =============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Options Chart Generator — Dhan API",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
EXAMPLES — by expiry date:
  python options_chart.py --symbol NIFTY     --type CE   --expiry 2025-06-05
  python options_chart.py --symbol BANKNIFTY --type PE   --expiry 2025-05-29 --strike ATM+1
  python options_chart.py --symbol RELIANCE  --type BOTH --expiry 2025-05-29

EXAMPLES — by specific day (expiry auto-detected):
  python options_chart.py --symbol NIFTY     --type CE   --day 2025-05-12
  python options_chart.py --symbol BANKNIFTY --type BOTH --day 2025-05-07
  python options_chart.py --symbol HDFCBANK  --type PE   --day 2025-04-10

EXAMPLES — date range:
  python options_chart.py --symbol NIFTY    --type CE   --from 2025-04-01 --to 2025-04-30
  python options_chart.py --symbol RELIANCE --type BOTH --from 2025-03-01 --to 2025-03-31

EXAMPLES — with interval:
  python options_chart.py --symbol NIFTY --type BOTH --day 2025-05-07 --interval 5
  python options_chart.py --symbol NIFTY --type CE   --expiry 2025-06-05 --interval 60

STRIKE OPTIONS:
  ATM (default), ATM+1, ATM+2, ATM-1, ATM-2, OTM1, OTM2, ITM1, ITM2

SUPPORTED SYMBOLS:
  Indices  : NIFTY, BANKNIFTY, SENSEX, MIDCPNIFTY, FINNIFTY
  Stocks   : HDFCBANK, RELIANCE, INFY, TCS, ICICIBANK, SBIN, BAJFINANCE,
             AXISBANK, WIPRO, TITAN, TATAMOTORS, HCLTECH, DRREDDY, MARUTI
  Currency : USDINR
  Commodity: GOLD, CRUDEOIL
        """
    )
    parser.add_argument("--symbol",   required=True,  help="Instrument name (e.g. NIFTY, RELIANCE)")
    parser.add_argument("--type",     required=True,  choices=["CE","PE","BOTH"],
                        help="CE = Call, PE = Put, BOTH = side-by-side")
    parser.add_argument("--strike",   default="ATM",  help="Strike offset (default: ATM)")
    parser.add_argument("--expiry",   default=None,   help="Expiry date YYYY-MM-DD")
    parser.add_argument("--day",      default=None,   help="Single trading day YYYY-MM-DD")
    parser.add_argument("--from",     dest="from_date", default=None, help="From date YYYY-MM-DD")
    parser.add_argument("--to",       dest="to_date",   default=None, help="To date YYYY-MM-DD")
    parser.add_argument("--interval", default="15",   choices=["1","3","5","15","25","60"],
                        help="Candle interval in minutes (default: 15)")
    parser.add_argument("--output",   default=None,   help="Output HTML filename")
    args = parser.parse_args()

    # Validate: at least one date spec required
    if not any([args.expiry, args.day, args.from_date]):
        print("\n  ERROR: Provide at least one of: --expiry, --day, or --from/--to")
        parser.print_help()
        sys.exit(1)

    run_chart(
        symbol      = args.symbol,
        opt_type    = args.type,
        strike      = args.strike,
        expiry      = args.expiry,
        day         = args.day,
        from_date   = args.from_date,
        to_date     = args.to_date,
        interval    = args.interval,
        output_file = args.output,
    )


if __name__ == "__main__":
    main()
