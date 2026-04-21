#!/usr/bin/env python3
# =============================================================================
# dhan_trading_system.py  —  Unified Options Trading System
# =============================================================================
#
# Supports three modes, all sharing the same strategy and signal logic:
#   1. BACKTEST    — historical simulation using Dhan rollingoption data
#   2. PAPER       — live market scanning, no real orders placed
#   3. LIVE        — real order execution via Dhan v2 API (dhanhq SDK)
#
# INSTRUMENTS COVERED:
#   • NSE Equity stocks (OPTSTK)
#   • NSE Indices      (NIFTY, BANKNIFTY  — OPTIDX, weekly)
#   • BSE Index        (SENSEX            — OPTIDX, weekly)
#   • NSE Currency     (USDINR, EURINR, GBPINR, JPYINR — OPTCUR, monthly)
#   • MCX Commodity    (GOLD, SILVER, CRUDEOIL, NATURALGAS — OPTFUT, monthly)
#
# INSTALL:
#   pip install requests pandas numpy schedule pyotp dhanhq
#
# USAGE:
#   # Backtest — indices
#   python dhan_trading_system.py --mode backtest --universe index --from 2023-01-01 --to 2026-12-31 --strike ATM
#
#   # Backtest — currency + commodities
#   python dhan_trading_system.py --mode backtest --universe currency commodity --from 2023-01-01 --to 2024-12-31
#
#   # Backtest — everything
#   python dhan_trading_system.py --mode backtest --universe stocks index currency commodity
#
#   # Paper trading — all universes
#   python dhan_trading_system.py --mode paper --universe index currency
#
#   # Live trading — indices only (REAL MONEY — use with caution)
#   python dhan_trading_system.py --mode live --universe index --strike ATM --capital 100000
#
# CREDENTIALS:
#   Set env vars DHAN_CLIENT_ID and DHAN_ACCESS_TOKEN, or use TOTP auto-login.
#   For TOTP: set DHAN_PIN and DHAN_TOTP_SECRET env vars as well.
#
# =============================================================================

import os, sys, math, time, json, logging, argparse, threading
from datetime import datetime, timedelta, date
from dataclasses import dataclass, field
from typing import Optional, List, Dict

import requests
import pandas as pd
import numpy as np


# ─── CREDENTIALS ──────────────────────────────────────────────────────────────
DHAN_CLIENT_ID    = os.getenv("DHAN_CLIENT_ID",   "1111077247")
DHAN_ACCESS_TOKEN = os.getenv("DHAN_ACCESS_TOKEN", "")   # not needed when TOTP env vars are set
DHAN_PIN          = os.getenv("DHAN_PIN",          "")  # for TOTP auto-login
DHAN_TOTP_SECRET  = os.getenv("DHAN_TOTP_SECRET",  "")  # BASE32 secret from Dhan TOTP setup
TOKEN_CACHE_FILE  = "dhan_token_cache.json"

# ─── STRATEGY PARAMETERS ──────────────────────────────────────────────────────
INITIAL_CAPITAL       = 100_000   # ₹1 lakhs
RISK_PCT_PER_TRADE    = 2       # risk 2% of current equity per trade
OPTION_SL_PCT         = 40.0     # kept for lot-sizing math (risk per trade calculation)
TRAIL_PCT             = 0.15     # trailing stop: 15% below peak premium
HALF_EXIT_TRIGGER_PCT = 10.0     # exit half lots when option PREMIUM itself rises >= 10%
# Stop-loss rule (underlying-chart based, replaces premium SL for trade exits):
#   CE trade: exit when any candle after entry closes BELOW the entry candle's OPEN
#   PE trade: exit when any candle after entry closes ABOVE the entry candle's OPEN
BROKERAGE_PER_LOT     = 20.0    # ₹20 per lot (Dhan flat fee)
KILL_SWITCH_PCT       = 10.0     # halt if daily loss exceeds 10% of capital
SLIPPAGE_PCT          = 0.05     # 0.05% slippage per side
EOD_HOUR, EOD_MINUTE  = 15, 15  # square-off time 15:15 IST

# ─── LIVE / PAPER SCAN SETTINGS ───────────────────────────────────────────────
SCAN_INTERVAL_MIN     = 15       # how often to scan (minutes)
DATA_LOOKBACK_DAYS    = 30       # history to fetch for indicator warmup
MARKET_OPEN           = "09:15"
MARKET_CLOSE          = "15:30"

# ─── INDICATOR PARAMETERS ─────────────────────────────────────────────────────
BB_LEN = 20;  BB_STD  = 2.0
EMA5   = 5;   EMA13   = 13;   EMA26   = 26;   EMA50   = 50
VOL_MA = 20
MACD_F = 12;  MACD_S  = 26;   MACD_SIG = 9
RSI_LEN = 14; ADX_LEN = 14

# ─── API ───────────────────────────────────────────────────────────────────────
BASE_URL         = "https://api.dhan.co/v2"
AUTH_URL         = "https://auth.dhan.co/app/generateAccessToken"
MAX_DAYS_PER_REQ = 29   # rollingoption endpoint limit per chunk

# ─── STRIKE OFFSET MAPS ───────────────────────────────────────────────────────
CE_OFFSETS = {"ATM": "ATM", "OTM1": "ATM+1", "OTM2": "ATM+2", "ITM1": "ATM-1"}
PE_OFFSETS = {"ATM": "ATM", "OTM1": "ATM-1", "OTM2": "ATM-2", "ITM1": "ATM+1"}

# =============================================================================
# INSTRUMENT UNIVERSE REGISTRY
# Each entry follows this schema:
#   name           → display name
#   eq_sid         → security ID for underlying price data (string)
#   opt_sid        → security ID for rollingoption API (int)
#   eq_segment     → exchange segment for price data fetch
#   eq_instrument  → instrument type for price data fetch
#   opt_segment    → exchange segment for option data fetch
#   opt_instrument → OPTSTK / OPTIDX / OPTCUR / OPTFUT
#   lot_size       → contract lot size
#   strike_interval→ distance between strikes (for reference only)
#   expiry_flag    → WEEK or MONTH
#   product_type   → INTRADAY (for live orders)
# =============================================================================

STOCK_UNIVERSE: Dict[str, dict] = {
    "HDFCBANK":   {"eq_sid":"1333",  "opt_sid":13,    "eq_segment":"NSE_EQ","eq_instrument":"EQUITY","opt_segment":"NSE_FNO","opt_instrument":"OPTSTK","lot_size":550, "strike_interval":10,  "expiry_flag":"MONTH","product_type":"INTRADAY"},
    "RELIANCE":   {"eq_sid":"2885",  "opt_sid":2885,  "eq_segment":"NSE_EQ","eq_instrument":"EQUITY","opt_segment":"NSE_FNO","opt_instrument":"OPTSTK","lot_size":250, "strike_interval":20,  "expiry_flag":"MONTH","product_type":"INTRADAY"},
    "INFY":       {"eq_sid":"10604", "opt_sid":10604, "eq_segment":"NSE_EQ","eq_instrument":"EQUITY","opt_segment":"NSE_FNO","opt_instrument":"OPTSTK","lot_size":300, "strike_interval":20,  "expiry_flag":"MONTH","product_type":"INTRADAY"},
    "TCS":        {"eq_sid":"11536", "opt_sid":11536, "eq_segment":"NSE_EQ","eq_instrument":"EQUITY","opt_segment":"NSE_FNO","opt_instrument":"OPTSTK","lot_size":150, "strike_interval":50,  "expiry_flag":"MONTH","product_type":"INTRADAY"},
    "ICICIBANK":  {"eq_sid":"4963",  "opt_sid":4963,  "eq_segment":"NSE_EQ","eq_instrument":"EQUITY","opt_segment":"NSE_FNO","opt_instrument":"OPTSTK","lot_size":700, "strike_interval":5,   "expiry_flag":"MONTH","product_type":"INTRADAY"},
    "SBIN":       {"eq_sid":"3045",  "opt_sid":3045,  "eq_segment":"NSE_EQ","eq_instrument":"EQUITY","opt_segment":"NSE_FNO","opt_instrument":"OPTSTK","lot_size":1500,"strike_interval":5,   "expiry_flag":"MONTH","product_type":"INTRADAY"},
    "BAJFINANCE": {"eq_sid":"317",   "opt_sid":317,   "eq_segment":"NSE_EQ","eq_instrument":"EQUITY","opt_segment":"NSE_FNO","opt_instrument":"OPTSTK","lot_size":125, "strike_interval":50,  "expiry_flag":"MONTH","product_type":"INTRADAY"},
    "AXISBANK":   {"eq_sid":"5900",  "opt_sid":5900,  "eq_segment":"NSE_EQ","eq_instrument":"EQUITY","opt_segment":"NSE_FNO","opt_instrument":"OPTSTK","lot_size":625, "strike_interval":5,   "expiry_flag":"MONTH","product_type":"INTRADAY"},
    "MARUTI":     {"eq_sid":"10999", "opt_sid":10999, "eq_segment":"NSE_EQ","eq_instrument":"EQUITY","opt_segment":"NSE_FNO","opt_instrument":"OPTSTK","lot_size":100, "strike_interval":100, "expiry_flag":"MONTH","product_type":"INTRADAY"},
    "TITAN":      {"eq_sid":"3506",  "opt_sid":3506,  "eq_segment":"NSE_EQ","eq_instrument":"EQUITY","opt_segment":"NSE_FNO","opt_instrument":"OPTSTK","lot_size":375, "strike_interval":10,  "expiry_flag":"MONTH","product_type":"INTRADAY"},
    "TATAMOTORS": {"eq_sid":"3456",  "opt_sid":3456,  "eq_segment":"NSE_EQ","eq_instrument":"EQUITY","opt_segment":"NSE_FNO","opt_instrument":"OPTSTK","lot_size":1425,"strike_interval":5,   "expiry_flag":"MONTH","product_type":"INTRADAY"},
    "WIPRO":      {"eq_sid":"3787",  "opt_sid":3787,  "eq_segment":"NSE_EQ","eq_instrument":"EQUITY","opt_segment":"NSE_FNO","opt_instrument":"OPTSTK","lot_size":1500,"strike_interval":5,   "expiry_flag":"MONTH","product_type":"INTRADAY"},
    "HCLTECH":    {"eq_sid":"10666", "opt_sid":10666, "eq_segment":"NSE_EQ","eq_instrument":"EQUITY","opt_segment":"NSE_FNO","opt_instrument":"OPTSTK","lot_size":700, "strike_interval":10,  "expiry_flag":"MONTH","product_type":"INTRADAY"},
    "DRREDDY":    {"eq_sid":"881",   "opt_sid":881,   "eq_segment":"NSE_EQ","eq_instrument":"EQUITY","opt_segment":"NSE_FNO","opt_instrument":"OPTSTK","lot_size":125, "strike_interval":50,  "expiry_flag":"MONTH","product_type":"INTRADAY"},
    "CIPLA":      {"eq_sid":"694",   "opt_sid":694,   "eq_segment":"NSE_EQ","eq_instrument":"EQUITY","opt_segment":"NSE_FNO","opt_instrument":"OPTSTK","lot_size":650, "strike_interval":10,  "expiry_flag":"MONTH","product_type":"INTRADAY"},
}

INDEX_UNIVERSE: Dict[str, dict] = {
    "NIFTY":     {"eq_sid":"13", "opt_sid":13, "eq_segment":"IDX_I", "eq_instrument":"INDEX","opt_segment":"NSE_FNO","opt_instrument":"OPTIDX","lot_size":75,  "strike_interval":50,  "expiry_flag":"WEEK", "product_type":"INTRADAY"},
    "BANKNIFTY": {"eq_sid":"25", "opt_sid":25, "eq_segment":"IDX_I", "eq_instrument":"INDEX","opt_segment":"NSE_FNO","opt_instrument":"OPTIDX","lot_size":30,  "strike_interval":100, "expiry_flag":"WEEK", "product_type":"INTRADAY"},
    "SENSEX":    {"eq_sid":"1",  "opt_sid":1,  "eq_segment":"IDX_I", "eq_instrument":"INDEX","opt_segment":"NSE_FNO","opt_instrument":"OPTIDX","lot_size":10,  "strike_interval":200, "expiry_flag":"WEEK", "product_type":"INTRADAY"},
    "MIDCPNIFTY":{"eq_sid":"27", "opt_sid":27, "eq_segment":"IDX_I", "eq_instrument":"INDEX","opt_segment":"NSE_FNO","opt_instrument":"OPTIDX","lot_size":75,  "strike_interval":25,  "expiry_flag":"WEEK", "product_type":"INTRADAY"},
    "FINNIFTY":  {"eq_sid":"26", "opt_sid":26, "eq_segment":"IDX_I", "eq_instrument":"INDEX","opt_segment":"NSE_FNO","opt_instrument":"OPTIDX","lot_size":40,  "strike_interval":50,  "expiry_flag":"WEEK", "product_type":"INTRADAY"},
}

# NSE Currency options (OPTCUR) — traded on NSE_CURRENCY segment
# Price data uses IDX_I + INDEX (rolling security IDs 10093-10096 work as index values)
# Option data uses NSE_CURRENCY + OPTCUR
CURRENCY_UNIVERSE: Dict[str, dict] = {
    "USDINR":  {"eq_sid":"10093","opt_sid":10093,"eq_segment":"IDX_I","eq_instrument":"INDEX","opt_segment":"NSE_CURRENCY","opt_instrument":"OPTCUR","lot_size":1000,"strike_interval":0.25,"expiry_flag":"MONTH","product_type":"INTRADAY"},
    "EURINR":  {"eq_sid":"10094","opt_sid":10094,"eq_segment":"IDX_I","eq_instrument":"INDEX","opt_segment":"NSE_CURRENCY","opt_instrument":"OPTCUR","lot_size":1000,"strike_interval":0.25,"expiry_flag":"MONTH","product_type":"INTRADAY"},
    "GBPINR":  {"eq_sid":"10095","opt_sid":10095,"eq_segment":"IDX_I","eq_instrument":"INDEX","opt_segment":"NSE_CURRENCY","opt_instrument":"OPTCUR","lot_size":1000,"strike_interval":0.25,"expiry_flag":"MONTH","product_type":"INTRADAY"},
    "JPYINR":  {"eq_sid":"10096","opt_sid":10096,"eq_segment":"IDX_I","eq_instrument":"INDEX","opt_segment":"NSE_CURRENCY","opt_instrument":"OPTCUR","lot_size":1000,"strike_interval":0.25,"expiry_flag":"MONTH","product_type":"INTRADAY"},
}

# MCX Commodity options (OPTFUT) — traded on MCX_COMM segment
# Price data uses IDX_I + INDEX (rolling security IDs work as index values)
# Option data uses MCX_COMM + OPTFUT
COMMODITY_UNIVERSE: Dict[str, dict] = {
    "GOLD":       {"eq_sid":"10080","opt_sid":10080,"eq_segment":"IDX_I","eq_instrument":"INDEX","opt_segment":"MCX_COMM","opt_instrument":"OPTFUT","lot_size":100, "strike_interval":100, "expiry_flag":"MONTH","product_type":"INTRADAY"},
    "SILVER":     {"eq_sid":"10081","opt_sid":10081,"eq_segment":"IDX_I","eq_instrument":"INDEX","opt_segment":"MCX_COMM","opt_instrument":"OPTFUT","lot_size":30,  "strike_interval":500, "expiry_flag":"MONTH","product_type":"INTRADAY"},
    "CRUDEOIL":   {"eq_sid":"10082","opt_sid":10082,"eq_segment":"IDX_I","eq_instrument":"INDEX","opt_segment":"MCX_COMM","opt_instrument":"OPTFUT","lot_size":100, "strike_interval":50,  "expiry_flag":"MONTH","product_type":"INTRADAY"},
    "NATURALGAS": {"eq_sid":"10083","opt_sid":10083,"eq_segment":"IDX_I","eq_instrument":"INDEX","opt_segment":"MCX_COMM","opt_instrument":"OPTFUT","lot_size":1250,"strike_interval":10,  "expiry_flag":"MONTH","product_type":"INTRADAY"},
    "COPPER":     {"eq_sid":"10084","opt_sid":10084,"eq_segment":"IDX_I","eq_instrument":"INDEX","opt_segment":"MCX_COMM","opt_instrument":"OPTFUT","lot_size":2500,"strike_interval":5,   "expiry_flag":"MONTH","product_type":"INTRADAY"},
}

# Combined lookup for easy resolution by name
ALL_UNIVERSES: Dict[str, Dict[str, dict]] = {
    "stocks":    STOCK_UNIVERSE,
    "index":     INDEX_UNIVERSE,
    "currency":  CURRENCY_UNIVERSE,
    "commodity": COMMODITY_UNIVERSE,
}

# =============================================================================
# LOGGING
# =============================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(f"trading_{date.today()}.log", encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)


# =============================================================================
# SECTION 1 — AUTHENTICATION & TOKEN MANAGEMENT
# =============================================================================

def _load_token_cache() -> Optional[dict]:
    """Load token from local cache file if it exists."""
    try:
        if os.path.exists(TOKEN_CACHE_FILE):
            with open(TOKEN_CACHE_FILE) as f:
                return json.load(f)
    except Exception:
        pass
    return None


def _save_token_cache(token: str, expiry: str) -> None:
    """Save freshly generated token to local cache."""
    try:
        with open(TOKEN_CACHE_FILE, "w") as f:
            json.dump({"access_token": token, "expiry": expiry,
                       "fetched_at": datetime.now().isoformat()}, f)
    except Exception as e:
        log.warning(f"Could not save token cache: {e}")


def auto_login_totp(client_id: str, pin: str, totp_secret: str) -> str:
    """
    Automatically generate a fresh Dhan access token using PIN + TOTP.
    Requires pyotp: pip install pyotp
    Requires TOTP to be enabled on your Dhan account (web.dhan.co → API → Setup TOTP).
    TOTP secret is the BASE32 key shown during TOTP setup.
    """
    try:
        import pyotp
    except ImportError:
        raise ImportError("Run: pip install pyotp  — needed for TOTP auto-login")

    # Check cache first — reuse if token fetched less than 20 hours ago
    # NOTE: must use .total_seconds(), not .seconds (.seconds caps at 86399)
    cache = _load_token_cache()
    if cache:
        age_hours = (datetime.now() - datetime.fromisoformat(cache["fetched_at"])).total_seconds() / 3600
        if age_hours < 20:
            log.info(f"Using cached token (age: {age_hours:.1f}h, expires: {cache['expiry']})")
            return cache["access_token"]

    totp_code = pyotp.TOTP(totp_secret).now()

    # Correct Dhan endpoint: GET https://auth.dhan.co/app/generateAccessToken
    # with dhanClientId, pin, totp as query parameters
    r = requests.post(
        AUTH_URL,
        params={"dhanClientId": client_id, "pin": pin, "totp": totp_code},
        timeout=15,
    )
    if not r.ok:
        raise ValueError(
            f"Token generation failed ({r.status_code}): {r.text[:300]}\n"
            "Check DHAN_PIN and DHAN_TOTP_SECRET are correct, "
            "and that TOTP is enabled on your Dhan account (web.dhan.co → Profile → API → Setup TOTP)."
        )
    data = r.json()

    token = data.get("accessToken")
    expiry = data.get("expiryTime", "")
    if not token:
        raise ValueError(f"accessToken not found in response: {data}")

    _save_token_cache(token, expiry)
    log.info(f"New Dhan token generated. Expires: {expiry}")
    return token


def renew_token(access_token: str, client_id: str) -> str:
    """
    Renew an existing ACTIVE token for another 24 hours.
    Only works on tokens that have NOT yet expired.
    """
    r = requests.get(
        f"{BASE_URL}/RenewToken",
        headers={"access-token": access_token, "dhanClientId": client_id},
        timeout=15,
    )
    r.raise_for_status()
    data = r.json()
    new_token = data.get("accessToken", access_token)
    log.info("Token renewed successfully.")
    return new_token


def get_access_token() -> str:
    """
    Entry point for obtaining a valid access token.
    Priority:
      1. TOTP auto-login  (if DHAN_PIN and DHAN_TOTP_SECRET are set)
      2. DHAN_ACCESS_TOKEN env var / hardcoded (manual mode)
    """
    if DHAN_PIN and DHAN_TOTP_SECRET:
        return auto_login_totp(DHAN_CLIENT_ID, DHAN_PIN, DHAN_TOTP_SECRET)
    elif DHAN_ACCESS_TOKEN:
        return DHAN_ACCESS_TOKEN
    else:
        raise ValueError(
            "No credentials found. Set either:\n"
            "  • DHAN_PIN + DHAN_TOTP_SECRET  (for auto-login)\n"
            "  • DHAN_ACCESS_TOKEN            (for manual token)"
        )


# =============================================================================
# SECTION 2 — HTTP SESSION
# =============================================================================

def make_session(token: str) -> requests.Session:
    """Create an authenticated HTTP session for all Dhan API calls."""
    s = requests.Session()
    s.headers.update({
        "Content-Type": "application/json",
        "Accept":       "application/json",
        "access-token": token,
        "client-id":    DHAN_CLIENT_ID,
    })
    return s


# Global session — initialised after token is resolved in main()
SESSION: Optional[requests.Session] = None


# =============================================================================
# SECTION 3 — DATA FETCHING
# =============================================================================

def _parse_columnar(data: dict) -> pd.DataFrame:
    """Convert Dhan columnar OHLCV response to a DatetimeIndex DataFrame."""
    if not data or "open" not in data:
        return pd.DataFrame()
    cols = {k: data[k] for k in ("open", "high", "low", "close", "volume") if k in data}
    df = pd.DataFrame(
        cols,
        index=pd.to_datetime(data["timestamp"], unit="s", utc=True).tz_convert("Asia/Kolkata"),
    )
    df.index.name = "datetime"
    for c in df.columns:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    return df.sort_index()


def nearest_trading_day(d: date) -> date:
    """
    Return d itself if it is a weekday (Mon–Fri).
    Walk backwards until we hit a weekday.
    This prevents 400 errors from Dhan when fromDate lands on a weekend.
    Note: does not account for exchange holidays — weekday check is sufficient
    for the Dhan intraday API (it returns empty data for holidays, not 400).
    """
    while d.weekday() >= 5:   # 5=Sat, 6=Sun
        d -= timedelta(days=1)
    return d


def safe_from_date(lookback_days: int = DATA_LOOKBACK_DAYS) -> str:
    """
    Calculate a safe from_date for live/paper data fetching.
    Adds a 10-day buffer on top of lookback_days to ensure enough
    trading days are returned even across weekends and holidays.
    Always snaps to a weekday.
    """
    today  = date.today()
    # Add 14-day buffer: 30 trading days ≈ 42 calendar days
    buffer = max(lookback_days, int(lookback_days * 1.5))
    raw    = today - timedelta(days=buffer)
    return nearest_trading_day(raw).strftime("%Y-%m-%d")


def fetch_price_15m(security_id: str, from_date: str, to_date: str,
                    segment: str = "NSE_EQ",
                    instrument: str = "EQUITY") -> pd.DataFrame:
    """
    Fetch 15-min OHLCV for any underlying: equity, index, currency future, commodity future.
    Auto-chunked in 89-day windows to respect API limits.
    """
    fmt = "%Y-%m-%d"
    start, end = datetime.strptime(from_date, fmt), datetime.strptime(to_date, fmt)
    frames, cursor = [], start
    while cursor <= end:
        chunk_end = min(cursor + timedelta(days=89), end)
        payload = {
            "securityId":      security_id,
            "exchangeSegment": segment,
            "instrument":      instrument,
            "interval":        "15",
            "oi":              False,
            "fromDate":        cursor.strftime(fmt),
            "toDate":          chunk_end.strftime(fmt),
        }
        try:
            r = SESSION.post(f"{BASE_URL}/charts/intraday", json=payload, timeout=30)
            if not r.ok:
                # Log the full error body so misconfig (wrong segment/instrument) is visible
                log.warning(
                    f"Price fetch error [{security_id}] {cursor.strftime(fmt)}: "
                    f"HTTP {r.status_code} | body={r.text[:300]} | "
                    f"segment={segment} instrument={instrument}"
                )
                cursor = chunk_end + timedelta(days=1)
                time.sleep(0.35)
                continue
            df = _parse_columnar(r.json())
            if not df.empty:
                frames.append(df)
        except Exception as e:
            log.warning(f"Price fetch error [{security_id}] {cursor.strftime(fmt)}: {e}")
        cursor = chunk_end + timedelta(days=1)
        time.sleep(0.35)
    if not frames:
        return pd.DataFrame()
    out = pd.concat(frames).sort_index()
    return out[~out.index.duplicated(keep="first")]


def resample_to_60m(df: pd.DataFrame) -> pd.DataFrame:
    """Resample 15-min data to 1H for tide (higher timeframe) signals."""
    if df.empty:
        return df
    out = df.resample("60min").agg(
        {"open": "first", "high": "max", "low": "min",
         "close": "last", "volume": "sum"}
    ).dropna(subset=["close"])
    # fill zero volume so indicators don't break for indices
    if out["volume"].sum() == 0:
        out["volume"] = 1_000_000
    return out


def fetch_option_candles(security_id: int, option_type: str, strike_offset: str,
                         from_date: str, to_date: str,
                         opt_segment: str = "NSE_FNO",
                         opt_instrument: str = "OPTSTK",
                         expiry_flag: str = "MONTH",
                         interval: str = "15") -> pd.DataFrame:
    """
    Fetch rolling option OHLCV via Dhan rollingoption endpoint.
    Works for OPTSTK (stocks), OPTIDX (indices), OPTCUR (currency), OPTFUT (commodities).
    Auto-chunked in 29-day windows.
    """
    fmt = "%Y-%m-%d"
    start, end = datetime.strptime(from_date, fmt), datetime.strptime(to_date, fmt)
    side = "ce" if option_type == "CALL" else "pe"
    frames, cursor = [], start
    while cursor <= end:
        chunk_end = min(cursor + timedelta(days=MAX_DAYS_PER_REQ), end)
        payload = {
            "exchangeSegment": opt_segment,
            "interval":        interval,
            "securityId":      security_id,
            "instrument":      opt_instrument,
            "expiryFlag":      expiry_flag,
            "expiryCode":      1,
            "strike":          strike_offset,
            "drvOptionType":   option_type,
            "requiredData":    ["open", "high", "low", "close",
                                "volume", "oi", "iv", "spot", "strike"],
            "fromDate":        cursor.strftime(fmt),
            "toDate":          chunk_end.strftime(fmt),
        }
        try:
            r = SESSION.post(f"{BASE_URL}/charts/rollingoption", json=payload, timeout=30)
            r.raise_for_status()
            raw = r.json()
            inner = raw.get("data", {})
            side_data = inner.get(side) or inner.get(side.upper()) or {}
            timestamps = side_data.get("timestamp", [])
            if timestamps:
                cols = {}
                for field_name in ["open", "high", "low", "close",
                                   "volume", "oi", "iv", "spot", "strike"]:
                    v = side_data.get(field_name, [])
                    if v and len(v) == len(timestamps):
                        cols[field_name] = v
                if cols.get("close"):
                    df = pd.DataFrame(
                        cols,
                        index=pd.to_datetime(timestamps, unit="s", utc=True)
                              .tz_convert("Asia/Kolkata"),
                    )
                    df.index.name = "datetime"
                    for c in df.columns:
                        df[c] = pd.to_numeric(df[c], errors="coerce")
                    frames.append(df.dropna(subset=["close"]))
        except Exception as e:
            log.warning(f"Option fetch error [{security_id}] {option_type} "
                        f"{cursor.strftime(fmt)}: {e}")
        cursor = chunk_end + timedelta(days=1)
        time.sleep(0.4)
    if not frames:
        return pd.DataFrame()
    out = pd.concat(frames).sort_index()
    return out[~out.index.duplicated(keep="first")]


def fetch_live_option_ltp(security_id: int, option_type: str,
                          strike_offset: str,
                          opt_segment: str, opt_instrument: str,
                          expiry_flag: str) -> Optional[float]:
    """
    Fetch the latest option premium (LTP) for live / paper trading.
    Uses rollingoption with today's date range.
    """
    today = date.today().strftime("%Y-%m-%d")
    df = fetch_option_candles(
        security_id, option_type, strike_offset,
        from_date=today, to_date=today,
        opt_segment=opt_segment, opt_instrument=opt_instrument,
        expiry_flag=expiry_flag,
    )
    if df.empty:
        return None
    return float(df["close"].iloc[-1])


# =============================================================================
# SECTION 4 — TECHNICAL INDICATORS
# =============================================================================

def _ema(s: pd.Series, n: int) -> pd.Series:
    return s.ewm(span=n, adjust=False).mean()

def _sma(s: pd.Series, n: int) -> pd.Series:
    return s.rolling(n).mean()

def _rma(s: pd.Series, n: int) -> pd.Series:
    return s.ewm(alpha=1/n, adjust=False).mean()


def compute_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute all indicators on any OHLCV DataFrame.
    Returns empty DataFrame if too few rows to warm up all indicators.
    Minimum rows required: 55 (EMA50 needs 50 + buffer).
    """
    if df.empty or len(df) < 55:
        log.warning(f"compute_indicators: only {len(df)} rows — need ≥55. "
                    "Returning empty DataFrame.")
        return pd.DataFrame()

    df = df.copy()
    c, h, l, v = df["close"], df["high"], df["low"], df["volume"]

    # Bollinger Bands
    basis = _sma(c, BB_LEN)
    sigma = c.rolling(BB_LEN).std(ddof=0)
    df["bb_upper"] = basis + BB_STD * sigma
    df["bb_lower"] = basis - BB_STD * sigma

    # EMAs
    df["ema5"]  = _ema(c, EMA5)
    df["ema13"] = _ema(c, EMA13)
    df["ema26"] = _ema(c, EMA26)
    df["ema50"] = _ema(c, EMA50)

    # Volume MA
    df["vol_ma"] = _sma(v, VOL_MA)

    # MACD
    fast = _ema(c, MACD_F); slow = _ema(c, MACD_S)
    df["macd"]        = fast - slow
    df["macd_signal"] = _ema(df["macd"], MACD_SIG)
    df["macd_hist"]   = df["macd"] - df["macd_signal"]

    # RSI
    delta = c.diff()
    gain  = delta.clip(lower=0); loss = (-delta).clip(lower=0)
    df["rsi"] = 100 - 100 / (1 + _rma(gain, RSI_LEN) / (_rma(loss, RSI_LEN) + 1e-9))

    # ADX / DMI
    tr   = pd.concat([h - l, (h - c.shift()).abs(), (l - c.shift()).abs()], axis=1).max(axis=1)
    atr  = _rma(tr, ADX_LEN)
    up   = h - h.shift(1); dn = l.shift(1) - l
    pdm  = pd.Series(np.where((up > dn) & (up > 0), up, 0.0), index=h.index)
    ndm  = pd.Series(np.where((dn > up) & (dn > 0), dn, 0.0), index=h.index)
    df["plus_di"]  = 100 * _rma(pdm, ADX_LEN) / (atr + 1e-9)
    df["minus_di"] = 100 * _rma(ndm, ADX_LEN) / (atr + 1e-9)
    dx   = 100 * (df["plus_di"] - df["minus_di"]).abs() / (df["plus_di"] + df["minus_di"] + 1e-9)
    df["adx"] = _rma(dx, ADX_LEN)

    # Consecutive higher-lows / lower-highs (structure)
    def _hl_count(w):
        cnt = 0
        for i in range(1, len(w)):
            cnt = cnt + 1 if w[i] > w[i-1] else 0
        return cnt

    def _lh_count(w):
        cnt = 0
        for i in range(1, len(w)):
            cnt = cnt + 1 if w[i] < w[i-1] else 0
        return cnt

    df["higher_lows"] = l.rolling(10).apply(_hl_count, raw=True)
    df["lower_highs"] = h.rolling(10).apply(_lh_count, raw=True)

    # EMA crossover events
    df["ema5_bull_13"] = (df["ema5"] >  df["ema13"]) & (df["ema5"].shift(1) <= df["ema13"].shift(1))
    df["ema5_bull_26"] = (df["ema5"] >  df["ema26"]) & (df["ema5"].shift(1) <= df["ema26"].shift(1))
    df["ema5_bear_13"] = (df["ema5"] <  df["ema13"]) & (df["ema5"].shift(1) >= df["ema13"].shift(1))
    df["ema5_bear_26"] = (df["ema5"] <  df["ema26"]) & (df["ema5"].shift(1) >= df["ema26"].shift(1))

    # RSI threshold crosses
    df["rsi_cross60"] = (df["rsi"] > 60) & (df["rsi"].shift(1) <= 60)
    df["rsi_cross40"] = (df["rsi"] < 40) & (df["rsi"].shift(1) >= 40)

    # DI crossover events
    df["di_bull"] = (df["plus_di"] >  df["minus_di"]) & (df["plus_di"].shift(1) <= df["minus_di"].shift(1))
    df["di_bear"] = (df["minus_di"] > df["plus_di"])  & (df["minus_di"].shift(1) <= df["plus_di"].shift(1))

    return df


def _rany(s: pd.Series, n: int) -> pd.Series:
    """True if s was True in any of the last n bars."""
    return s.rolling(n, min_periods=1).max().astype(bool)


# =============================================================================
# SECTION 5 — SIGNAL GENERATION
# Dual-timeframe strategy:
#   TIDE (1H) = higher-timeframe trend filter
#   WAVE (15M) = entry-timing conditions
# Both must agree before a signal fires.
# =============================================================================

_REQUIRED_COLS = {
    "bb_upper", "bb_lower", "macd", "rsi", "adx",
    "ema5", "ema50", "vol_ma", "higher_lows", "lower_highs",
    "plus_di", "minus_di", "di_bull", "di_bear",
    "rsi_cross60", "rsi_cross40",
    "ema5_bull_13", "ema5_bull_26", "ema5_bear_13", "ema5_bear_26",
}


def generate_signals(wave: pd.DataFrame, tide: pd.DataFrame) -> pd.DataFrame:
    """
    Apply all dual-timeframe conditions and attach a 'signal' column to wave.
    signal = +1  → BUY CE (bullish)
    signal = -1  → BUY PE (bearish)
    signal =  0  → no trade

    Returns wave with signal column, or wave with all-zero signal if inputs are
    invalid (called gracefully instead of raising KeyError).
    """
    # Safety: return zeroed signals if indicator columns are missing
    for name, df in [("wave", wave), ("tide", tide)]:
        if df.empty or not _REQUIRED_COLS.issubset(df.columns):
            missing = _REQUIRED_COLS - set(df.columns) if not df.empty else {"(all)"}
            log.warning(f"generate_signals: {name} missing {missing} — no signals.")
            wave = wave.copy() if not wave.empty else pd.DataFrame()
            if not wave.empty:
                wave["signal"] = 0
            return wave

    # ── Tide conditions (1H) ────────────────────────────────────────────────
    tide_buy = (
        ((tide["close"] >= tide["bb_upper"] * 0.65) |
         (tide["bb_upper"] >= tide["bb_upper"].shift(1))) &
        (tide["macd"] > 0) & (tide["macd"] >= tide["macd"].shift(1))
    )
    tide_sell = (
        ((tide["close"] <= tide["bb_lower"] * 1.35) |
         (tide["bb_lower"] <= tide["bb_lower"].shift(1))) &
        (tide["macd"] < 0) & (tide["macd"] <= tide["macd"].shift(1))
    )
    tide_rsi_buy  = tide["rsi"] > 50
    tide_rsi_sell = tide["rsi"] < 50

    def _ff(s: pd.Series) -> pd.Series:
        """Forward-fill tide signals onto the wave (15M) index."""
        return s.reindex(wave.index, method="ffill").fillna(False)

    wave = wave.copy()
    wave["_tide_buy"]      = _ff(tide_buy)
    wave["_tide_sell"]     = _ff(tide_sell)
    wave["_tide_rsi_buy"]  = _ff(tide_rsi_buy)
    wave["_tide_rsi_sell"] = _ff(tide_rsi_sell)

    # ── Wave conditions (15M) ───────────────────────────────────────────────
    rsi60   = _rany(wave["rsi_cross60"],    3)
    rsi40   = _rany(wave["rsi_cross40"],    3)
    ema_b   = _rany(wave["ema5_bull_13"],   3) | _rany(wave["ema5_bull_26"], 3)
    ema_s   = _rany(wave["ema5_bear_13"],   3) | _rany(wave["ema5_bear_26"], 3)
    di_b    = _rany(wave["di_bull"],        3)
    di_s    = _rany(wave["di_bear"],        3)
    adx_ok  = (wave["adx"] > 15) & (wave["adx"] > wave["adx"].shift(1))

    # C3: RSI alignment between tide and wave
    c3_buy  = wave["_tide_rsi_buy"]  & rsi60 & (wave["rsi"] > 60)
    c3_sell = wave["_tide_rsi_sell"] & rsi40 & (wave["rsi"] < 40)

    # Full buy / sell condition sets
    wave_buy = (
        (wave["close"] >= wave["bb_upper"] * 0.85) &
        (wave["volume"] >= wave["vol_ma"]) &
        (wave["higher_lows"] >= 2) &
        ema_b & di_b & adx_ok &
        (wave["close"] > wave["ema50"])
    )
    wave_sell = (
        (wave["close"] <= wave["bb_lower"] * 1.25) &
        (wave["volume"] >= wave["vol_ma"]) &
        (wave["lower_highs"] >= 2) &
        ema_s & di_s & adx_ok &
        (wave["close"] < wave["ema50"])
    )

    wave["signal"] = 0
    wave.loc[wave["_tide_buy"]  & c3_buy  & wave_buy,  "signal"] = 1
    wave.loc[wave["_tide_sell"] & c3_sell & wave_sell, "signal"] = -1
    return wave


# =============================================================================
# SECTION 6 — POSITION SIZING & HELPERS
# =============================================================================

def calc_lots(capital: float, premium: float, lot_size: int,
              risk_pct: float = RISK_PCT_PER_TRADE,
              sl_pct: float   = OPTION_SL_PCT) -> tuple:
    """
    Risk-based lot sizing.
    Returns (total_lots, half_lots, trail_lots).
    risk_amount   = capital × risk_pct / 100
    loss_per_lot  = premium × sl_pct / 100 × lot_size
    """
    risk_amt       = capital * risk_pct / 100
    loss_per_share = premium * sl_pct / 100
    loss_per_lot   = loss_per_share * lot_size
    if loss_per_lot <= 0:
        return 0, 0, 0
    lots  = max(1, math.floor(risk_amt / loss_per_lot))
    half  = max(1, lots // 2)
    trail = lots - half
    if trail < 1:
        trail = 1; half = max(0, lots - 1)
    return lots, half, trail


def apply_slippage(price: float, side: str) -> float:
    """Buys pay more; sells receive less."""
    mult = (1 + SLIPPAGE_PCT / 100) if side == "buy" else (1 - SLIPPAGE_PCT / 100)
    return round(price * mult, 2)


def get_option_price(opt_df: pd.DataFrame, ts: pd.Timestamp) -> Optional[float]:
    """Get closest available option close price at or before timestamp ts."""
    if opt_df.empty:
        return None
    avail = opt_df[opt_df.index <= ts]
    return float(avail["close"].iloc[-1]) if not avail.empty else None


def get_option_strike(opt_df: pd.DataFrame, ts: pd.Timestamp) -> float:
    """Get the actual strike price of the option contract at entry time."""
    if opt_df.empty or "strike" not in opt_df.columns:
        return 0.0
    avail = opt_df[opt_df.index <= ts]
    if avail.empty:
        return 0.0
    val = avail["strike"].iloc[-1]
    return float(val) if pd.notna(val) else 0.0


# =============================================================================
# SECTION 7 — DATA CLASSES
# =============================================================================

@dataclass
class Trade:
    symbol:           str
    universe:         str           # stocks / index / currency / commodity
    option_type:      str           # CE or PE
    signal:           int
    strike_mode:      str
    entry_time:       pd.Timestamp
    entry_premium:    float
    lot_size:         int
    total_lots:       int
    half_lots:        int
    trail_lots:       int
    sl_premium:       float
    trail_premium:    float
    peak_premium:     float
    underlying_entry: float
    strike_price:     float = 0.0

    exit_time:        Optional[pd.Timestamp] = None
    exit_premium:     float = 0.0
    exit_reason:      str   = ""
    half_done:        bool  = False
    half_pnl:         float = 0.0
    trail_pnl:        float = 0.0
    net_pnl:          float = 0.0
    bars_held:        int   = 0

    # ── Entry candle OHLC (underlying bar that triggered the signal) ──────────
    entry_candle_open:  float = 0.0   # also used as the underlying stop-loss level
    entry_candle_high:  float = 0.0
    entry_candle_low:   float = 0.0
    entry_candle_close: float = 0.0   # == underlying_entry

    # ── Exit candle OHLC (underlying bar on which the trade was closed) ───────
    exit_candle_open:   float = 0.0
    exit_candle_high:   float = 0.0
    exit_candle_low:    float = 0.0
    exit_candle_close:  float = 0.0

    # ── Expiry metadata ──────────────────────────────────────────────────────
    # expiry_type : "WEEKLY" or "MONTHLY" — taken directly from instrument config
    # expiry_date : the actual calendar expiry date of the front contract that
    #               was traded. Computed from entry_time:
    #                 WEEKLY  → nearest Thursday on or after entry date
    #                 MONTHLY → last Thursday of entry month
    expiry_type:      str   = ""
    expiry_date:      str   = ""   # "YYYY-MM-DD"


@dataclass
class LivePosition:
    """Represents an open position in paper or live trading."""
    symbol:           str
    universe:         str
    option_type:      str           # CE or PE
    signal:           int
    strike_mode:      str
    entry_time:       datetime
    entry_premium:    float
    lot_size:         int
    total_lots:       int
    half_lots:        int
    trail_lots:       int
    sl_premium:       float
    trail_premium:    float
    peak_premium:     float
    underlying_entry: float
    strike_price:     float
    opt_sid:          int
    opt_segment:      str
    opt_instrument:   str
    expiry_flag:      str
    # Entry candle OHLC — entry_candle_open is the underlying SL level
    # CE: exit if underlying close < entry_candle_open
    # PE: exit if underlying close > entry_candle_open
    entry_candle_open:  float = 0.0
    entry_candle_high:  float = 0.0
    entry_candle_low:   float = 0.0
    entry_candle_close: float = 0.0

    # For live trading — order tracking
    entry_order_id:   str = ""
    exit_order_id:    str = ""
    half_done:        bool  = False
    half_pnl:         float = 0.0


# =============================================================================
# SECTION 8 — KILL SWITCH
# =============================================================================

class KillSwitch:
    """Halts all trading if daily loss exceeds the configured threshold."""

    def __init__(self, capital: float):
        self.threshold  = capital * KILL_SWITCH_PCT / 100
        self.daily_loss = 0.0
        self.triggered  = False

    def record(self, pnl: float) -> None:
        if pnl < 0:
            self.daily_loss += abs(pnl)
            if self.daily_loss >= self.threshold and not self.triggered:
                self.triggered = True
                log.critical(
                    f"\n{'='*55}\n"
                    f"  KILL SWITCH TRIGGERED\n"
                    f"  Daily loss ₹{self.daily_loss:,.0f} exceeded "
                    f"{KILL_SWITCH_PCT}% threshold (₹{self.threshold:,.0f})\n"
                    f"  ALL TRADING HALTED FOR THE DAY\n"
                    f"{'='*55}"
                )

    def is_on(self) -> bool:
        return self.triggered

    def reset(self) -> None:
        self.daily_loss = 0.0
        self.triggered  = False


# =============================================================================
# SECTION 9 — SHARED TRADE ENGINE (used by backtest AND paper/live)
# =============================================================================

def _close_trade(trade: Trade, ts: pd.Timestamp, exit_p: float,
                 reason: str, lot_sz: int,
                 trades_list: list, kill: KillSwitch,
                 exit_bar: Optional[pd.Series] = None) -> None:
    """
    Finalise a trade, compute PnL, update kill switch.
    PnL:
      • If half-exit done: only trail_lots remain for the final leg.
      • gross = half_pnl + (exit - entry) × remaining_lots × lot_size
      • net   = gross - brokerage (₹20/lot × total_lots × 2 sides)

    exit_bar: the underlying price bar (pd.Series) at exit time.
              Used to record exit candle OHLC in the report.
    """
    rem_lots  = trade.trail_lots if trade.half_done else trade.total_lots
    rem_qty   = rem_lots * lot_sz
    trail_pnl = (exit_p - trade.entry_premium) * rem_qty
    gross     = trade.half_pnl + trail_pnl
    brok      = BROKERAGE_PER_LOT * trade.total_lots * 2
    net       = gross - brok

    trade.exit_time    = ts
    trade.exit_premium = exit_p
    trade.exit_reason  = reason
    trade.trail_pnl    = trail_pnl
    trade.net_pnl      = net

    # Store exit candle OHLC if the underlying bar was provided
    if exit_bar is not None:
        trade.exit_candle_open  = float(exit_bar.get("open",  0))
        trade.exit_candle_high  = float(exit_bar.get("high",  0))
        trade.exit_candle_low   = float(exit_bar.get("low",   0))
        trade.exit_candle_close = float(exit_bar.get("close", 0))

    kill.record(net)
    trades_list.append(trade)
    log.info(f"  CLOSE {trade.symbol} {trade.option_type} @ ₹{exit_p:.2f} "
             f"| reason={reason} | net_pnl=₹{net:+,.0f}")


def _nearest_thursday(d: date) -> date:
    """Return the nearest Thursday on or after date d (0=Mon … 6=Sun)."""
    days_ahead = (3 - d.weekday()) % 7   # 3 = Thursday
    return d + timedelta(days=days_ahead)


def _last_thursday_of_month(d: date) -> date:
    """Return the last Thursday of the month that contains date d."""
    # Go to the last day of this month, then walk back to Thursday
    if d.month == 12:
        last_day = date(d.year + 1, 1, 1) - timedelta(days=1)
    else:
        last_day = date(d.year, d.month + 1, 1) - timedelta(days=1)
    offset = (last_day.weekday() - 3) % 7   # days to walk back to Thursday
    return last_day - timedelta(days=offset)


def compute_expiry_date(entry_ts: pd.Timestamp, expiry_flag: str) -> str:
    """
    Derive the expiry date of the front option contract from the trade entry date.

    WEEKLY  (WEEK)  — Index options (NIFTY, BANKNIFTY, SENSEX):
        Expiry is every Thursday. Return the nearest Thursday on or after
        the entry date. If entry is itself a Thursday, that day is the expiry.

    MONTHLY (MONTH) — Stock options, currency, commodity:
        Expiry is the last Thursday of the entry month.

    Returns a string "YYYY-MM-DD".
    """
    entry_date = entry_ts.date() if hasattr(entry_ts, "date") else pd.Timestamp(entry_ts).date()
    flag = expiry_flag.upper()

    if flag in ("WEEK", "WEEKLY"):
        exp = _nearest_thursday(entry_date)
    else:   # MONTH, MONTHLY, or anything else
        exp = _last_thursday_of_month(entry_date)

    return exp.strftime("%Y-%m-%d")


def run_one_instrument(name: str, universe_key: str, info: dict,
                       from_date: str, to_date: str,
                       strike_mode: str, capital: float,
                       kill: KillSwitch) -> List[Trade]:
    """
    Full backtest pipeline for a single instrument (stock / index / currency / commodity).
    Returns list of completed Trade objects.

    Steps:
      1. Fetch underlying 15M OHLCV
      2. Resample to 1H for tide
      3. Compute indicators on both timeframes
      4. Generate signals
      5. Fetch CE and PE option premium data
      6. Bar-by-bar simulation
    """
    lot_sz = info["lot_size"]

    # Step 1 & 2 — Price data
    log.info(f"  [{name}] Fetching price data ({info['eq_segment']})...")
    wave_raw = fetch_price_15m(
        info["eq_sid"], from_date, to_date,
        segment=info["eq_segment"], instrument=info["eq_instrument"],
    )
    if wave_raw.empty or len(wave_raw) < 100:
        log.warning(f"  [{name}] Insufficient price data — skipping.")
        return []

    if "volume" not in wave_raw.columns or wave_raw["volume"].sum() == 0:
        wave_raw["volume"] = 1_000_000

    tide_raw = resample_to_60m(wave_raw.copy())

    # Step 3 — Indicators
    wave = compute_indicators(wave_raw.copy())
    tide = compute_indicators(tide_raw.copy())

    if wave.empty or tide.empty:
        log.warning(f"  [{name}] Not enough rows for indicators — skipping.")
        return []

    # Step 4 — Signals
    wave = generate_signals(wave, tide)
    n_sig = (wave["signal"] != 0).sum()
    log.info(f"  [{name}] {len(wave)} bars | {n_sig} signals")
    if n_sig == 0:
        return []

    # Step 5 — Option data
    ce_off = CE_OFFSETS.get(strike_mode, "ATM")
    pe_off = PE_OFFSETS.get(strike_mode, "ATM")

    log.info(f"  [{name}] Fetching CE ({ce_off}) ...")
    ce_df = fetch_option_candles(
        info["opt_sid"], "CALL", ce_off, from_date, to_date,
        opt_segment=info["opt_segment"], opt_instrument=info["opt_instrument"],
        expiry_flag=info["expiry_flag"],
    )
    time.sleep(0.5)
    log.info(f"  [{name}] Fetching PE ({pe_off}) ...")
    pe_df = fetch_option_candles(
        info["opt_sid"], "PUT",  pe_off, from_date, to_date,
        opt_segment=info["opt_segment"], opt_instrument=info["opt_instrument"],
        expiry_flag=info["expiry_flag"],
    )

    if ce_df.empty and pe_df.empty:
        log.warning(f"  [{name}] No option data — skipping.")
        return []

    log.info(f"  [{name}] CE={len(ce_df)} bars | PE={len(pe_df)} bars")

    # Step 6 — Simulation
    trades: List[Trade] = []
    open_trade: Optional[Trade] = None
    equity = capital
    bars   = list(wave.iterrows())

    for i, (ts, bar) in enumerate(bars):

        # Kill switch check
        if kill.is_on():
            if open_trade:
                opt_df = ce_df if open_trade.option_type == "CE" else pe_df
                p = get_option_price(opt_df, ts) or open_trade.entry_premium
                _close_trade(open_trade, ts, apply_slippage(p, "sell"),
                             "kill_switch", lot_sz, trades, kill, exit_bar=bar)
                equity += open_trade.net_pnl
                open_trade = None
            break

        signal   = int(bar.get("signal", 0))
        close_p  = bar["close"]
        ema5_val = bar.get("ema5", close_p)

        # Manage existing open trade
        if open_trade:
            opt_df = ce_df if open_trade.option_type == "CE" else pe_df
            prem   = get_option_price(opt_df, ts)
            open_trade.bars_held += 1

            if prem is not None:

                # Update trailing stop
                if prem > open_trade.peak_premium:
                    open_trade.peak_premium = prem
                    new_trail = prem * (1 - TRAIL_PCT)
                    if new_trail > open_trade.trail_premium:
                        open_trade.trail_premium = round(new_trail, 2)

                # HALF-EXIT: option PREMIUM itself rises >= HALF_EXIT_TRIGGER_PCT (10%)
                # (Previously: underlying moved 0.3% — now premium moves 10%)
                if not open_trade.half_done and open_trade.half_lots > 0:
                    prem_move_pct = (prem - open_trade.entry_premium)                                     / open_trade.entry_premium * 100
                    if prem_move_pct >= HALF_EXIT_TRIGGER_PCT:
                        exit_p = apply_slippage(prem, "sell")
                        open_trade.half_pnl = (exit_p - open_trade.entry_premium)                                               * open_trade.half_lots * lot_sz
                        open_trade.sl_premium    = open_trade.entry_premium
                        open_trade.trail_premium = max(
                            open_trade.trail_premium, open_trade.entry_premium)
                        open_trade.half_done = True
                        log.debug(
                            f"  [{name}] HALF-EXIT @ Rs{exit_p:.2f} "
                            f"(premium +{prem_move_pct:.1f}%) "
                            f"pnl=Rs{open_trade.half_pnl:+.0f}"
                        )

                # EOD square-off
                if ts.hour == EOD_HOUR and ts.minute >= EOD_MINUTE:
                    _close_trade(open_trade, ts, apply_slippage(prem, "sell"),
                                 "eod", lot_sz, trades, kill, exit_bar=bar)
                    equity += open_trade.net_pnl
                    open_trade = None
                    continue

                # UNDERLYING-CHART STOP LOSS
                # CE: exit when underlying close < entry candle open
                # PE: exit when underlying close > entry candle open
                # Checked only after entry bar (bars_held > 0)
                if open_trade.bars_held > 0:
                    sl_hit = False
                    if (open_trade.option_type == "CE"
                            and close_p < open_trade.entry_candle_open):
                        sl_hit = True
                    elif (open_trade.option_type == "PE"
                          and close_p > open_trade.entry_candle_open):
                        sl_hit = True

                    if sl_hit:
                        exit_p = max(0.05, apply_slippage(prem, "sell"))
                        _close_trade(open_trade, ts, exit_p,
                                     "underlying_sl", lot_sz, trades, kill,
                                     exit_bar=bar)
                        equity += open_trade.net_pnl
                        open_trade = None
                        continue

                # EMA-5 exit
                reason = None
                if open_trade.option_type == "CE":
                    if close_p < ema5_val:
                        reason = "below_ema5"
                else:
                    if close_p > ema5_val:
                        reason = "above_ema5"

                if reason:
                    _close_trade(open_trade, ts, apply_slippage(prem, "sell"),
                                 reason, lot_sz, trades, kill, exit_bar=bar)
                    equity += open_trade.net_pnl
                    open_trade = None
                    continue

                # Trailing stop
                if (open_trade.trail_premium > open_trade.entry_premium
                        and prem <= open_trade.trail_premium):
                    _close_trade(open_trade, ts,
                                 apply_slippage(open_trade.trail_premium, "sell"),
                                 "trail_stop", lot_sz, trades, kill,
                                 exit_bar=bar)
                    equity += open_trade.net_pnl
                    open_trade = None
                    continue

        # New entry
        if signal != 0 and open_trade is None:
            opt_type = "CE" if signal == 1 else "PE"
            opt_df   = ce_df if opt_type == "CE" else pe_df
            prem     = get_option_price(opt_df, ts)

            if prem and prem > 0:
                entry_p = apply_slippage(prem, "buy")
                lots, half, trail = calc_lots(equity, entry_p, lot_sz)
                cost = entry_p * lot_sz * lots

                if lots > 0 and cost <= equity:
                    _eflag = info.get("expiry_flag", "MONTH")
                    open_trade = Trade(
                        symbol=name, universe=universe_key,
                        option_type=opt_type, signal=signal,
                        strike_mode=strike_mode, entry_time=ts,
                        entry_premium=entry_p, lot_size=lot_sz,
                        total_lots=lots, half_lots=half, trail_lots=trail,
                        sl_premium=0.01,
                        trail_premium=0.01,
                        peak_premium=entry_p,
                        underlying_entry=close_p,
                        strike_price=get_option_strike(opt_df, ts),
                        expiry_type="WEEKLY" if _eflag.upper() in ("WEEK","WEEKLY") else "MONTHLY",
                        expiry_date=compute_expiry_date(ts, _eflag),
                        entry_candle_open  = float(bar.get("open",  close_p)),
                        entry_candle_high  = float(bar.get("high",  close_p)),
                        entry_candle_low   = float(bar.get("low",   close_p)),
                        entry_candle_close = float(close_p),
                    )
                    equity -= cost
                    log.debug(
                        f"  [{name}] {opt_type} OPEN @ Rs{entry_p:.2f} "
                        f"strike={open_trade.strike_price:.0f} "
                        f"lots={lots} underlying=Rs{close_p:.2f} "
                        f"SL-level=Rs{open_trade.entry_candle_open:.2f}"
                    )

    # End-of-backtest: close any remaining open trade
    if open_trade:
        opt_df   = ce_df if open_trade.option_type == "CE" else pe_df
        last_ts  = bars[-1][0] if bars else pd.Timestamp.now(tz="Asia/Kolkata")
        last_bar = bars[-1][1] if bars else None
        p = get_option_price(opt_df, last_ts) or open_trade.entry_premium
        _close_trade(open_trade, last_ts, apply_slippage(p, "sell"),
                     "end_of_backtest", lot_sz, trades, kill, exit_bar=last_bar)

    return trades


# =============================================================================
# SECTION 10 — BACKTEST REPORT
# =============================================================================

def compute_concurrent_trades(df: pd.DataFrame) -> pd.Series:
    """
    For each trade row, count how many OTHER trades were active (open) during
    the same time window — i.e. their [entry_time, exit_time] interval overlaps
    with this trade's interval.

    Two trades overlap when:
        other.entry_time < this.exit_time  AND  other.exit_time > this.entry_time

    Returns an integer Series aligned to df.index.
    This is an O(n²) sweep — fine for typical backtest sizes (< 5,000 trades).
    """
    entry = pd.to_datetime(df["entry_time"], utc=True)
    exit_ = pd.to_datetime(df["exit_time"],  utc=True)

    counts = []
    for i in range(len(df)):
        e_i = entry.iloc[i]
        x_i = exit_.iloc[i]
        # Count rows j ≠ i where intervals overlap
        overlaps = (
            (entry < x_i) &   # other started before this one ended
            (exit_  > e_i) &  # other ended   after  this one started
            (df.index != df.index[i])  # exclude self
        )
        counts.append(int(overlaps.sum()))

    return pd.Series(counts, index=df.index, dtype=int)


def build_report(trades: List[Trade], capital: float) -> dict:
    """Build a comprehensive performance report from completed trades."""
    if not trades:
        return {"total_trades": 0}

    df = pd.DataFrame([{
        "symbol":              t.symbol,
        "universe":            t.universe,
        "option_type":         t.option_type,
        "strike_mode":         t.strike_mode,
        "strike_price":        t.strike_price,
        "expiry_type":         t.expiry_type,
        "expiry_date":         t.expiry_date,
        "entry_time":          t.entry_time,
        "exit_time":           t.exit_time,
        # Entry candle OHLC (underlying bar that triggered the signal)
        "entry_candle_open":   t.entry_candle_open,
        "entry_candle_high":   t.entry_candle_high,
        "entry_candle_low":    t.entry_candle_low,
        "entry_candle_close":  t.entry_candle_close,
        # Exit candle OHLC (underlying bar on which the trade was closed)
        "exit_candle_open":    t.exit_candle_open,
        "exit_candle_high":    t.exit_candle_high,
        "exit_candle_low":     t.exit_candle_low,
        "exit_candle_close":   t.exit_candle_close,
        "entry_premium":       t.entry_premium,
        "exit_premium":        t.exit_premium,
        "exit_reason":         t.exit_reason,
        "total_lots":          t.total_lots,
        "lot_size":            t.lot_size,
        "bars_held":           t.bars_held,
        "half_pnl":            t.half_pnl,
        "trail_pnl":           t.trail_pnl,
        "net_pnl":             t.net_pnl,
    } for t in trades])

    # ── trade_days column ────────────────────────────────────────────────────
    # Number of calendar days between entry and exit dates.
    #   0  → entry and exit on the same calendar day   (typical intraday)
    #   1  → held overnight, exited the following day
    #   N  → held N calendar days (e.g. over a weekend = 2 or 3)
    # Formula: exit_date - entry_date  (dates only, time stripped)
    entry_dates = pd.to_datetime(df["entry_time"]).dt.normalize()
    exit_dates  = pd.to_datetime(df["exit_time"]).dt.normalize()
    df["trade_days"] = (exit_dates - entry_dates).dt.days.fillna(0).astype(int)

    # ── Concurrent trades column ─────────────────────────────────────────────
    # How many OTHER trades were open (overlapping) at the same time as this one.
    # A value of 0 means this trade had no overlap with any other position.
    # A value of 3 means 3 other positions were open simultaneously.
    df["concurrent_trades"] = compute_concurrent_trades(df)

    total_pnl   = df["net_pnl"].sum()
    win_rate    = (df["net_pnl"] > 0).mean() * 100
    n_trades    = len(df)
    avg_pnl     = df["net_pnl"].mean()

    # Sharpe ratio (annualised, assuming 252 trading days × 25 bars/day)
    pnl_std = df["net_pnl"].std()
    sharpe  = (avg_pnl / pnl_std * math.sqrt(252 * 25)) if pnl_std > 0 else 0.0

    # Max drawdown
    cumulative = df["net_pnl"].cumsum()
    rolling_max = cumulative.cummax()
    drawdown    = cumulative - rolling_max
    max_dd      = drawdown.min()

    # Monthly PnL
    df["month"] = pd.to_datetime(df["entry_time"]).dt.to_period("M")
    monthly_pnl = df.groupby("month")["net_pnl"].sum().to_dict()

    # Yearly PnL
    df["year"] = pd.to_datetime(df["entry_time"]).dt.year
    yearly_pnl  = df.groupby("year")["net_pnl"].sum().to_dict()

    # Per-symbol summary
    sym_summary = (
        df.groupby(["symbol", "universe"])
          .agg(trades=("net_pnl", "count"),
               total_pnl=("net_pnl", "sum"),
               win_rate=("net_pnl", lambda x: (x > 0).mean() * 100))
          .sort_values("total_pnl", ascending=False)
    )

    # Per-universe summary
    uni_summary = (
        df.groupby("universe")
          .agg(trades=("net_pnl", "count"),
               total_pnl=("net_pnl", "sum"),
               win_rate=("net_pnl", lambda x: (x > 0).mean() * 100))
          .sort_values("total_pnl", ascending=False)
    )

    # Concurrent trades summary
    conc = df["concurrent_trades"]
    dist = conc.value_counts().sort_index().to_dict()
    solo = int((conc == 0).sum())
    concurrent_summary = {
        "max_concurrent":  int(conc.max()),
        "avg_concurrent":  round(float(conc.mean()), 2),
        "solo_trades":     solo,
        "solo_pct":        round(solo / len(df) * 100, 1),
        "distribution":    {int(k): int(v) for k, v in dist.items()},
    }

    return {
        "trades_df":          df,
        "total_trades":       n_trades,
        "total_pnl":          total_pnl,
        "win_rate":           win_rate,
        "avg_pnl":            avg_pnl,
        "sharpe":             sharpe,
        "max_drawdown":       max_dd,
        "return_pct":         total_pnl / capital * 100,
        "monthly_pnl":        monthly_pnl,
        "yearly_pnl":         yearly_pnl,
        "symbol_summary":     sym_summary,
        "universe_summary":   uni_summary,
        "concurrent_summary": concurrent_summary,
    }


def print_report(r: dict, capital: float) -> None:
    """Pretty-print backtest report to console and save CSVs."""
    if r.get("total_trades", 0) == 0:
        print("\nNo trades executed.")
        return

    print(f"\n{'='*65}")
    print(f"  BACKTEST RESULTS")
    print(f"{'='*65}")
    print(f"  Total trades  : {r['total_trades']}")
    print(f"  Total PnL     : ₹{r['total_pnl']:>12,.0f}  "
          f"({r['return_pct']:+.1f}% on ₹{capital:,.0f})")
    print(f"  Win rate      : {r['win_rate']:.1f}%")
    print(f"  Avg PnL/trade : ₹{r['avg_pnl']:>10,.0f}")
    print(f"  Sharpe ratio  : {r['sharpe']:.2f}")
    print(f"  Max drawdown  : ₹{r['max_drawdown']:>12,.0f}")

    # Concurrent trades summary
    if "concurrent_summary" in r:
        cs = r["concurrent_summary"]
        print(f"\n  Concurrent trades (simultaneous open positions):")
        print(f"    Max at any one time : {cs['max_concurrent']}")
        print(f"    Average concurrent  : {cs['avg_concurrent']:.2f}")
        print(f"    Trades with 0 overlap : {cs['solo_trades']} "
              f"({cs['solo_pct']:.1f}%)")
        print(f"    Distribution:")
        for n_conc, count in cs["distribution"].items():
            bar = "█" * min(30, count)
            print(f"      {n_conc} concurrent: {count:>4} trades  {bar}")

    # Expiry type breakdown
    df = r["trades_df"]
    if "expiry_type" in df.columns:
        print(f"\n  Expiry type breakdown:")
        for etype, grp in df.groupby("expiry_type"):
            wins = (grp["net_pnl"] > 0).sum()
            wr   = wins / len(grp) * 100
            print(f"    {etype:<8}  trades={len(grp):>4}  "
                  f"win_rate={wr:>5.1f}%  "
                  f"total_pnl=₹{grp['net_pnl'].sum():>10,.0f}")

    # Trade days breakdown
    if "trade_days" in df.columns:
        print(f"\n  Trade duration (calendar days):")
        print(f"    Same-day exits (0 days) : "
              f"{(df['trade_days']==0).sum():>4} trades")
        print(f"    Overnight (1 day)        : "
              f"{(df['trade_days']==1).sum():>4} trades")
        multi = df[df['trade_days'] > 1]
        if len(multi):
            print(f"    Multi-day (2+ days)      : "
                  f"{len(multi):>4} trades  "
                  f"(max {df['trade_days'].max()} days)")
        td_dist = df["trade_days"].value_counts().sort_index()
        for days, cnt in td_dist.items():
            bar = "█" * min(30, cnt)
            print(f"      {days} day(s): {cnt:>4} trades  {bar}")

    print(f"\n  By universe:")
    print(r["universe_summary"].to_string())
    print(f"\n  Top symbols:")
    print(r["symbol_summary"].head(10).to_string())
    print(f"\n  Yearly PnL:")
    for yr, pnl in r["yearly_pnl"].items():
        bar = ("+" if pnl >= 0 else "-") * min(40, int(abs(pnl) / 10_000))
        print(f"    {yr}: ₹{pnl:>10,.0f}  {bar}")
    print(f"{'='*65}\n")

    df = r["trades_df"]
    df.to_csv("bt_trades.csv", index=False)
    pd.DataFrame(
        [(str(k), v) for k, v in r["monthly_pnl"].items()],
        columns=["month", "pnl"]
    ).to_csv("bt_monthly.csv", index=False)
    print("  Saved: bt_trades.csv | bt_monthly.csv")


# =============================================================================
# SHARED CLOCK-ALIGNED SCHEDULER
# =============================================================================

def wait_for_next_candle(interval_min: int = 15) -> None:
    """
    Block until the next clock-aligned candle boundary.

    NSE candles close at exact multiples of interval_min past the hour:
        15m  → :00, :15, :30, :45
        5m   → :00, :05, :10, :15 … :55
        1m   → :00, :01, … :59

    Algorithm:
        1. Get current time in seconds since the top of the hour.
        2. Calculate how many seconds remain until the next multiple
           of (interval_min × 60).
        3. Add a small grace period (CANDLE_GRACE_SEC) so we fetch
           data AFTER Dhan has closed and published the candle.
        4. Sleep that many seconds.

    This guarantees the scanner fires at  9:15, 9:30, 9:45, 10:00 …
    regardless of when the program was started.
    """
    CANDLE_GRACE_SEC = 2        # wait 2 s after candle close before fetching
    interval_sec     = interval_min * 60

    now        = datetime.now()
    sec_in_hour= now.minute * 60 + now.second
    elapsed    = sec_in_hour % interval_sec
    remaining  = (interval_sec - elapsed) + CANDLE_GRACE_SEC

    next_fire  = now + timedelta(seconds=remaining)
    log.info(
        f"⏳ Next scan scheduled at "
        f"{next_fire.strftime('%H:%M:%S')}  "
        f"(sleeping {int(remaining)}s)"
    )
    time.sleep(remaining)


def is_candle_time(interval_min: int = 15) -> bool:
    """
    Return True if right now is within CANDLE_GRACE_SEC seconds
    after a clock-aligned candle boundary.
    Used as a safety guard inside the loop.
    """
    CANDLE_GRACE_SEC = 10
    now     = datetime.now()
    elapsed = (now.minute * 60 + now.second) % (interval_min * 60)
    return elapsed <= CANDLE_GRACE_SEC


# =============================================================================
# SECTION 11 — PAPER TRADING ENGINE
# =============================================================================

class PaperTrader:
    """
    Paper trading engine — scans live market every 15 minutes.
    Uses identical signal logic as backtest.
    Logs every entry and exit to paper_trades_YYYY-MM-DD.json.
    No real orders are ever placed.
    """

    def __init__(self, capital: float, strike_mode: str,
                 universes: List[str], symbols: Optional[List[str]] = None):
        self.capital     = capital
        self.equity      = capital
        self.strike_mode = strike_mode
        self.kill        = KillSwitch(capital)
        self.positions: Dict[str, LivePosition] = {}
        self.closed:    List[dict]              = []
        self.scan_count = 0
        self._lock      = threading.Lock()
        self.journal    = f"paper_trades_{date.today()}.json"

        # Build scan target list: (name, universe_key, info_dict)
        self.targets: List[tuple] = []
        for uni_key in universes:
            uni = ALL_UNIVERSES.get(uni_key, {})
            for name, info in uni.items():
                if symbols and name not in symbols:
                    continue
                self.targets.append((name, uni_key, info))

        log.info(f"PaperTrader ready | capital=₹{capital:,.0f} | "
                 f"universes={universes} | symbols={len(self.targets)}")

    def _market_open(self) -> bool:
        now = datetime.now()
        if now.weekday() >= 5:
            return False
        return MARKET_OPEN <= now.strftime("%H:%M") <= MARKET_CLOSE

    def _get_signal(self, name: str, info: dict) -> tuple:
        """Fetch recent price data, compute indicators, return (signal, latest_bar).

        Uses safe_from_date() which:
          • Adds a 50% calendar-day buffer so we always get enough trading bars
          • Snaps to the nearest weekday to avoid 400 errors on Dhan intraday API
        """
        from_d = safe_from_date(DATA_LOOKBACK_DAYS)
        to_d   = date.today().strftime("%Y-%m-%d")

        wave_raw = fetch_price_15m(
            info["eq_sid"], from_d, to_d,
            segment=info["eq_segment"], instrument=info["eq_instrument"],
        )
        if wave_raw.empty or len(wave_raw) < 60:
            return 0, None
        if wave_raw["volume"].sum() == 0:
            wave_raw["volume"] = 1_000_000

        tide_raw = resample_to_60m(wave_raw.copy())
        wave = compute_indicators(wave_raw.copy())
        tide = compute_indicators(tide_raw.copy())

        if wave.empty or tide.empty:
            log.warning(f"[{name}] Not enough data for indicators (lookback too short).")
            return 0, None
        if not _REQUIRED_COLS.issubset(wave.columns) or \
           not _REQUIRED_COLS.issubset(tide.columns):
            return 0, None

        wave = generate_signals(wave, tide)
        latest = wave.iloc[-1]
        return int(latest.get("signal", 0)), latest

    def _log_event(self, event: dict) -> None:
        """Append event to today's JSON journal."""
        try:
            existing = []
            if os.path.exists(self.journal):
                with open(self.journal) as f:
                    existing = json.load(f)
            existing.append(event)
            with open(self.journal, "w") as f:
                json.dump(existing, f, indent=2, default=str)
        except Exception as e:
            log.warning(f"Journal write error: {e}")

    def scan(self) -> None:
        """Main scan loop — called every 15 minutes by scheduler."""
        if not self._market_open():
            log.info("Market closed — skipping scan.")
            return
        if self.kill.is_on():
            log.critical("Kill switch active — no new entries.")
            return

        self.scan_count += 1
        log.info(f"\n{'─'*55}")
        log.info(f"PAPER SCAN #{self.scan_count} @ {datetime.now().strftime('%H:%M:%S')}")
        log.info(f"Equity=₹{self.equity:,.2f} | Open={len(self.positions)}")

        for name, uni_key, info in self.targets:
            with self._lock:
                self._scan_one(name, uni_key, info)

        # Print running P&L
        total = sum(t["net_pnl"] for t in self.closed)
        log.info(f"  Running closed P&L: ₹{total:+,.0f} | "
                 f"Trades: {len(self.closed)}")

    def _scan_one(self, name: str, uni_key: str, info: dict) -> None:
        """Process one symbol: manage open position or look for entry."""
        lot_sz = info["lot_size"]

        # ── Manage open position ────────────────────────────────────────────
        if name in self.positions:
            pos  = self.positions[name]

            # Fetch live option premium
            prem = fetch_live_option_ltp(
                pos.opt_sid, pos.option_type,
                CE_OFFSETS[pos.strike_mode] if pos.option_type == "CE"
                    else PE_OFFSETS[pos.strike_mode],
                pos.opt_segment, pos.opt_instrument, pos.expiry_flag,
            )
            if prem is None:
                log.debug(f"[{name}] Could not fetch live LTP — skipping exit check.")
                return

            # Update trailing stop on premium
            if prem > pos.peak_premium:
                pos.peak_premium = prem
                new_trail = prem * (1 - TRAIL_PCT)
                if new_trail > pos.trail_premium:
                    pos.trail_premium = round(new_trail, 2)

            # HALF-EXIT: option premium rises >= 10% from entry
            if not pos.half_done and pos.half_lots > 0:
                prem_move_pct = (prem - pos.entry_premium) / pos.entry_premium * 100
                if prem_move_pct >= HALF_EXIT_TRIGGER_PCT:
                    exit_p   = apply_slippage(prem, "sell")
                    half_qty = pos.half_lots * lot_sz
                    half_pnl = (exit_p - pos.entry_premium) * half_qty
                    pos.half_pnl      = half_pnl
                    pos.sl_premium    = pos.entry_premium   # SL to breakeven
                    pos.trail_premium = max(pos.trail_premium, pos.entry_premium)
                    pos.half_done     = True
                    log.info(f"[{name}] PAPER HALF-EXIT @ ₹{exit_p:.2f} "
                             f"(premium +{prem_move_pct:.1f}%) "
                             f"pnl=₹{half_pnl:+,.0f}")
                    self._log_event({
                        "event": "half_exit", "symbol": name,
                        "option_type": pos.option_type,
                        "exit_premium": exit_p, "half_pnl": half_pnl,
                        "time": datetime.now().isoformat(),
                    })

            # UNDERLYING-CHART SL: fetch latest underlying price
            # CE: exit if current underlying close < entry_candle_open
            # PE: exit if current underlying close > entry_candle_open
            reason = None
            if pos.entry_candle_open > 0:
                from_d = safe_from_date(3)   # just need last few bars
                to_d   = date.today().strftime("%Y-%m-%d")
                ul_df  = fetch_price_15m(
                    info["eq_sid"], from_d, to_d,
                    segment=info["eq_segment"],
                    instrument=info["eq_instrument"],
                )
                if not ul_df.empty:
                    current_ul = float(ul_df["close"].iloc[-1])
                    if pos.option_type == "CE" and current_ul < pos.entry_candle_open:
                        reason = "underlying_sl"
                    elif pos.option_type == "PE" and current_ul > pos.entry_candle_open:
                        reason = "underlying_sl"

            # EMA-5 exit (secondary)
            if reason is None:
                from_d = safe_from_date(DATA_LOOKBACK_DAYS)
                to_d   = date.today().strftime("%Y-%m-%d")
                ul_df  = fetch_price_15m(
                    info["eq_sid"], from_d, to_d,
                    segment=info["eq_segment"],
                    instrument=info["eq_instrument"],
                )
                if not ul_df.empty:
                    ul_ind = compute_indicators(ul_df.copy())
                    if not ul_ind.empty and "ema5" in ul_ind.columns:
                        cur_close = float(ul_ind["close"].iloc[-1])
                        cur_ema5  = float(ul_ind["ema5"].iloc[-1])
                        if pos.option_type == "CE" and cur_close < cur_ema5:
                            reason = "below_ema5"
                        elif pos.option_type == "PE" and cur_close > cur_ema5:
                            reason = "above_ema5"

            # Trailing stop on premium
            if reason is None:
                if (pos.trail_premium > pos.entry_premium
                        and prem <= pos.trail_premium):
                    reason = "trail_stop"

            # EOD
            if reason is None:
                now = datetime.now()
                if now.hour == EOD_HOUR and now.minute >= EOD_MINUTE:
                    reason = "eod"

            if reason:
                self._close_position(name, pos, prem, reason, lot_sz)
            return

        # ── Look for new entry ──────────────────────────────────────────────
        signal, latest = self._get_signal(name, info)
        if signal == 0 or latest is None:
            return

        opt_type  = "CE" if signal == 1 else "PE"
        strike_off = (CE_OFFSETS if signal == 1 else PE_OFFSETS).get(
            self.strike_mode, "ATM")
        prem = fetch_live_option_ltp(
            info["opt_sid"], opt_type, strike_off,
            info["opt_segment"], info["opt_instrument"], info["expiry_flag"],
        )
        if not prem or prem <= 0:
            return

        entry_p = apply_slippage(prem, "buy")
        lots, half, trail = calc_lots(self.equity, entry_p, lot_sz)
        cost = entry_p * lot_sz * lots
        if lots <= 0 or cost > self.equity:
            return

        pos  = LivePosition(
            symbol=name, universe=uni_key, option_type=opt_type,
            signal=signal, strike_mode=self.strike_mode,
            entry_time=datetime.now(), entry_premium=entry_p,
            lot_size=lot_sz, total_lots=lots, half_lots=half, trail_lots=trail,
            # sl_premium set to 0.01 — actual SL is underlying-chart based
            sl_premium=0.01, trail_premium=0.01,
            peak_premium=entry_p, underlying_entry=float(latest["close"]),
            strike_price=0.0,
            opt_sid=info["opt_sid"], opt_segment=info["opt_segment"],
            opt_instrument=info["opt_instrument"], expiry_flag=info["expiry_flag"],
            # Store entry candle OHLC — entry_candle_open is the SL trigger level
            entry_candle_open  = float(latest.get("open",  latest["close"])),
            entry_candle_high  = float(latest.get("high",  latest["close"])),
            entry_candle_low   = float(latest.get("low",   latest["close"])),
            entry_candle_close = float(latest["close"]),
        )
        self.positions[name] = pos
        self.equity -= cost

        event = {"type": "ENTRY", "symbol": name, "universe": uni_key,
                 "option_type": opt_type, "lots": lots,
                 "entry_premium": entry_p, "sl": sl_p,
                 "time": datetime.now().isoformat()}
        self._log_event(event)
        log.info(f"  [PAPER ENTRY] {name} {opt_type} @ ₹{entry_p:.2f} "
                 f"lots={lots} SL=₹{sl_p:.2f}")

    def _close_position(self, name: str, pos: LivePosition,
                        exit_prem: float, reason: str, lot_sz: int) -> None:
        """Close a paper position, compute PnL, log the trade."""
        exit_p    = apply_slippage(exit_prem, "sell")
        rem_lots  = pos.trail_lots if pos.half_done else pos.total_lots
        trail_pnl = (exit_p - pos.entry_premium) * rem_lots * lot_sz
        gross     = pos.half_pnl + trail_pnl
        brok      = BROKERAGE_PER_LOT * pos.total_lots * 2
        net       = gross - brok

        self.equity += exit_p * rem_lots * lot_sz
        self.kill.record(net)
        del self.positions[name]

        record = {
            "type": "EXIT", "symbol": name, "universe": pos.universe,
            "option_type": pos.option_type, "lots": pos.total_lots,
            "entry_premium": pos.entry_premium, "exit_premium": exit_p,
            "reason": reason, "net_pnl": net,
            "time": datetime.now().isoformat(),
        }
        self.closed.append(record)
        self._log_event(record)
        log.info(f"  [PAPER EXIT] {name} {pos.option_type} @ ₹{exit_p:.2f} "
                 f"reason={reason} net_pnl=₹{net:+,.0f}")

    def run(self) -> None:
        """
        Start the paper trading scheduler — runs until Ctrl+C.

        Clock-aligned firing: scans at exact 15-minute marks
        (9:15, 9:30, 9:45, 10:00 … 15:15) regardless of start time.

        Flow per iteration:
          1. If market is closed → sleep to the next 15-min boundary and retry.
          2. Run self.scan() — checks all open positions and looks for entries.
          3. Sleep until the next 15-min clock boundary + 5s grace period.
        """
        log.info("Paper trading started (clock-aligned). Press Ctrl+C to stop.")
        log.info(
            f"Will scan at exact :00/:15/:30/:45 marks between "
            f"{MARKET_OPEN} and {MARKET_CLOSE}."
        )

        # ── First scan: run immediately if we are AT a candle boundary,
        #    otherwise wait for the next one. ──────────────────────────────
        if is_candle_time(SCAN_INTERVAL_MIN):
            log.info("Starting at a candle boundary — scanning now.")
            self.scan()
        else:
            log.info("Waiting for the first 15-min candle boundary …")
            wait_for_next_candle(SCAN_INTERVAL_MIN)
            self.scan()

        # ── Main loop ────────────────────────────────────────────────────
        try:
            while True:
                wait_for_next_candle(SCAN_INTERVAL_MIN)
                self.scan()
        except KeyboardInterrupt:
            log.info("Paper trading stopped by user.")
            self._squareoff_all("manual_stop")


# =============================================================================
# SECTION 12 — LIVE TRADING ENGINE
# =============================================================================

class LiveTrader:
    """
    Live trading engine — places REAL orders via Dhan v2 API.
    Uses dhanhq SDK for order management.

    WARNING: This places REAL orders with REAL money.
             Test thoroughly in paper mode first.
             Static IP whitelisting is REQUIRED by Dhan for order placement.
    """

    def __init__(self, capital: float, strike_mode: str,
                 universes: List[str], symbols: Optional[List[str]] = None):
        try:
            from dhanhq import DhanContext, dhanhq
        except ImportError:
            raise ImportError("Run: pip install dhanhq")

        token = get_access_token()
        self.ctx  = DhanContext(DHAN_CLIENT_ID, token)
        self.dhan = dhanhq(self.ctx)

        self.capital     = capital
        self.equity      = capital
        self.strike_mode = strike_mode
        self.kill        = KillSwitch(capital)
        self.positions: Dict[str, LivePosition] = {}
        self.closed:    List[dict]              = []
        self.scan_count = 0
        self._lock      = threading.Lock()
        self.journal    = f"live_trades_{date.today()}.json"

        self.targets: List[tuple] = []
        for uni_key in universes:
            uni = ALL_UNIVERSES.get(uni_key, {})
            for name, info in uni.items():
                if symbols and name not in symbols:
                    continue
                self.targets.append((name, uni_key, info))

        log.info(f"LiveTrader ready | capital=₹{capital:,.0f} | "
                 f"universes={universes} | symbols={len(self.targets)}")
        log.warning("⚠️  LIVE TRADING ACTIVE — REAL ORDERS WILL BE PLACED")

    def _market_open(self) -> bool:
        now = datetime.now()
        if now.weekday() >= 5:
            return False
        return MARKET_OPEN <= now.strftime("%H:%M") <= MARKET_CLOSE

    def _get_signal(self, name: str, info: dict) -> tuple:
        """Same signal logic as paper trader."""
        today  = date.today()
        from_d = (today - timedelta(days=DATA_LOOKBACK_DAYS)).strftime("%Y-%m-%d")
        to_d   = today.strftime("%Y-%m-%d")

        wave_raw = fetch_price_15m(
            info["eq_sid"], from_d, to_d,
            segment=info["eq_segment"], instrument=info["eq_instrument"],
        )
        if wave_raw.empty or len(wave_raw) < 60:
            return 0, None
        if wave_raw["volume"].sum() == 0:
            wave_raw["volume"] = 1_000_000

        tide_raw = resample_to_60m(wave_raw.copy())
        wave = compute_indicators(wave_raw.copy())
        tide = compute_indicators(tide_raw.copy())

        if wave.empty or tide.empty:
            return 0, None
        if not _REQUIRED_COLS.issubset(wave.columns) or \
           not _REQUIRED_COLS.issubset(tide.columns):
            return 0, None

        wave = generate_signals(wave, tide)
        latest = wave.iloc[-1]
        return int(latest.get("signal", 0)), latest

    def _place_order(self, security_id: str, exchange_segment: str,
                     transaction_type: str, quantity: int,
                     product_type: str = "INTRADAY",
                     order_type: str   = "MARKET",
                     price: float = 0.0,
                     correlation_id: str = "") -> Optional[str]:
        """
        Place a real order via Dhan API.
        Returns order_id on success, None on failure.
        """
        try:
            resp = self.dhan.place_order(
                security_id    = security_id,
                exchange_segment = exchange_segment,
                transaction_type = transaction_type,
                quantity       = quantity,
                order_type     = order_type,
                product_type   = product_type,
                price          = price,
            )
            order_id = resp.get("orderId") or resp.get("data", {}).get("orderId")
            if order_id:
                log.info(f"  Order placed: {transaction_type} {quantity} qty "
                         f"[{exchange_segment}:{security_id}] → orderId={order_id}")
                return str(order_id)
            else:
                log.error(f"  Order failed — no orderId in response: {resp}")
                return None
        except Exception as e:
            log.error(f"  Order placement exception: {e}")
            return None

    def _cancel_order(self, order_id: str) -> bool:
        """Cancel a pending order."""
        try:
            self.dhan.cancel_order(order_id)
            log.info(f"  Order cancelled: {order_id}")
            return True
        except Exception as e:
            log.error(f"  Cancel order error [{order_id}]: {e}")
            return False

    def _get_funds(self) -> float:
        """Fetch available cash balance from Dhan."""
        try:
            resp = self.dhan.get_fund_limits()
            return float(resp.get("availabelBalance", self.equity))
        except Exception:
            return self.equity

    def _log_event(self, event: dict) -> None:
        try:
            existing = []
            if os.path.exists(self.journal):
                with open(self.journal) as f:
                    existing = json.load(f)
            existing.append(event)
            with open(self.journal, "w") as f:
                json.dump(existing, f, indent=2, default=str)
        except Exception as e:
            log.warning(f"Journal write error: {e}")

    def scan(self) -> None:
        """Main scan — called every 15 minutes."""
        if not self._market_open():
            log.info("Market closed — skipping scan.")
            return
        if self.kill.is_on():
            log.critical("Kill switch active — squaring off all positions.")
            self._squareoff_all()
            return

        self.scan_count += 1
        self.equity = self._get_funds()
        log.info(f"\n{'─'*55}")
        log.info(f"LIVE SCAN #{self.scan_count} @ {datetime.now().strftime('%H:%M:%S')}")
        log.info(f"Equity=₹{self.equity:,.2f} | Open={len(self.positions)}")

        for name, uni_key, info in self.targets:
            with self._lock:
                self._scan_one(name, uni_key, info)

    def _scan_one(self, name: str, uni_key: str, info: dict) -> None:
        """Process one symbol for live trading."""
        lot_sz = info["lot_size"]

        # ── Manage open position ────────────────────────────────────────────
        if name in self.positions:
            pos  = self.positions[name]
            prem = fetch_live_option_ltp(
                pos.opt_sid, pos.option_type,
                CE_OFFSETS[pos.strike_mode] if pos.option_type == "CE"
                    else PE_OFFSETS[pos.strike_mode],
                pos.opt_segment, pos.opt_instrument, pos.expiry_flag,
            )
            if prem is None:
                return

            if prem > pos.peak_premium:
                pos.peak_premium = prem
                new_trail = prem * (1 - TRAIL_PCT)
                if new_trail > pos.trail_premium:
                    pos.trail_premium = round(new_trail, 2)

            reason = None
            if prem <= pos.sl_premium:
                reason = "stop_loss"
            elif pos.trail_premium > pos.entry_premium and prem <= pos.trail_premium:
                reason = "trail_stop"
            elif datetime.now().hour == EOD_HOUR and datetime.now().minute >= EOD_MINUTE:
                reason = "eod"

            if reason:
                self._exit_position(name, pos, prem, reason, lot_sz, info)
            return

        # ── Look for new entry ──────────────────────────────────────────────
        signal, latest = self._get_signal(name, info)
        if signal == 0 or latest is None:
            return

        opt_type   = "CE" if signal == 1 else "PE"
        strike_off = (CE_OFFSETS if signal == 1 else PE_OFFSETS).get(
            self.strike_mode, "ATM")
        prem = fetch_live_option_ltp(
            info["opt_sid"], opt_type, strike_off,
            info["opt_segment"], info["opt_instrument"], info["expiry_flag"],
        )
        if not prem or prem <= 0:
            return

        entry_p = apply_slippage(prem, "buy")
        lots, half, trail = calc_lots(self.equity, entry_p, lot_sz)
        qty  = lots * lot_sz
        cost = entry_p * qty
        if lots <= 0 or cost > self.equity:
            log.warning(f"  [{name}] Insufficient capital for entry "
                        f"(need ₹{cost:,.0f}, have ₹{self.equity:,.0f})")
            return

        # Place real buy order
        order_id = self._place_order(
            security_id      = str(info["opt_sid"]),
            exchange_segment = info["opt_segment"],
            transaction_type = "BUY",
            quantity         = qty,
            product_type     = info["product_type"],
        )
        if not order_id:
            return  # order failed

        sl_p = entry_p * (1 - OPTION_SL_PCT / 100)
        pos  = LivePosition(
            symbol=name, universe=uni_key, option_type=opt_type,
            signal=signal, strike_mode=self.strike_mode,
            entry_time=datetime.now(), entry_premium=entry_p,
            lot_size=lot_sz, total_lots=lots, half_lots=half, trail_lots=trail,
            sl_premium=round(sl_p, 2), trail_premium=round(sl_p, 2),
            peak_premium=entry_p, underlying_entry=float(latest["close"]),
            strike_price=0.0,
            opt_sid=info["opt_sid"], opt_segment=info["opt_segment"],
            opt_instrument=info["opt_instrument"], expiry_flag=info["expiry_flag"],
            entry_order_id=order_id,
        )
        self.positions[name] = pos
        self.equity -= cost

        event = {"type": "LIVE_ENTRY", "symbol": name, "universe": uni_key,
                 "option_type": opt_type, "lots": lots, "qty": qty,
                 "entry_premium": entry_p, "sl": sl_p, "order_id": order_id,
                 "time": datetime.now().isoformat()}
        self._log_event(event)
        log.info(f"  [LIVE ENTRY] {name} {opt_type} qty={qty} @ ₹{entry_p:.2f} "
                 f"SL=₹{sl_p:.2f} orderId={order_id}")

    def _exit_position(self, name: str, pos: LivePosition,
                       exit_prem: float, reason: str,
                       lot_sz: int, info: dict) -> None:
        """Place a SELL order to exit the position."""
        rem_lots = pos.trail_lots if pos.half_done else pos.total_lots
        qty      = rem_lots * lot_sz
        exit_p   = apply_slippage(exit_prem, "sell")

        order_id = self._place_order(
            security_id      = str(pos.opt_sid),
            exchange_segment = pos.opt_segment,
            transaction_type = "SELL",
            quantity         = qty,
            product_type     = info["product_type"],
        )

        trail_pnl = (exit_p - pos.entry_premium) * qty
        gross     = pos.half_pnl + trail_pnl
        brok      = BROKERAGE_PER_LOT * pos.total_lots * 2
        net       = gross - brok

        self.equity += exit_p * qty
        self.kill.record(net)
        del self.positions[name]

        record = {
            "type": "LIVE_EXIT", "symbol": name, "universe": pos.universe,
            "option_type": pos.option_type, "qty": qty,
            "entry_premium": pos.entry_premium, "exit_premium": exit_p,
            "reason": reason, "net_pnl": net,
            "entry_order_id": pos.entry_order_id,
            "exit_order_id": order_id or "FAILED",
            "time": datetime.now().isoformat(),
        }
        self.closed.append(record)
        self._log_event(record)
        log.info(f"  [LIVE EXIT] {name} {pos.option_type} qty={qty} @ ₹{exit_p:.2f} "
                 f"reason={reason} net_pnl=₹{net:+,.0f}")

    def _squareoff_all(self) -> None:
        """Emergency square-off all open positions."""
        for name, pos in list(self.positions.items()):
            info = None
            for _, uni_key, inf in self.targets:
                if _ == name:
                    info = inf
                    break
            if not info:
                continue
            lot_sz = info["lot_size"]
            prem   = fetch_live_option_ltp(
                pos.opt_sid, pos.option_type, "ATM",
                pos.opt_segment, pos.opt_instrument, pos.expiry_flag,
            ) or pos.entry_premium
            self._exit_position(name, pos, prem, "squareoff_all", lot_sz, info)

    def run(self) -> None:
        """
        Start the LIVE trading scheduler — runs until Ctrl+C.

        ⚠️  REAL MONEY. Real orders. Use with extreme caution.

        Clock-aligned firing: scans at exact 15-minute marks
        (9:15, 9:30, 9:45, 10:00 … 15:15) regardless of start time.

        Flow per iteration:
          1. If market is closed → sleep to the next 15-min boundary and retry.
          2. Run self.scan() — manages open positions, places new orders.
          3. Sleep until the next 15-min clock boundary + 5s grace period.
        """
        log.warning("═" * 55)
        log.warning("  LIVE TRADING SCHEDULER STARTED — REAL MONEY MODE")
        log.warning(f"  Clock-aligned: fires at :00/:15/:30/:45 marks")
        log.warning(f"  Market hours : {MARKET_OPEN} – {MARKET_CLOSE}")
        log.warning("  Press Ctrl+C to stop and square off all positions.")
        log.warning("═" * 55)

        # ── First scan ───────────────────────────────────────────────────
        if is_candle_time(SCAN_INTERVAL_MIN):
            log.info("Starting at a candle boundary — scanning now.")
            self.scan()
        else:
            log.info("Waiting for the first 15-min candle boundary …")
            wait_for_next_candle(SCAN_INTERVAL_MIN)
            self.scan()

        # ── Main loop ────────────────────────────────────────────────────
        try:
            while True:
                wait_for_next_candle(SCAN_INTERVAL_MIN)
                self.scan()
        except KeyboardInterrupt:
            log.warning("Live trading stopped by user — squaring off all positions.")
            self._squareoff_all("manual_stop")


# =============================================================================
# SECTION 13 — MODE RUNNERS
# =============================================================================

def run_backtest(args) -> None:
    """Backtest mode — iterate over all requested instruments historically."""
    universes = args.universe
    capital   = args.capital

    # Resolve instrument list
    targets: List[tuple] = []
    for uni_key in universes:
        uni = ALL_UNIVERSES.get(uni_key, {})
        for name, info in uni.items():
            if args.symbols and name not in args.symbols:
                continue
            targets.append((name, uni_key, info))

    if not targets:
        print("No valid instruments found for the requested universes/symbols.")
        sys.exit(1)

    print(f"\n{'='*60}")
    print(f"  BACKTEST — Dhan API")
    print(f"  Universes : {universes}")
    print(f"  Symbols   : {len(targets)}")
    print(f"  Period    : {args.from_date} → {args.to_date}")
    print(f"  Strike    : {args.strike}")
    print(f"  Capital   : ₹{capital:,.0f}")
    print(f"  Risk/trade: {args.risk}%  SL: {OPTION_SL_PCT}%  Trail: {int(TRAIL_PCT*100)}%")
    print(f"{'='*60}\n")

    kill       = KillSwitch(capital)
    all_trades: List[Trade] = []

    for i, (name, uni_key, info) in enumerate(targets, 1):
        print(f"[{i:02d}/{len(targets)}] {name} ({uni_key}) ...", flush=True)
        kill.reset()
        trades = run_one_instrument(
            name=name, universe_key=uni_key, info=info,
            from_date=args.from_date, to_date=args.to_date,
            strike_mode=args.strike, capital=capital, kill=kill,
        )
        all_trades.extend(trades)
        print(f"         {len(trades)} trades")
        time.sleep(1.0)

    if not all_trades:
        print("\nNo trades executed. Check date range and credentials.")
        return

    report = build_report(all_trades, capital)
    print_report(report, capital)


def run_paper(args) -> None:
    """Paper trading mode — live scan, no real orders."""
    trader = PaperTrader(
        capital=args.capital,
        strike_mode=args.strike,
        universes=args.universe,
        symbols=args.symbols or None,
    )
    trader.run()


def run_live(args) -> None:
    """Live trading mode — real orders via Dhan API."""
    print("\n" + "!"*60)
    print("  WARNING: LIVE TRADING MODE")
    print("  Real orders will be placed with real money.")
    print("  Ensure static IP is whitelisted with Dhan.")
    print("  Press Ctrl+C within 5 seconds to abort...")
    print("!"*60 + "\n")
    time.sleep(5)

    trader = LiveTrader(
        capital=args.capital,
        strike_mode=args.strike,
        universes=args.universe,
        symbols=args.symbols or None,
    )
    trader.run()


# =============================================================================
# SECTION 14 — CLI ENTRY POINT
# =============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Dhan Options Trading System — Backtest / Paper / Live",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
EXAMPLES:
  # Backtest — all indices, 3 years, ATM
  python dhan_trading_system.py --mode backtest --universe index --from 2022-01-01 --to 2025-12-31 --strike ATM

  # Backtest — currency + commodity
  python dhan_trading_system.py --mode backtest --universe currency commodity --from 2022-01-01 --to 2025-12-31

  # Backtest — selected stocks
  python dhan_trading_system.py --mode backtest --universe stocks --symbols RELIANCE TCS HDFCBANK --strike OTM1

  # Backtest — everything
  python dhan_trading_system.py --mode backtest --universe stocks index currency commodity

  # Paper trading — indices + currency
  python dhan_trading_system.py --mode paper --universe index currency

  # Live trading — NIFTY only (REAL MONEY)
  python dhan_trading_system.py --mode live --universe index --symbols NIFTY --strike ATM --capital 500000
        """
    )
    parser.add_argument(
        "--mode", required=True, choices=["backtest", "paper", "live"],
        help="backtest=historical sim | paper=live no-orders | live=real orders",
    )
    parser.add_argument(
        "--universe", nargs="+",
        choices=["stocks", "index", "currency", "commodity"],
        default=["index"],
        help="Which instrument universes to trade (default: index)",
    )
    parser.add_argument(
        "--symbols", nargs="+",
        help="Optional subset of symbols within the chosen universes",
    )
    parser.add_argument("--from",   dest="from_date", default="2022-01-01")
    parser.add_argument("--to",     dest="to_date",   default="2025-12-31")
    parser.add_argument("--strike", default="ATM", choices=["ATM","OTM1","OTM2","ITM1"])
    parser.add_argument("--capital",type=float, default=100_000)
    parser.add_argument("--risk",   type=float, default=2,
                        help="Risk %% of capital per trade (default 2)")
    parser.add_argument("--verbose",action="store_true")
    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    # Apply risk override
    global RISK_PCT_PER_TRADE
    RISK_PCT_PER_TRADE = args.risk

    # Resolve token and create global session
    global SESSION, DHAN_ACCESS_TOKEN
    DHAN_ACCESS_TOKEN = get_access_token()
    SESSION = make_session(DHAN_ACCESS_TOKEN)

    # Validate token via user profile
    try:
        resp = SESSION.get(f"{BASE_URL}/profile", timeout=10)
        profile = resp.json()
        log.info(f"Authenticated as: {profile.get('dhanClientId')} | "
                 f"Token valid until: {profile.get('tokenValidity')}")
    except Exception as e:
        log.warning(f"Could not validate token: {e}")

    if   args.mode == "backtest": run_backtest(args)
    elif args.mode == "paper":    run_paper(args)
    elif args.mode == "live":     run_live(args)


if __name__ == "__main__":
    main()
