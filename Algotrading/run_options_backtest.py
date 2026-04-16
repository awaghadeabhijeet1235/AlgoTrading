#!/usr/bin/env python3
# =============================================================================
# run_options_backtest.py  — SELF-CONTAINED Options Backtest
# =============================================================================
# HOW TO RUN:
#   pip install requests pandas numpy
#
#   python run_options_backtest.py --strike ATM  --from 2022-01-01 --to 2024-12-31
#   python run_options_backtest.py --strike OTM1 --from 2023-01-01 --to 2024-12-31
#   python run_options_backtest.py --stocks RELIANCE HDFCBANK --strike ATM
#
# Index options
#python run_options_backtest.py --mode index --index NIFTY --from 2022-01-01 --to 2024-12-31
#python run_options_backtest.py --mode index --index BANKNIFTY SENSEX --strike OTM1

# Stock options (unchanged behaviour)
#python run_options_backtest.py --strike ATM --from 2022-01-01 --to 2024-12-31

# Both together
#python run_options_backtest.py --mode both --stocks RELIANCE TCS --index NIFTY

# Set your Dhan credentials either:
#   Option A: edit DHAN_CLIENT_ID / DHAN_ACCESS_TOKEN below
#   Option B: set environment variables DHAN_CLIENT_ID and DHAN_ACCESS_TOKEN

''' how to run this code
# ATM options, 3 years, all supported stocks
python run_options_backtest.py --strike ATM --from 2022-01-01 --to 2024-12-31

# OTM1 options, select stocks only
python run_options_backtest.py --strike OTM1 --stocks RELIANCE HDFCBANK TCS INFY SBIN

# Custom capital and risk
python run_options_backtest.py --capital 1000000 --risk 1.0 --strike ATM
'''

# =============================================================================

import os, sys, math, time, argparse, logging
from datetime import datetime, timedelta, date
from dataclasses import dataclass
from typing import Optional

import requests
import pandas as pd
import numpy as np

# ─── CONFIGURE THESE ──────────────────────────────────────────────────────────
DHAN_CLIENT_ID    = os.getenv("DHAN_CLIENT_ID",    "1111077247")
DHAN_ACCESS_TOKEN = os.getenv("DHAN_ACCESS_TOKEN",  "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJpc3MiOiJkaGFuIiwicGFydG5lcklkIjoiIiwiZXhwIjoxNzc2MzM2OTQ5LCJpYXQiOjE3NzYyNTA1NDksInRva2VuQ29uc3VtZXJUeXBlIjoiU0VMRiIsIndlYmhvb2tVcmwiOiIiLCJkaGFuQ2xpZW50SWQiOiIxMTExMDc3MjQ3In0.aygNNKPt8vqLi4afDvjFgA-UpMPIolNVPp7S_U16q5NbXA4WnkrdeUOtcVUcfqen9QJK941MJc4iYcQm6KtN3w")
# DHAN_CLIENT_ID = "YOUR_ID_HERE"  # set via env var or edit above

# ─── STRATEGY PARAMETERS ──────────────────────────────────────────────────────
INITIAL_CAPITAL       = 500_000   # ₹5 Lakhs default
RISK_PCT_PER_TRADE    = 1.5       # max % of capital to risk per trade
OPTION_SL_PCT         = 40.0      # stop loss: exit if premium drops 40%
TRAIL_PCT             = 0.20      # trailing stop: 20% below peak premium
HALF_EXIT_TRIGGER_PCT = 0.5       # exit half when underlying moves 0.5%
KILL_SWITCH_PCT       = 10.0      # halt if daily loss > 10% of capital
SLIPPAGE_PCT          = 0.05      # 0.05% slippage per side
BROKERAGE_PER_LOT     = 20.0      # ₹20 per lot (Dhan flat fee)
EOD_HOUR, EOD_MINUTE  = 15, 15   # square off at 15:15 IST

# ─── INDICATOR SETTINGS ────────────────────────────────────────────────────────
BB_LEN=20; BB_STD=2.0
EMA5=5; EMA13=13; EMA26=26; EMA50=50
VOL_MA=20
MACD_F=12; MACD_S=26; MACD_SIG=9
RSI_LEN=14; ADX_LEN=14

# ─── API SETTINGS ──────────────────────────────────────────────────────────────
BASE_URL         = "https://api.dhan.co/v2"
MAX_DAYS_PER_REQ = 29   # rolling options API limit

# ─── NSE LOT SIZES ──────────────────────────────────────────────────────────────
LOT_SIZES = {
    "HDFCBANK":550,"RELIANCE":250,"INFY":300,"TCS":150,"ICICIBANK":700,
    "KOTAKBANK":400,"LT":300,"SBIN":1500,"BAJFINANCE":125,"HINDUNILVR":300,
    "ITC":3200,"AXISBANK":625,"ASIANPAINT":200,"MARUTI":100,"TITAN":375,
    "SUNPHARMA":700,"WIPRO":1500,"HCLTECH":700,"ULTRACEMCO":100,
    "BAJAJFINSV":500,"NESTLEIND":50,"POWERGRID":2700,"NTPC":3000,
    "TATAMOTORS":1425,"TATASTEEL":5500,"ADANIPORTS":625,"COALINDIA":4200,
    "HINDALCO":2150,"DRREDDY":125,"CIPLA":650,"DIVISLAB":200,"BRITANNIA":200,
    "EICHERMOT":175,"TECHM":600,"INDUSINDBK":525,
}

# ─── UNDERLYING SECURITY IDs (for equity OHLCV fetch) ─────────────────────────
EQUITY_SEC_IDS = {
    "HDFCBANK":"1333","RELIANCE":"2885","INFY":"10604","TCS":"11536",
    "ICICIBANK":"4963","KOTAKBANK":"1922","LT":"11483","SBIN":"3045",
    "BAJFINANCE":"317","HINDUNILVR":"1394","ITC":"1660","AXISBANK":"5900",
    "ASIANPAINT":"236","MARUTI":"10999","TITAN":"3506","SUNPHARMA":"3351",
    "WIPRO":"3787","HCLTECH":"10666","ULTRACEMCO":"11532","BAJAJFINSV":"16675",
    "NESTLEIND":"17963","POWERGRID":"14977","NTPC":"11630","TATAMOTORS":"3456",
    "TATASTEEL":"3459","ADANIPORTS":"15083","COALINDIA":"12070","HINDALCO":"1363",
    "DRREDDY":"881","CIPLA":"694","DIVISLAB":"10243","BRITANNIA":"547",
    "EICHERMOT":"910","TECHM":"13538","INDUSINDBK":"5258",
}

# ─── SECURITY IDs for rollingoption API (integer) ─────────────────────────────
OPT_SEC_IDS = {
    "HDFCBANK":13,"RELIANCE":2885,"INFY":10604,"TCS":11536,"ICICIBANK":4963,
    "KOTAKBANK":1922,"LT":11483,"SBIN":3045,"BAJFINANCE":317,"HINDUNILVR":1394,
    "ITC":1660,"AXISBANK":5900,"ASIANPAINT":236,"MARUTI":10999,"TITAN":3506,
    "SUNPHARMA":3351,"WIPRO":3787,"HCLTECH":10666,"ULTRACEMCO":11532,
    "BAJAJFINSV":16675,"NESTLEIND":17963,"POWERGRID":14977,"NTPC":11630,
    "TATAMOTORS":3456,"TATASTEEL":3459,"ADANIPORTS":15083,"COALINDIA":12070,
    "HINDALCO":1363,"DRREDDY":881,"CIPLA":694,"DIVISLAB":10243,"BRITANNIA":547,
    "EICHERMOT":910,"TECHM":13538,"INDUSINDBK":5258,
}

# Strike offsets for CE and PE
CE_OFFSETS = {"ATM":"ATM","OTM1":"ATM+1","OTM2":"ATM+2","ITM1":"ATM-1"}
PE_OFFSETS = {"ATM":"ATM","OTM1":"ATM-1","OTM2":"ATM-2","ITM1":"ATM+1"}

# ─── INDEX CONFIGURATION ───────────────────────────────────────────────────────
# Signals are computed on INDEX OHLCV (price data).
# Options are traded on NSE_FNO using OPTIDX instrument type.
#
# Dhan security IDs:
#   NIFTY 50   → equity feed: segment=IDX_I, secId="13"
#   BANKNIFTY  → equity feed: segment=IDX_I, secId="25"
#   SENSEX     → equity feed: segment=BSE_I, secId="1"  (BSE index)
#
# rollingoption API uses instrument="OPTIDX" for index options.
#
INDEX_CONFIG = {
    "NIFTY": {
        "name":            "NIFTY 50",
        "equity_sec_id":   "13",         # for price/OHLCV fetch
        "equity_segment":  "IDX_I",      # Dhan segment for NSE index
        "equity_instrument":"INDEX",
        "opt_sec_id":      13,            # integer, for rollingoption API
        "opt_instrument":  "OPTIDX",
        "lot_size":        75,            # NSE lot size (verify before trading)
        "strike_interval": 50,            # NIFTY strikes in multiples of 50
        "expiry_flag":     "WEEK",        # weekly expiry for NIFTY
    },
    "BANKNIFTY": {
        "name":            "BANK NIFTY",
        "equity_sec_id":   "25",
        "equity_segment":  "IDX_I",
        "equity_instrument":"INDEX",
        "opt_sec_id":      25,
        "opt_instrument":  "OPTIDX",
        "lot_size":        30,
        "strike_interval": 100,
        "expiry_flag":     "WEEK",        # weekly expiry
    },
    "SENSEX": {
        "name":            "SENSEX",
        "equity_sec_id":   "1",
        "equity_segment":  "BSE_I",       # BSE index feed
        "equity_instrument":"INDEX",
        "opt_sec_id":      1,
        "opt_instrument":  "OPTIDX",
        "lot_size":        10,
        "strike_interval": 200,
        "expiry_flag":     "WEEK",        # weekly expiry
    },
}

# Supported index names for CLI
INDEX_NAMES = list(INDEX_CONFIG.keys())

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(f"backtest_{date.today()}.log"),
    ]
)
log = logging.getLogger(__name__)


# =============================================================================
# HTTP SESSION
# =============================================================================

def make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "Content-Type":  "application/json",
        "Accept":        "application/json",
        "access-token":  DHAN_ACCESS_TOKEN,
        "client-id":     DHAN_CLIENT_ID,
    })
    return s

SESSION = make_session()


# =============================================================================
# DATA FETCHING
# =============================================================================

def fetch_equity_15m(security_id: str, from_date: str, to_date: str) -> pd.DataFrame:
    """Fetch 15-min equity OHLCV from Dhan, auto-chunked at 90 days."""
    fmt   = "%Y-%m-%d"
    start = datetime.strptime(from_date, fmt)
    end   = datetime.strptime(to_date, fmt)
    chunk = timedelta(days=89)
    frames = []
    cursor = start
    while cursor <= end:
        chunk_end = min(cursor + chunk, end)
        payload = {
            "securityId":      security_id,
            "exchangeSegment": "NSE_EQ",
            "instrument":      "EQUITY",
            "interval":        "15",
            "oi":              False,
            "fromDate":        cursor.strftime(fmt),
            "toDate":          chunk_end.strftime(fmt),
        }
        try:
            r = SESSION.post(f"{BASE_URL}/charts/intraday", json=payload, timeout=30)
            r.raise_for_status()
            data = r.json()
            df = _parse_columnar(data)
            if not df.empty:
                frames.append(df)
        except Exception as e:
            log.warning(f"Equity fetch error {security_id} {cursor.strftime(fmt)}: {e}")
        cursor = chunk_end + timedelta(days=1)
        time.sleep(0.35)
    if not frames:
        return pd.DataFrame()
    out = pd.concat(frames).sort_index()
    return out[~out.index.duplicated(keep="first")]


def fetch_index_15m(security_id: str, from_date: str, to_date: str,
                     segment: str = "IDX_I") -> pd.DataFrame:
    """
    Fetch 15-min OHLCV for an index (NIFTY, BANKNIFTY, SENSEX).
    Uses segment=IDX_I for NSE indices, BSE_I for SENSEX.
    instrument type = INDEX (not EQUITY).
    Auto-chunked at 90 days same as equities.
    """
    fmt   = "%Y-%m-%d"
    start = datetime.strptime(from_date, fmt)
    end   = datetime.strptime(to_date, fmt)
    chunk = timedelta(days=89)
    frames = []
    cursor = start
    while cursor <= end:
        chunk_end = min(cursor + chunk, end)
        payload = {
            "securityId":      security_id,
            "exchangeSegment": segment,
            "instrument":      "INDEX",
            "interval":        "15",
            "oi":              False,
            "fromDate":        cursor.strftime(fmt),
            "toDate":          chunk_end.strftime(fmt),
        }
        try:
            r = SESSION.post(f"{BASE_URL}/charts/intraday", json=payload, timeout=30)
            r.raise_for_status()
            data = r.json()
            df = _parse_columnar(data)
            if not df.empty:
                frames.append(df)
        except Exception as e:
            log.warning(f"Index fetch error {security_id} {cursor.strftime(fmt)}: {e}")
        cursor = chunk_end + timedelta(days=1)
        time.sleep(0.35)
    if not frames:
        return pd.DataFrame()
    out = pd.concat(frames).sort_index()
    return out[~out.index.duplicated(keep="first")]


def fetch_index_option_candles(
    security_id: int,
    option_type: str,
    strike_offset: str,
    from_date: str,
    to_date: str,
    expiry_flag: str = "WEEK",
    interval: str = "15",
) -> pd.DataFrame:
    """
    Fetch real historical option OHLCV for INDEX options (NIFTY/BANKNIFTY/SENSEX).
    Uses instrument="OPTIDX" instead of "OPTSTK".
    expiry_flag="WEEK" for weekly expiry (NIFTY/BANKNIFTY/SENSEX all have weekly).
    expiryCode=1 means nearest (front-week) expiry in the date range.
    Max 30 days per call — auto-chunked.
    """
    fmt   = "%Y-%m-%d"
    start = datetime.strptime(from_date, fmt)
    end   = datetime.strptime(to_date, fmt)
    chunk = timedelta(days=MAX_DAYS_PER_REQ)
    frames = []
    cursor = start
    side = "ce" if option_type == "CALL" else "pe"
    while cursor <= end:
        chunk_end = min(cursor + chunk, end)
        payload = {
            "exchangeSegment": "NSE_FNO",
            "interval":        interval,
            "securityId":      security_id,
            "instrument":      "OPTIDX",       # index options
            "expiryFlag":      expiry_flag,    # "WEEK" for weekly
            "expiryCode":      1,              # front expiry
            "strike":          strike_offset,
            "drvOptionType":   option_type,
            "requiredData":    ["open","high","low","close","volume","oi","iv","spot","strike"],
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
                for field in ["open","high","low","close","volume","oi","iv","spot","strike"]:
                    v = side_data.get(field, [])
                    if v and len(v) == len(timestamps):
                        cols[field] = v
                if cols.get("close"):
                    df = pd.DataFrame(
                        cols,
                        index=pd.to_datetime(timestamps, unit="s", utc=True)
                              .tz_convert("Asia/Kolkata")
                    )
                    df.index.name = "datetime"
                    for c in df.columns:
                        df[c] = pd.to_numeric(df[c], errors="coerce")
                    frames.append(df.dropna(subset=["close"]))
        except Exception as e:
            log.warning(f"Index option fetch error {option_type} {strike_offset} "
                        f"{cursor.strftime(fmt)}: {e}")
        cursor = chunk_end + timedelta(days=1)
        time.sleep(0.4)
    if not frames:
        return pd.DataFrame()
    out = pd.concat(frames).sort_index()
    return out[~out.index.duplicated(keep="first")]


def fetch_option_candles(
    security_id: int,
    option_type: str,      # "CALL" or "PUT"
    strike_offset: str,    # "ATM", "ATM+1", etc.
    from_date: str,
    to_date: str,
    interval: str = "15",
) -> pd.DataFrame:
    """Fetch real historical option OHLCV from Dhan rollingoption endpoint (STOCK options)."""
    fmt   = "%Y-%m-%d"
    start = datetime.strptime(from_date, fmt)
    end   = datetime.strptime(to_date, fmt)
    chunk = timedelta(days=MAX_DAYS_PER_REQ)
    frames = []
    cursor = start
    side = "ce" if option_type == "CALL" else "pe"
    while cursor <= end:
        chunk_end = min(cursor + chunk, end)
        payload = {
            "exchangeSegment": "NSE_FNO",
            "interval":        interval,
            "securityId":      security_id,
            "instrument":      "OPTSTK",
            "expiryFlag":      "MONTH",
            "expiryCode":      1,
            "strike":          strike_offset,
            "drvOptionType":   option_type,
            "requiredData":    ["open","high","low","close","volume","oi","iv","spot","strike"],
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
                for field in ["open","high","low","close","volume","oi","iv","spot","strike"]:
                    v = side_data.get(field, [])
                    if v and len(v) == len(timestamps):
                        cols[field] = v
                if cols.get("close"):
                    df = pd.DataFrame(
                        cols,
                        index=pd.to_datetime(timestamps, unit="s", utc=True)
                              .tz_convert("Asia/Kolkata")
                    )
                    df.index.name = "datetime"
                    for c in df.columns:
                        df[c] = pd.to_numeric(df[c], errors="coerce")
                    df = df.dropna(subset=["close"])
                    frames.append(df)
        except Exception as e:
            log.warning(f"Option fetch error {option_type} {strike_offset} "
                        f"{cursor.strftime(fmt)}: {e}")
        cursor = chunk_end + timedelta(days=1)
        time.sleep(0.4)
    if not frames:
        return pd.DataFrame()
    out = pd.concat(frames).sort_index()
    return out[~out.index.duplicated(keep="first")]


def _parse_columnar(data: dict) -> pd.DataFrame:
    if not data or "open" not in data:
        return pd.DataFrame()
    df = pd.DataFrame(
        {"open":data["open"],"high":data["high"],"low":data["low"],
         "close":data["close"],"volume":data["volume"]},
        index=pd.to_datetime(data["timestamp"], unit="s", utc=True)
              .tz_convert("Asia/Kolkata")
    )
    df.index.name = "datetime"
    return df.sort_index()


def resample_to_60m(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    return df.resample("60min").agg(
        {"open":"first","high":"max","low":"min","close":"last","volume":"sum"}
    ).dropna(subset=["close"])


# =============================================================================
# INDICATORS  (pure pandas/numpy, no external TA library)
# =============================================================================

def _ema(s, n): return s.ewm(span=n, adjust=False).mean()
def _sma(s, n): return s.rolling(n).mean()
def _rma(s, n): return s.ewm(alpha=1/n, adjust=False).mean()

def compute_indicators(df: pd.DataFrame) -> pd.DataFrame:
    c, h, l, v = df["close"], df["high"], df["low"], df["volume"]

    # Bollinger Bands
    basis = _sma(c, BB_LEN)
    sigma = c.rolling(BB_LEN).std(ddof=0)
    df["bb_upper"] = basis + BB_STD * sigma
    df["bb_lower"] = basis - BB_STD * sigma
    #df["bb_pct_b"] = (c - df["bb_lower"]) / (df["bb_upper"] - df["bb_lower"] + 1e-9)

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
    tr   = pd.concat([h-l, (h-c.shift()).abs(), (l-c.shift()).abs()], axis=1).max(axis=1)
    atr  = _rma(tr, ADX_LEN)
    up   = h - h.shift(1); dn = l.shift(1) - l
    pdm  = pd.Series(np.where((up>dn)&(up>0), up, 0.0), index=h.index)
    ndm  = pd.Series(np.where((dn>up)&(dn>0), dn, 0.0), index=h.index)
    df["plus_di"]  = 100 * _rma(pdm, ADX_LEN) / (atr + 1e-9)
    df["minus_di"] = 100 * _rma(ndm, ADX_LEN) / (atr + 1e-9)
    dx   = 100 * (df["plus_di"] - df["minus_di"]).abs() / (df["plus_di"] + df["minus_di"] + 1e-9)
    df["adx"] = _rma(dx, ADX_LEN)

    # Higher lows count
    def _hl_count(w):
        cnt = 0
        for i in range(1, len(w)):
            if w[i] > w[i-1]: cnt += 1
            else: cnt = 0
        return cnt
    df["higher_lows"] = l.rolling(10).apply(_hl_count, raw=True)
    def _lh_count(w):
        cnt = 0
        for i in range(1, len(w)):
            if w[i] < w[i-1]: cnt += 1
            else: cnt = 0
        return cnt
    df["lower_highs"] = h.rolling(10).apply(_lh_count, raw=True)

    # Cross helpers
    def _cross_up(a, b, n=1):
        return (a.rolling(n, min_periods=1).max() > b.rolling(n, min_periods=1).max()) & \
               (a.shift(n) <= b.shift(n))
    def _cross_dn(a, b, n=1):
        return (a.rolling(n, min_periods=1).min() < b.rolling(n, min_periods=1).min()) & \
               (a.shift(n) >= b.shift(n))

    df["ema5_bull_13"] = (df["ema5"] > df["ema13"]) & (df["ema5"].shift(1) <= df["ema13"].shift(1))
    df["ema5_bull_26"] = (df["ema5"] > df["ema26"]) & (df["ema5"].shift(1) <= df["ema26"].shift(1))
    df["ema5_bear_13"] = (df["ema5"] < df["ema13"]) & (df["ema5"].shift(1) >= df["ema13"].shift(1))
    df["ema5_bear_26"] = (df["ema5"] < df["ema26"]) & (df["ema5"].shift(1) >= df["ema26"].shift(1))

    df["rsi_cross60"] = (df["rsi"] > 60) & (df["rsi"].shift(1) <= 60)
    df["rsi_cross40"] = (df["rsi"] < 40) & (df["rsi"].shift(1) >= 40)

    df["di_bull"] = (df["plus_di"] > df["minus_di"]) & (df["plus_di"].shift(1) <= df["minus_di"].shift(1))
    df["di_bear"] = (df["minus_di"] > df["plus_di"]) & (df["minus_di"].shift(1) <= df["plus_di"].shift(1))

    return df


def _rany(s: pd.Series, n: int) -> pd.Series:
    return s.rolling(n, min_periods=1).max().astype(bool)


# =============================================================================
# SIGNAL GENERATION
# =============================================================================

def generate_signals(wave: pd.DataFrame, tide: pd.DataFrame) -> pd.DataFrame:
    """Apply all 10 buy/sell conditions. Returns wave with 'signal' column."""

    # ── Tide conditions (1H) ─────────────────────────────────────────────
    tide_buy = (
        ((tide["close"] >= tide["bb_upper"]*0.65) | (tide["bb_upper"] >= tide["bb_upper"].shift(1))) &
        (tide["macd"] > 0) & (tide["macd"] >= tide["macd"].shift(1))
    )
    tide_sell = (
        ((tide["close"] <= tide["bb_lower"]*1.35) | (tide["bb_lower"] <= tide["bb_lower"].shift(1))) &
        (tide["macd"] < 0) & (tide["macd"] <= tide["macd"].shift(1))
    )
    tide_rsi_buy  = tide["rsi"] > 50
    tide_rsi_sell = tide["rsi"] < 50

    def _ffill(s):
        return s.reindex(wave.index, method="ffill").fillna(False)

    wave["_tide_buy"]      = _ffill(tide_buy)
    wave["_tide_sell"]     = _ffill(tide_sell)
    wave["_tide_rsi_buy"]  = _ffill(tide_rsi_buy)
    wave["_tide_rsi_sell"] = _ffill(tide_rsi_sell)

    # ── Wave buy conditions (15M) ────────────────────────────────────────
    rsi60_recent  = _rany(wave["rsi_cross60"], 3)
    rsi40_recent  = _rany(wave["rsi_cross40"], 3)
    ema_bull = _rany(wave["ema5_bull_13"], 3) | _rany(wave["ema5_bull_26"], 3)
    ema_bear = _rany(wave["ema5_bear_13"], 3) | _rany(wave["ema5_bear_26"], 3)
    di_bull  = _rany(wave["di_bull"], 3)
    di_bear  = _rany(wave["di_bear"], 3)
    adx_ok   = (wave["adx"] > 15) & (wave["adx"] > wave["adx"].shift(1))

    c3_buy  = wave["_tide_rsi_buy"]  & (wave["rsi"] > 60) & wave["rsi_cross60"] & _rany(wave["rsi_cross60"],3)
    c3_sell = wave["_tide_rsi_sell"] & (wave["rsi"] < 40) & wave["rsi_cross40"] & _rany(wave["rsi_cross40"],3)

    # Relax C3 slightly: tide RSI + wave RSI crossed and currently in zone
    c3_buy  = wave["_tide_rsi_buy"]  & rsi60_recent & (wave["rsi"] > 60)
    c3_sell = wave["_tide_rsi_sell"] & rsi40_recent & (wave["rsi"] < 40)

    wave_buy = (
        (wave["close"] >= wave["bb_upper"]*0.85) &
        (wave["volume"] >= wave["vol_ma"]) &
        (wave["higher_lows"] >= 2) &
        ema_bull & di_bull & adx_ok &
        (wave["close"] > wave["ema50"])
    )
    wave_sell = (
        (wave["close"] <= wave["bb_lower"]*1.25) &
        (wave["volume"] >= wave["vol_ma"]) &
        (wave["lower_highs"] >= 2) &
        ema_bear & di_bear & adx_ok &
        (wave["close"] < wave["ema50"])
    )

    wave["signal"] = 0
    wave.loc[wave["_tide_buy"]  & c3_buy  & wave_buy,  "signal"] = 1
    wave.loc[wave["_tide_sell"] & c3_sell & wave_sell, "signal"] = -1

    return wave


# =============================================================================
# POSITION SIZING
# =============================================================================

def calc_lots(capital, premium, lot_size, risk_pct=RISK_PCT_PER_TRADE,
              sl_pct=OPTION_SL_PCT):
    risk_amt       = capital * risk_pct / 100
    loss_per_share = premium * sl_pct / 100
    loss_per_lot   = loss_per_share * lot_size
    if loss_per_lot <= 0:
        return 0, 0, 0
    lots = max(1, math.floor(risk_amt / loss_per_lot))
    half = max(1, lots // 2)
    trail = lots - half
    if trail < 1:
        trail = 1; half = max(0, lots - 1)
    return lots, half, trail


# =============================================================================
# TRADE DATACLASS
# =============================================================================

@dataclass
class Trade:
    symbol:        str
    option_type:   str
    signal:        int
    strike_mode:   str
    entry_time:    pd.Timestamp
    entry_premium: float
    lot_size:      int
    total_lots:    int
    half_lots:     int
    trail_lots:    int
    sl_premium:    float
    trail_premium: float
    peak_premium:  float
    underlying_entry: float

    exit_time:     Optional[pd.Timestamp] = None
    exit_premium:  float = 0.0
    exit_reason:   str = ""
    strike_price:  float = 0.0   # actual strike price of the option contract

    half_done:     bool  = False
    half_pnl:      float = 0.0
    trail_pnl:     float = 0.0
    net_pnl:       float = 0.0
    bars_held:     int   = 0


# =============================================================================
# KILL SWITCH
# =============================================================================

class KillSwitch:
    def __init__(self, capital):
        self.threshold  = capital * KILL_SWITCH_PCT / 100
        self.daily_loss = 0.0
        self.triggered  = False

    def record(self, pnl):
        if pnl < 0:
            self.daily_loss += abs(pnl)
            if self.daily_loss >= self.threshold and not self.triggered:
                self.triggered = True
                log.critical(
                    f"\n{'='*55}\n"
                    f"  KILL SWITCH — daily loss ₹{self.daily_loss:,.0f} "
                    f"exceeded {KILL_SWITCH_PCT}% threshold\n"
                    f"  ALL TRADING HALTED\n{'='*55}"
                )

    def is_on(self): return self.triggered
    def reset(self): self.daily_loss = 0.0; self.triggered = False


# =============================================================================
# MAIN BACKTEST ENGINE
# =============================================================================

def get_option_price(opt_df: pd.DataFrame, ts: pd.Timestamp) -> Optional[float]:
    """Get closest option close price at or before ts."""
    if opt_df.empty:
        return None
    avail = opt_df[opt_df.index <= ts]
    if avail.empty:
        return None
    return float(avail["close"].iloc[-1])


def get_option_strike(opt_df: pd.DataFrame, ts: pd.Timestamp) -> float:
    """
    Get the actual strike price of the option contract at entry time.
    The rollingoption API returns a 'strike' column with the real strike value
    (e.g. 24950, 45000, 1250 etc.) for each bar.
    Falls back to 0.0 if strike column is not present or no data available.
    """
    if opt_df.empty or "strike" not in opt_df.columns:
        return 0.0
    avail = opt_df[opt_df.index <= ts]
    if avail.empty:
        return 0.0
    val = avail["strike"].iloc[-1]
    return float(val) if pd.notna(val) else 0.0


def slip(p, side):
    """Apply slippage: buys pay more, sells receive less."""
    mult = (1 + SLIPPAGE_PCT/100) if side == "buy" else (1 - SLIPPAGE_PCT/100)
    return round(p * mult, 2)


def run_symbol(symbol: str, from_date: str, to_date: str,
               strike_mode: str, capital: float,
               kill: KillSwitch) -> list[Trade]:
    """Full pipeline for one symbol. Returns list of completed trades."""

    eq_sid  = EQUITY_SEC_IDS.get(symbol)
    opt_sid = OPT_SEC_IDS.get(symbol)
    lot_sz  = LOT_SIZES.get(symbol, 500)

    if not eq_sid or not opt_sid:
        log.warning(f"{symbol}: missing security IDs — skipping")
        return []

    # ── Step 1: Underlying OHLCV ─────────────────────────────────────────
    log.info(f"{symbol}: fetching underlying 15M data...")
    wave_raw = fetch_equity_15m(eq_sid, from_date, to_date)
    if wave_raw.empty or len(wave_raw) < 100:
        log.warning(f"{symbol}: insufficient underlying data"); return []

    tide_raw = wave_raw.resample("60min").agg(
        {"open":"first","high":"max","low":"min","close":"last","volume":"sum"}
    ).dropna(subset=["close"])

    wave = compute_indicators(wave_raw.copy())
    tide = compute_indicators(tide_raw.copy())
    wave = generate_signals(wave, tide)

    n_signals = (wave["signal"] != 0).sum()
    log.info(f"{symbol}: {len(wave)} bars | {n_signals} signals")
    if n_signals == 0:
        return []

    # ── Step 2: Option premium data ──────────────────────────────────────
    ce_off = CE_OFFSETS.get(strike_mode, "ATM")
    pe_off = PE_OFFSETS.get(strike_mode, "ATM")

    log.info(f"{symbol}: fetching CE ({ce_off})...")
    ce_df = fetch_option_candles(opt_sid, "CALL", ce_off, from_date, to_date)
    time.sleep(0.5)
    log.info(f"{symbol}: fetching PE ({pe_off})...")
    pe_df = fetch_option_candles(opt_sid, "PUT",  pe_off, from_date, to_date)

    if ce_df.empty and pe_df.empty:
        log.warning(f"{symbol}: no option data — skipping"); return []

    log.info(f"{symbol}: CE={len(ce_df)} bars | PE={len(pe_df)} bars")

    # ── Step 3: Bar-by-bar simulation ────────────────────────────────────
    trades:    list[Trade]  = []
    open_trade: Optional[Trade] = None
    equity = capital

    bars = list(wave.iterrows())

    for i, (ts, bar) in enumerate(bars):
        if kill.is_on():
            if open_trade:
                opt_df = ce_df if open_trade.option_type == "CE" else pe_df
                p = get_option_price(opt_df, ts) or open_trade.entry_premium
                _close(open_trade, ts, slip(p,"sell"), "kill_switch", lot_sz, trades, kill)
                equity += open_trade.net_pnl
                open_trade = None
            break

        signal   = int(bar.get("signal", 0))
        close    = bar["close"]
        ema5_val = bar.get("ema5", close)
        prev_open = bars[i-1][1]["open"] if i > 0 else close

        # ── Manage open trade ─────────────────────────────────────────────
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

                # Half-exit
                if not open_trade.half_done and open_trade.half_lots > 0:
                    move_pct = abs(close - open_trade.underlying_entry) / open_trade.underlying_entry * 100
                    if move_pct >= HALF_EXIT_TRIGGER_PCT:
                        exit_p = slip(prem, "sell")
                        open_trade.half_pnl    = (exit_p - open_trade.entry_premium) * open_trade.half_lots * lot_sz
                        open_trade.sl_premium  = open_trade.entry_premium   # move SL to breakeven
                        open_trade.trail_premium = max(open_trade.trail_premium, open_trade.entry_premium)
                        open_trade.half_done   = True
                        log.debug(f"{symbol} HALF-EXIT @ ₹{exit_p:.2f} pnl=₹{open_trade.half_pnl:+.0f}")

                # EOD
                if ts.hour == EOD_HOUR and ts.minute >= EOD_MINUTE:
                    _close(open_trade, ts, slip(prem,"sell"), "eod", lot_sz, trades, kill)
                    equity += open_trade.net_pnl
                    open_trade = None; continue

                # Price exits on underlying
                reason = None
                if open_trade.signal == 1:
                    if close < ema5_val:   reason = "below_ema5"
                    elif close < prev_open: reason = "below_prev_open"
                else:
                    if close > ema5_val:   reason = "above_ema5"
                    elif close > prev_open: reason = "above_prev_open"

                if reason:
                    _close(open_trade, ts, slip(prem,"sell"), reason, lot_sz, trades, kill)
                    equity += open_trade.net_pnl
                    open_trade = None; continue

                # Premium stops
                if prem <= open_trade.sl_premium:
                    exit_p = max(0.05, open_trade.sl_premium)
                    _close(open_trade, ts, slip(exit_p,"sell"), "stop_loss", lot_sz, trades, kill)
                    equity += open_trade.net_pnl
                    open_trade = None; continue

                trail_active = open_trade.trail_premium > open_trade.entry_premium
                if trail_active and prem <= open_trade.trail_premium:
                    _close(open_trade, ts, slip(open_trade.trail_premium,"sell"),
                           "trail_stop", lot_sz, trades, kill)
                    equity += open_trade.net_pnl
                    open_trade = None; continue

        # ── New entry ─────────────────────────────────────────────────────
        if signal != 0 and open_trade is None:
            opt_type = "CE" if signal == 1 else "PE"
            opt_df   = ce_df if opt_type == "CE" else pe_df
            prem     = get_option_price(opt_df, ts)

            if prem and prem > 0:
                entry_p = slip(prem, "buy")
                lots, half, trail = calc_lots(equity, entry_p, lot_sz)
                cost = entry_p * lot_sz * lots

                if lots > 0 and cost <= equity:
                    sl_p = entry_p * (1 - OPTION_SL_PCT/100)
                    open_trade = Trade(
                        symbol=symbol, option_type=opt_type, signal=signal,
                        strike_mode=strike_mode, entry_time=ts,
                        entry_premium=entry_p, lot_size=lot_sz,
                        total_lots=lots, half_lots=half, trail_lots=trail,
                        sl_premium=round(sl_p, 2),
                        trail_premium=round(sl_p, 2),
                        peak_premium=entry_p,
                        underlying_entry=close,
                        strike_price=get_option_strike(opt_df, ts),
                    )
                    equity -= cost
                    log.debug(f"{symbol} {opt_type} OPEN @ ₹{entry_p:.2f} "
                              f"strike={open_trade.strike_price:.0f} "
                              f"lots={lots} underlying=₹{close:.2f}")

    # End-of-backtest close
    if open_trade:
        opt_df = ce_df if open_trade.option_type == "CE" else pe_df
        last_ts = bars[-1][0] if bars else pd.Timestamp.now()
        p = get_option_price(opt_df, last_ts) or open_trade.entry_premium
        _close(open_trade, last_ts, slip(p,"sell"), "end_of_backtest",
               lot_sz, trades, kill)

    return trades


def _close(trade: Trade, ts, exit_p, reason, lot_sz, trades_list, kill):
    """
    Finalise a trade: compute PnL, record it, notify kill switch.

    PnL calculation:
      - If half-exit was done: trail_lots remain, half_pnl already captured.
      - rem_qty = trail_lots * lot_sz  (or total_lots if no half-exit yet)
      - trail_pnl = (exit_premium - entry_premium) * rem_qty
      - gross = half_pnl + trail_pnl
      - brokerage = Rs20/lot * total_lots * 2  (entry + exit)
      - net = gross - brokerage
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

    kill.record(net)
    trades_list.append(trade)
    log.info(f"{trade.symbol} {trade.option_type} CLOSE @ Rs{exit_p:.2f} | "
             f"reason={reason} | net_pnl=Rs{net:+,.0f}")


def run_index(
    index_name: str,     # "NIFTY", "BANKNIFTY", or "SENSEX"
    from_date:  str,
    to_date:    str,
    strike_mode: str,
    capital:    float,
    kill:       KillSwitch,
) -> list:
    """
    Full pipeline for ONE index (NIFTY / BANKNIFTY / SENSEX).

    Differences vs run_symbol():
      • Price data fetched via fetch_index_15m() using IDX_I / BSE_I segment
      • Option data fetched via fetch_index_option_candles() using OPTIDX instrument
      • expiry_flag = WEEK (weekly expiry for all three indices)
      • No volume column in index OHLCV — volume condition (C5) is relaxed to True
        so 9 remaining conditions still apply
    """
    if index_name not in INDEX_CONFIG:
        log.error(f"Unknown index '{index_name}'. Supported: {INDEX_NAMES}")
        return []

    cfg      = INDEX_CONFIG[index_name]
    lot_sz   = cfg["lot_size"]
    ce_off   = CE_OFFSETS.get(strike_mode, "ATM")
    pe_off   = PE_OFFSETS.get(strike_mode, "ATM")

    # ── Step 1: Index OHLCV ──────────────────────────────────────────────
    log.info(f"{index_name}: fetching 15M index price data...")
    wave_raw = fetch_index_15m(
        cfg["equity_sec_id"], from_date, to_date, segment=cfg["equity_segment"]
    )
    if wave_raw.empty or len(wave_raw) < 100:
        log.warning(f"{index_name}: insufficient price data"); return []

    # Indices have no volume in feed — fill with synthetic volume = 1
    # so volume-based indicators don't break. C5 is then always True.
    if "volume" not in wave_raw.columns or wave_raw["volume"].sum() == 0:
        wave_raw["volume"] = 1_000_000   # large constant → vol always > vol_ma

    tide_raw = wave_raw.resample("60min").agg(
        {"open":"first","high":"max","low":"min","close":"last","volume":"sum"}
    ).dropna(subset=["close"])

    wave = compute_indicators(wave_raw.copy())
    tide = compute_indicators(tide_raw.copy())
    wave = generate_signals(wave, tide)

    n_signals = (wave["signal"] != 0).sum()
    log.info(f"{index_name}: {len(wave)} bars | {n_signals} signals")
    if n_signals == 0:
        return []

    # ── Step 2: Index option premium data ────────────────────────────────
    log.info(f"{index_name}: fetching CE ({ce_off}) index options...")
    ce_df = fetch_index_option_candles(
        cfg["opt_sec_id"], "CALL", ce_off, from_date, to_date,
        expiry_flag=cfg["expiry_flag"]
    )
    time.sleep(0.5)
    log.info(f"{index_name}: fetching PE ({pe_off}) index options...")
    pe_df = fetch_index_option_candles(
        cfg["opt_sec_id"], "PUT", pe_off, from_date, to_date,
        expiry_flag=cfg["expiry_flag"]
    )

    if ce_df.empty and pe_df.empty:
        log.warning(f"{index_name}: no option data — skipping"); return []
    log.info(f"{index_name}: CE={len(ce_df)} bars | PE={len(pe_df)} bars")

    # ── Step 3: Bar-by-bar simulation (same engine as run_symbol) ────────
    trades:     list        = []
    open_trade: Optional[Trade] = None
    equity = capital
    bars   = list(wave.iterrows())

    for i, (ts, bar) in enumerate(bars):
        if kill.is_on():
            if open_trade:
                opt_df = ce_df if open_trade.option_type == "CE" else pe_df
                p = get_option_price(opt_df, ts) or open_trade.entry_premium
                _close(open_trade, ts, slip(p,"sell"), "kill_switch",
                       lot_sz, trades, kill)
                equity += open_trade.net_pnl
                open_trade = None
            break

        signal    = int(bar.get("signal", 0))
        close     = bar["close"]
        ema5_val  = bar.get("ema5", close)
        prev_open = bars[i-1][1]["open"] if i > 0 else close

        # ── Manage open trade ──────────────────────────────────────────────
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

                # Half-exit
                if not open_trade.half_done and open_trade.half_lots > 0:
                    move_pct = abs(close - open_trade.underlying_entry) / \
                               open_trade.underlying_entry * 100
                    if move_pct >= HALF_EXIT_TRIGGER_PCT:
                        exit_p = slip(prem, "sell")
                        open_trade.half_pnl   = (exit_p - open_trade.entry_premium) * \
                                                 open_trade.half_lots * lot_sz
                        open_trade.sl_premium  = open_trade.entry_premium
                        open_trade.trail_premium = max(open_trade.trail_premium,
                                                       open_trade.entry_premium)
                        open_trade.half_done   = True
                        log.debug(f"{index_name} HALF-EXIT @ ₹{exit_p:.2f} "
                                  f"pnl=₹{open_trade.half_pnl:+.0f}")

                # EOD
                if ts.hour == EOD_HOUR and ts.minute >= EOD_MINUTE:
                    _close(open_trade, ts, slip(prem,"sell"), "eod",
                           lot_sz, trades, kill)
                    equity += open_trade.net_pnl
                    open_trade = None; continue

                # Price exits on index price
                reason = None
                if open_trade.signal == 1:
                    if close < ema5_val:    reason = "below_ema5"
                    elif close < prev_open: reason = "below_prev_open"
                else:
                    if close > ema5_val:    reason = "above_ema5"
                    elif close > prev_open: reason = "above_prev_open"

                if reason:
                    _close(open_trade, ts, slip(prem,"sell"), reason,
                           lot_sz, trades, kill)
                    equity += open_trade.net_pnl
                    open_trade = None; continue

                # Premium stops
                if prem <= open_trade.sl_premium:
                    exit_p = max(0.05, open_trade.sl_premium)
                    _close(open_trade, ts, slip(exit_p,"sell"), "stop_loss",
                           lot_sz, trades, kill)
                    equity += open_trade.net_pnl
                    open_trade = None; continue

                trail_active = open_trade.trail_premium > open_trade.entry_premium
                if trail_active and prem <= open_trade.trail_premium:
                    _close(open_trade, ts, slip(open_trade.trail_premium,"sell"),
                           "trail_stop", lot_sz, trades, kill)
                    equity += open_trade.net_pnl
                    open_trade = None; continue

        # ── New entry ──────────────────────────────────────────────────────
        if signal != 0 and open_trade is None:
            opt_type = "CE" if signal == 1 else "PE"
            opt_df   = ce_df if opt_type == "CE" else pe_df
            prem     = get_option_price(opt_df, ts)

            if prem and prem > 0:
                entry_p = slip(prem, "buy")
                lots, half, trail = calc_lots(equity, entry_p, lot_sz)
                cost = entry_p * lot_sz * lots
                if lots > 0 and cost <= equity:
                    sl_p = entry_p * (1 - OPTION_SL_PCT/100)
                    open_trade = Trade(
                        symbol=index_name, option_type=opt_type, signal=signal,
                        strike_mode=strike_mode, entry_time=ts,
                        entry_premium=entry_p, lot_size=lot_sz,
                        total_lots=lots, half_lots=half, trail_lots=trail,
                        sl_premium=round(sl_p, 2),
                        trail_premium=round(sl_p, 2),
                        peak_premium=entry_p,
                        underlying_entry=close,
                        strike_price=get_option_strike(opt_df, ts),
                    )
                    equity -= cost
                    log.debug(f"{index_name} {opt_type} OPEN @ ₹{entry_p:.2f} "
                              f"strike={open_trade.strike_price:.0f} "
                              f"lots={lots} index=₹{close:.2f}")

        # Equity snapshot
        open_pnl = 0.0
        if open_trade:
            opt_df = ce_df if open_trade.option_type == "CE" else pe_df
            p = get_option_price(opt_df, ts)
            if p:
                open_pnl = (p - open_trade.entry_premium) * \
                           open_trade.trail_lots * lot_sz

    # End-of-backtest close
    if open_trade:
        opt_df = ce_df if open_trade.option_type == "CE" else pe_df
        last_ts = bars[-1][0] if bars else pd.Timestamp.now()
        p = get_option_price(opt_df, last_ts) or open_trade.entry_premium
        _close(open_trade, last_ts, slip(p,"sell"), "end_of_backtest",
               lot_sz, trades, kill)

    return trades


# =============================================================================
# REPORTING
# =============================================================================

def build_report(all_trades: list[Trade], capital: float) -> dict:
    if not all_trades:
        return {"error": "No trades executed"}

    rows = []
    for t in all_trades:
        rows.append({
            "symbol":          t.symbol,
            "option_type":     t.option_type,
            "strike_price":    t.strike_price,        # actual strike (e.g. 24950 CE)
            "strike_mode":     t.strike_mode,
            "signal":          t.signal,
            "entry_time":      t.entry_time,
            "exit_time":       t.exit_time,
            "underlying_entry":t.underlying_entry,
            "entry_premium":   t.entry_premium,
            "exit_premium":    t.exit_premium,
            "total_lots":      t.total_lots,
            "lot_size":        t.lot_size,
            "half_pnl":        t.half_pnl,
            "trail_pnl":       t.trail_pnl,
            "net_pnl":         t.net_pnl,
            "exit_reason":     t.exit_reason,
            "bars_held":       t.bars_held,
        })
    df = pd.DataFrame(rows)

    wins   = df[df["net_pnl"] > 0]
    losses = df[df["net_pnl"] <= 0]
    total  = len(df)
    wr     = len(wins)/total*100 if total else 0
    avg_w  = wins["net_pnl"].mean()   if len(wins)   else 0
    avg_l  = losses["net_pnl"].mean() if len(losses) else 0
    rr     = abs(avg_w/avg_l) if avg_l else float("inf")
    pf     = wins["net_pnl"].sum()/abs(losses["net_pnl"].sum()) if losses["net_pnl"].sum() else float("inf")

    net_total  = df["net_pnl"].sum()
    final_eq   = capital + net_total
    total_ret  = (final_eq - capital) / capital * 100

    ce_df = df[df["option_type"]=="CE"]; pe_df = df[df["option_type"]=="PE"]

    stock_summary = df.groupby("symbol").agg(
        trades=("net_pnl","count"),
        total_pnl=("net_pnl","sum"),
        win_rate=("net_pnl", lambda x:(x>0).mean()*100),
        avg_pnl=("net_pnl","mean"),
    ).sort_values("total_pnl", ascending=False)

    df["month"] = pd.to_datetime(df["exit_time"]).dt.to_period("M")
    df["year"]  = pd.to_datetime(df["exit_time"]).dt.year
    monthly = df.groupby("month")["net_pnl"].sum()
    yearly  = df.groupby("year")["net_pnl"].sum()

    return {
        "total_trades":    total,
        "num_wins":        len(wins),
        "num_losses":      len(losses),
        "win_rate":        round(wr, 2),
        "avg_win":         round(avg_w, 2),
        "avg_loss":        round(avg_l, 2),
        "rr_ratio":        round(rr, 2),
        "profit_factor":   round(pf, 2),
        "total_return_pct":round(total_ret, 2),
        "final_equity":    round(final_eq, 2),
        "initial_capital": capital,
        "gross_pnl":       round(df["net_pnl"].sum() + df.get("half_pnl",pd.Series([0])).sum(), 2),
        "net_pnl":         round(net_total, 2),
        "total_brokerage": round(BROKERAGE_PER_LOT * df["total_lots"].sum() * 2, 2),
        "ce_trades":       len(ce_df),
        "pe_trades":       len(pe_df),
        "ce_win_rate":     round((ce_df["net_pnl"]>0).mean()*100,1) if len(ce_df) else 0,
        "pe_win_rate":     round((pe_df["net_pnl"]>0).mean()*100,1) if len(pe_df) else 0,
        "exit_reasons":    df["exit_reason"].value_counts().to_dict(),
        "stock_summary":   stock_summary,
        "monthly_pnl":     monthly,
        "yearly_pnl":      yearly,
        "trades_df":       df,
    }


def print_report(r: dict):
    if "error" in r:
        print(f"\nError: {r['error']}"); return

    df = r["trades_df"]

    print(f"\n{'='*65}")
    print(f"  OPTIONS BACKTEST RESULTS  (Real Premium Data via Dhan)")
    print(f"{'='*65}")
    print(f"  Capital        : ₹{r['initial_capital']:>12,.0f}")
    print(f"  Final equity   : ₹{r['final_equity']:>12,.2f}")
    print(f"  Total return   : {r['total_return_pct']:>+.2f}%")
    print(f"  Net PnL        : ₹{r['net_pnl']:>12,.0f}")
    print(f"  Brokerage      : ₹{r['total_brokerage']:>12,.0f}")
    print(f"")
    print(f"  Total trades   : {r['total_trades']}")
    print(f"  Wins / Losses  : {r['num_wins']} / {r['num_losses']}")
    print(f"  Win rate       : {r['win_rate']:.1f}%")
    print(f"  Avg win        : ₹{r['avg_win']:>10,.0f}")
    print(f"  Avg loss       : ₹{r['avg_loss']:>10,.0f}")
    print(f"  R:R ratio      : 1:{r['rr_ratio']:.2f}")
    print(f"  Profit factor  : {r['profit_factor']:.2f}")
    print(f"")
    print(f"  CE trades: {r['ce_trades']}  WR={r['ce_win_rate']:.1f}%  |  "
          f"PE trades: {r['pe_trades']}  WR={r['pe_win_rate']:.1f}%")
    print(f"")
    print(f"  Exit reasons:")
    for reason, cnt in r["exit_reasons"].items():
        print(f"    {reason:<25}: {cnt:>4}")
    print(f"")

    # ── Strike price breakdown ───────────────────────────────────────────
    print(f"  Strike price breakdown:")
    if "strike_price" in df.columns and df["strike_price"].sum() > 0:
        # Most frequently traded strikes
        strike_counts = (df.groupby(["option_type","strike_price"])
                           .agg(trades=("net_pnl","count"),
                                total_pnl=("net_pnl","sum"),
                                win_rate=("net_pnl", lambda x:(x>0).mean()*100))
                           .sort_values("trades", ascending=False))
        for (opt, strike), row in strike_counts.head(10).iterrows():
            pnl_sign = "+" if row["total_pnl"] >= 0 else ""
            print(f"    {opt} {strike:>8.0f}  "
                  f"trades={int(row['trades']):>4}  "
                  f"wr={row['win_rate']:>5.1f}%  "
                  f"PnL=₹{pnl_sign}{row['total_pnl']:>10,.0f}")
    else:
        print(f"    (strike data not available — API may not have returned strike column)")
    print(f"")

    # ── Recent trades with strikes ───────────────────────────────────────
    print(f"  Last 10 trades (with strike prices):")
    cols_show = ["symbol","option_type","strike_price","entry_time","exit_time",
                 "underlying_entry","entry_premium","exit_premium","net_pnl","exit_reason"]
    cols_avail = [c for c in cols_show if c in df.columns]
    recent = df[cols_avail].tail(10)
    for _, row in recent.iterrows():
        strike_str = f"{row.get('strike_price',0):.0f}" if row.get('strike_price',0) > 0 else "N/A"
        entry_t = str(row["entry_time"])[:16] if pd.notna(row["entry_time"]) else "N/A"
        exit_t  = str(row["exit_time"])[:16]  if pd.notna(row.get("exit_time")) else "N/A"
        pnl     = row.get("net_pnl", 0)
        print(f"    {row['symbol']:<12} {row['option_type']} "
              f"strike={strike_str:>7}  "
              f"underlying=₹{row.get('underlying_entry',0):>8.2f}  "
              f"entry=₹{row.get('entry_premium',0):>7.2f}  "
              f"exit=₹{row.get('exit_premium',0):>7.2f}  "
              f"PnL=₹{pnl:>+9,.0f}  "
              f"[{row.get('exit_reason','?')}]")
    print(f"")

    # ── Top stocks/indices ───────────────────────────────────────────────
    print(f"  Top symbols:")
    for sym, row in r["stock_summary"].head(5).iterrows():
        print(f"    {sym:<14} trades={int(row['trades']):>4}  "
              f"wr={row['win_rate']:>5.1f}%  PnL=₹{row['total_pnl']:>10,.0f}")
    print(f"")
    print(f"  Yearly PnL:")
    for yr, pnl in r["yearly_pnl"].items():
        bar = ("+" if pnl >= 0 else "-") * min(40, int(abs(pnl)/10000))
        print(f"    {yr}: ₹{pnl:>10,.0f}  {bar}")
    print(f"{'='*65}")

    # Save CSV outputs — strike_price is now included automatically
    r["trades_df"].to_csv("options_bt_trades.csv", index=False)
    pd.DataFrame([(str(k),v) for k,v in r["monthly_pnl"].items()],
                 columns=["month","pnl"]).to_csv("options_bt_monthly.csv", index=False)

    # Save separate strike summary CSV
    if "strike_price" in df.columns and df["strike_price"].sum() > 0:
        (df.groupby(["symbol","option_type","strike_price"])
           .agg(trades=("net_pnl","count"),
                total_pnl=("net_pnl","sum"),
                win_rate=("net_pnl", lambda x:(x>0).mean()*100),
                avg_entry_prem=("entry_premium","mean"),
                avg_exit_prem=("exit_premium","mean"))
           .reset_index()
           .sort_values("total_pnl", ascending=False)
           .to_csv("options_bt_strikes.csv", index=False))
        print(f"\n  Saved: options_bt_trades.csv | options_bt_monthly.csv | options_bt_strikes.csv")
    else:
        print(f"\n  Saved: options_bt_trades.csv | options_bt_monthly.csv")
    print(f"{'='*65}\n")


# =============================================================================
# ENTRY POINT
# =============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Options Backtest — Stocks + Indices (NIFTY/BANKNIFTY/SENSEX)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
EXAMPLES — EQUITY STOCKS:
  python run_options_backtest.py --from 2022-01-01 --to 2024-12-31 --strike ATM
  python run_options_backtest.py --stocks RELIANCE HDFCBANK TCS --strike OTM1
  python run_options_backtest.py --capital 1000000 --risk 1.0 --strike ATM

EXAMPLES — INDEX OPTIONS:
  python run_options_backtest.py --mode index --index NIFTY --from 2022-01-01 --to 2024-12-31
  python run_options_backtest.py --mode index --index BANKNIFTY --strike OTM1
  python run_options_backtest.py --mode index --index SENSEX --strike ATM
  python run_options_backtest.py --mode index --index NIFTY BANKNIFTY SENSEX

EXAMPLES — BOTH stocks and indices:
  python run_options_backtest.py --mode both --stocks RELIANCE TCS --index NIFTY
        """
    )
    parser.add_argument("--mode",    default="stocks",
                        choices=["stocks","index","both"],
                        help="stocks = equity only | index = NIFTY/BANKNIFTY/SENSEX | "
                             "both = stocks + indices")
    parser.add_argument("--stocks",  nargs="+",
                        help="Stock symbols (default: all supported). Only used in --mode stocks/both.")
    parser.add_argument("--index",   nargs="+",
                        choices=INDEX_NAMES, default=["NIFTY"],
                        metavar="INDEX",
                        help=f"Index name(s): {INDEX_NAMES}. Only used in --mode index/both.")
    parser.add_argument("--from",    dest="from_date", default="2022-01-01",
                        help="Start date YYYY-MM-DD")
    parser.add_argument("--to",      dest="to_date",   default="2024-12-31",
                        help="End date YYYY-MM-DD")
    parser.add_argument("--strike",  default="ATM",
                        choices=["ATM","OTM1","OTM2"],
                        help="Strike selection (default: ATM)")
    parser.add_argument("--capital", type=float, default=500_000,
                        help="Starting capital in ₹ (default: 500000)")
    parser.add_argument("--risk",    type=float, default=1.5,
                        help="Risk %% per trade (default: 1.5)")
    parser.add_argument("--verbose", action="store_true",
                        help="Show debug-level logs")
    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    # Validate credentials
    if DHAN_CLIENT_ID == "YOUR_CLIENT_ID":
        print("\nERROR: Set your Dhan credentials first!")
        print("  Option A: Edit DHAN_CLIENT_ID and DHAN_ACCESS_TOKEN at the top of this file")
        print("  Option B: Set environment variables:")
        print("    set DHAN_CLIENT_ID=your_id")
        print("    set DHAN_ACCESS_TOKEN=your_token")
        sys.exit(1)

    # Override global risk pct
    global RISK_PCT_PER_TRADE
    RISK_PCT_PER_TRADE = args.risk

    kill       = KillSwitch(args.capital)
    all_trades = []

    # ── STOCK symbols ────────────────────────────────────────────────────────
    if args.mode in ("stocks", "both"):
        supported = list(OPT_SEC_IDS.keys())
        if args.stocks:
            invalid = [s for s in args.stocks if s not in OPT_SEC_IDS]
            if invalid:
                print(f"\nWARNING: Not in rollingoption API: {invalid}")
            symbols = [s for s in args.stocks if s in OPT_SEC_IDS]
        else:
            symbols = supported

        if not symbols:
            print("No valid stock symbols."); sys.exit(1)

        print(f"\n{'='*60}")
        print(f"  STOCK OPTIONS BACKTEST  —  Dhan API")
        print(f"  Symbols : {len(symbols)}")
        print(f"  Period  : {args.from_date} → {args.to_date}")
        print(f"  Strike  : {args.strike}")
        print(f"  Capital : ₹{args.capital:,.0f}")
        print(f"  Risk/trade: {args.risk}%  |  SL: {OPTION_SL_PCT}%  |  Trail: {int(TRAIL_PCT*100)}%")
        print(f"  Kill switch: {KILL_SWITCH_PCT}% daily loss")
        print(f"{'='*60}\n")

        for i, symbol in enumerate(symbols, 1):
            print(f"[{i:02d}/{len(symbols)}] {symbol} ...", flush=True)
            kill.reset()
            trades = run_symbol(
                symbol=symbol, from_date=args.from_date, to_date=args.to_date,
                strike_mode=args.strike, capital=args.capital, kill=kill,
            )
            all_trades.extend(trades)
            print(f"         {len(trades)} trades completed")
            time.sleep(1.0)

    # ── INDEX symbols ────────────────────────────────────────────────────────
    if args.mode in ("index", "both"):
        indices = args.index or ["NIFTY"]

        print(f"\n{'='*60}")
        print(f"  INDEX OPTIONS BACKTEST  —  Dhan API")
        print(f"  Indices : {', '.join(indices)}")
        print(f"  Period  : {args.from_date} → {args.to_date}")
        print(f"  Strike  : {args.strike}")
        print(f"  Capital : ₹{args.capital:,.0f}")
        print(f"  Expiry  : Weekly (NIFTY/BANKNIFTY/SENSEX all use weekly)")
        print(f"{'='*60}\n")

        for i, index_name in enumerate(indices, 1):
            cfg = INDEX_CONFIG[index_name]
            print(f"[{i:02d}/{len(indices)}] {index_name} ({cfg['name']}) "
                  f"lot={cfg['lot_size']} ...", flush=True)
            kill.reset()
            trades = run_index(
                index_name=index_name, from_date=args.from_date, to_date=args.to_date,
                strike_mode=args.strike, capital=args.capital, kill=kill,
            )
            all_trades.extend(trades)
            print(f"         {len(trades)} trades completed")
            time.sleep(1.0)

    if not all_trades:
        print("\nNo trades executed. Check date range, credentials, and API access.")
        sys.exit(0)

    report = build_report(all_trades, args.capital)
    print_report(report)


if __name__ == "__main__":
    main()
