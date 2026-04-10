import pandas as pd
import numpy as np
import requests
from datetime import datetime

# =========================
# CONFIG
# =========================
client_id = "1111077247"
access_token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJpc3MiOiJkaGFuIiwicGFydG5lcklkIjoiIiwiZXhwIjoxNzc1ODk2ODA1LCJpYXQiOjE3NzU4MTA0MDUsInRva2VuQ29uc3VtZXJUeXBlIjoiU0VMRiIsIndlYmhvb2tVcmwiOiIiLCJkaGFuQ2xpZW50SWQiOiIxMTExMDc3MjQ3In0.Pe7X9mTBpxgGU5ATxmQX-XnGnSyx1zBYiS58TdLFrJrXt4elDmUSx4alHN4pUmb674GZVhg8wlZ9ALKr7UD5xA"


INITIAL_CAPITAL = 100000

symbol_map = {
    "RELIANCE.NS": "1333",
    "TCS.NS": "11536"
}

symbols = list(symbol_map.keys())

# =========================
# FETCH DATA FROM DHAN
# =========================
def fetch_data(security_id):

    url = "https://api.dhan.co/v2/charts/historical"

    headers = {
        "access-token": access_token,
        "client-id": client_id,
        "Content-Type": "application/json"
    }

    payload = {
        "securityId": security_id,
        "exchangeSegment": "NSE_EQ",
        "instrument": "EQUITY",
        "fromDate": "2026-01-01",
        "toDate": "2026-04-09",
        "interval": "15"
    }

    response = requests.post(url, json=payload, headers=headers)
    data = response.json()

    if "open" not in data or len(data["open"]) == 0:
        print("No data for:", security_id)
        return pd.DataFrame()

    df = pd.DataFrame({
        "open": data["open"],
        "high": data["high"],
        "low": data["low"],
        "close": data["close"],
        "volume": data["volume"],
        "timestamp": data["timestamp"]
    })

    df["datetime"] = pd.to_datetime(df["timestamp"], unit="s")
    df.set_index("datetime", inplace=True)

    return df[["open", "high", "low", "close", "volume"]]

# =========================
# INDICATORS
# =========================
class Indicators:

    @staticmethod
    def calculate(df):
        df = df.copy()

        df["bb_mid"] = df["close"].rolling(20).mean()
        df["bb_std"] = df["close"].rolling(20).std()
        df["bb_upper"] = df["bb_mid"] + 2 * df["bb_std"]
        df["bb_lower"] = df["bb_mid"] - 2 * df["bb_std"]

        df["ema5"] = df["close"].ewm(span=5).mean()
        df["ema13"] = df["close"].ewm(span=13).mean()
        df["ema26"] = df["close"].ewm(span=26).mean()
        df["ema50"] = df["close"].ewm(span=50).mean()

        df["vol_ma"] = df["volume"].rolling(20).mean()

        delta = df["close"].diff()
        gain = delta.clip(lower=0).rolling(14).mean()
        loss = -delta.clip(upper=0).rolling(14).mean()
        rs = gain / loss
        df["rsi"] = 100 - (100 / (1 + rs))

        return df

# =========================
# STRATEGY (YOUR LOGIC FIXED SAFELY)
# =========================
class Strategy:

    def __init__(self, df):
        self.df = Indicators.calculate(df)

    def buy_signal(self, i):
        df = self.df

        if i < 50:
            return False

        return all([
            df["close"].iloc[i] > df["bb_upper"].iloc[i] * 0.95
            #,
            #df["rsi"].iloc[i] > 55,
            #df["ema5"].iloc[i] > df["ema13"].iloc[i],
            #df["volume"].iloc[i] > df["vol_ma"].iloc[i]
        ])

    def sell_signal(self, i):
        df = self.df

        if i < 50:
            return False

        return all([
            df["close"].iloc[i] < df["bb_lower"].iloc[i] * 1.05
            #,
            #df["rsi"].iloc[i] < 45,
            #df["ema5"].iloc[i] < df["ema13"].iloc[i]
        ])

# =========================
# BACKTEST ENGINE
# =========================
def backtest(symbol, security_id):

    df = fetch_data(security_id)

    if df.empty:
        return [], 100000

    strat = Strategy(df)

    capital = INITIAL_CAPITAL
    position = None
    trades = []

    for i in range(50, len(df)):

        price = df["close"].iloc[i]

        # ENTRY
        if position is None:

            if strat.buy_signal(i):
                position = ("LONG", price)

            elif strat.sell_signal(i):
                position = ("SHORT", price)

        # EXIT
        else:
            direction, entry = position

            pnl = (price - entry) if direction == "LONG" else (entry - price)
            pnl_pct = pnl / entry

            if pnl_pct < -0.015 or pnl_pct > 0.02:
                capital += pnl
                trades.append(pnl)
                position = None

    return trades, capital

# =========================
# RUN BACKTEST
# =========================
all_trades = []
final_capital = INITIAL_CAPITAL

for s in symbols:
    print("\nRunning:", s)

    trades, cap = backtest(s, symbol_map[s])

    all_trades.extend(trades)
    final_capital += (cap - INITIAL_CAPITAL)

# =========================
# RESULTS
# =========================
wins = [t for t in all_trades if t > 0]
losses = [t for t in all_trades if t < 0]

print("\n========== RESULTS ==========")
print("Final Capital:", final_capital)
print("Total Trades:", len(all_trades))
print("Win Rate:", (len(wins)/len(all_trades))*100 if all_trades else 0)
print("Profit:", sum(wins))
print("Loss:", sum(losses))