import pandas as pd
import numpy as np
import requests
from datetime import datetime

# =========================
# CONFIG
# =========================
client_id = "1111077247"
access_token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJpc3MiOiJkaGFuIiwicGFydG5lcklkIjoiIiwiZXhwIjoxNzc2MTAwNzMyLCJpYXQiOjE3NzYwMTQzMzIsInRva2VuQ29uc3VtZXJUeXBlIjoiU0VMRiIsIndlYmhvb2tVcmwiOiIiLCJkaGFuQ2xpZW50SWQiOiIxMTExMDc3MjQ3In0.d_LyApHgUpLSPy8PaTMS2xPZe-SEc8v1g5UP0gB2i-BPqyOWD2kVloK0EkH32lMb5HeR5w7Rs5JRT0P9XMfVag"


INITIAL_CAPITAL = 100000

symbol_map = {
    "RELIANCE.NS": "1333",
    "TCS.NS": "11536",
    "INFY.NS": "1594",
    #"HDFCBANK.NS": "1330",
    "ICICIBANK.NS": "4963",
    "SBIN.NS": "3045",
    "BHARTIARTL.NS": "10604",
    "ITC.NS": "1660",
    "KOTAKBANK.NS": "1922",
    "LT.NS": "11483",
    "AXISBANK.NS": "5900",
    "HINDUNILVR.NS": "1394",
    "BAJFINANCE.NS": "317",
    "ASIANPAINT.NS": "236",
    "MARUTI.NS": "10999",
    "SUNPHARMA.NS": "3351",
    "TITAN.NS": "3506",
    "WIPRO.NS": "3787",
    "ULTRACEMCO.NS": "11532",
    "NESTLEIND.NS": "17963",
    "POWERGRID.NS": "14977",
    "NTPC.NS": "11630",
    "TECHM.NS": "13538",
    "TATASTEEL.NS": "3499",
    "JSWSTEEL.NS": "11723",
    "HCLTECH.NS": "7229",
    "ADANIENT.NS": "25",
    "ADANIPORTS.NS": "10217",
    "COALINDIA.NS": "20374",
    "INDUSINDBK.NS": "5258",
    "DRREDDY.NS": "881",
    "CIPLA.NS": "694",
    "HEROMOTOCO.NS": "1348",
    "BAJAJ-AUTO.NS": "16669",
    "EICHERMOT.NS": "910",
    "APOLLOHOSP.NS": "157",
    "BRITANNIA.NS": "547",
    "DIVISLAB.NS": "10940",
    "GRASIM.NS": "1232",
    "HDFCLIFE.NS": "4244",
    "SBILIFE.NS": "21808",
    "BAJAJFINSV.NS": "16675",
    "ONGC.NS": "2475",
    "BPCL.NS": "526",
    "HINDALCO.NS": "1363",
    "UPL.NS": "11287",
    "TATACONSUM.NS": "3432",
    "M&M.NS": "2031",
    "DLF.NS": "14732",
    "LTIM.NS": "17130"
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

    # =========================
    # FETCH 1-dily DATA
    # =========================
    payload = {
        "securityId": security_id,
        "exchangeSegment": "NSE_EQ",
        "instrument": "EQUITY",
        "interval": "1d",
        "oi": False,
        "fromDate": "2020-01-05",
        "toDate": "2026-02-09"
    }

    response = requests.post(url, json=payload, headers=headers)

    print("Status Code:", response.status_code)

    data = response.json()

    if "open" not in data or len(data["open"]) == 0:
        raise Exception("No data received from Dhan API")

    # =========================
    # CREATE DATAFRAME (1-daily)
    # =========================
    df = pd.DataFrame({
        "open": data["open"],
        "high": data["high"],
        "low": data["low"],
        "close": data["close"],
        "volume": data["volume"],
        "timestamp": data["timestamp"]
    })

    # Convert timestamp
    df["datetime"] = pd.to_datetime(df["timestamp"], unit="s")
    df = df.sort_values("datetime")
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
        df["ema12"] = df["close"].ewm(span=12).mean()
        df["ema13"] = df["close"].ewm(span=13).mean()
        df["ema26"] = df["close"].ewm(span=26).mean()
        df["ema50"] = df["close"].ewm(span=50).mean()

        df["vol_ma"] = df["volume"].rolling(20).mean()
        df['macd'] = df["ema12"] - df["ema26"]
        df['macd_signal'] = df['macd'].ewm(span=9).mean()
        delta = df["close"].diff()
        gain = delta.clip(lower=0).rolling(14).mean()
        loss = -delta.clip(upper=0).rolling(14).mean()
        rs = gain / loss
        df["rsi"] = 100 - (100 / (1 + rs))
        high, low, close = df['high'], df['low'], df['close']
        plus_dm = high.diff()
        minus_dm = low.diff() * -1
        plus_dm[plus_dm < 0] = 0
        minus_dm[minus_dm < 0] = 0
        tr = pd.concat([
            high - low,
            (high - close.shift()).abs(),
            (low - close.shift()).abs()
        ], axis=1).max(axis=1)

        atr = tr.rolling(14).mean()
        df['plus_di'] = 100 * (plus_dm.rolling(14).mean() / atr)
        df['minus_di'] = 100 * (minus_dm.rolling(14).mean() / atr)
        df['adx'] = (abs(df['plus_di'] - df['minus_di']) /
                     (df['plus_di'] + df['minus_di'])) * 100
        return df

# =========================
# STRATEGY (UNCHANGED)
# =========================
class Strategy:

    def __init__(self, wave, tide):
        self.wave = Indicators.calculate(wave)
        self.tide = Indicators.calculate(tide)

    # =========================
    # SIGNAL GENERATION
    # =========================
    def generate_signals(self):

        signals = []

        for i in range(len(self.wave)):

            if i < 50:
                continue

            w = self.wave.iloc[i]
            time = self.wave.index[i]

            t = self.tide.loc[:time].iloc[-1]

            # =========================
            # BUY CONDITIONS
            # =========================
            buy_conds = [
                t['close'] > w['bb_upper'] * 0.80,
                t['macd'] > 0,
                t['rsi'] > 50,
                w['rsi'] > 60 and self.wave['rsi'].iloc[i-1] <= 60,
                w['close'] >= w['bb_upper'] * 0.90 and w['close'] > self.wave['close'].iloc[i-1],
                w['volume'] > w['vol_ma'],
                w['ema5'] >= w['ema13'] or w['ema5'] >= w['ema26'],
                w['plus_di'] > w['minus_di'],
                w['adx'] > 15 and w['adx'] > self.wave['adx'].iloc[i-1],
                w['close'] > w['ema50']
            ]

            # =========================
            # SELL CONDITIONS
            # =========================
            sell_conds = [
                t['close'] < w['bb_lower'] * 1.20,
                t['macd'] < 0,
                t['rsi'] < 50,
                w['rsi'] < 40 and self.wave['rsi'].iloc[i-1] >= 40,
                w['close'] <= w['bb_lower'] * 1.10 and w['close'] < self.wave['close'].iloc[i-1],
                w['volume'] > w['vol_ma'],
                w['ema5'] < w['ema13'] or w['ema5'] <= w['ema26'],
                w['plus_di'] < w['minus_di'],
                w['adx'] > 15 and w['adx'] > self.wave['adx'].iloc[i-1],
                w['close'] < w['ema50']
            ]

            if all(buy_conds):
                signals.append({
                    "i": i,
                    "date": time,
                    "signal": "BUY"
                })

            elif all(sell_conds):
                signals.append({
                    "i": i,
                    "date": time,
                    "signal": "SELL"
                })

        return pd.DataFrame(signals)

    # =========================
    # PAPER TRADING ENGINE
    # =========================
    def paper_trading(self, initial_capital=100000):

        signals = self.generate_signals()

        capital = initial_capital

        position = 0
        entry_price = 0
        entry_type = None

        trades = []

        for _, row in signals.iterrows():

            i = int(row["i"])

            current = self.wave.iloc[i]
            prev = self.wave.iloc[i - 1]

            close = current["close"]
            prev_open = prev["open"]

            # =========================
            # ENTRY
            # =========================
            if position == 0:

                # =========================
                # POSITION SIZING (2% RISK)
                # =========================
                risk_amount = capital * 0.02
                sl_price = close * 0.98  # 2% price SL
                risk_per_share = abs(close - sl_price)

                if risk_per_share == 0:
                    continue

                position_size = risk_amount / risk_per_share

                # cap exposure (safety)
                max_position = capital / close
                position_size = min(position_size, max_position)

                # =========================
                # LONG ENTRY
                # =========================
                if row["signal"] == "BUY":

                    entry_price = close
                    entry_type = "LONG"
                    position = position_size

                    trades.append({
                        "date": row["date"],
                        "type": "BUY",
                        "price": entry_price,
                        "position": position,
                        "capital": capital
                    })

                # =========================
                # SHORT ENTRY
                # =========================
                elif row["signal"] == "SELL":

                    entry_price = close
                    entry_type = "SHORT"
                    position = position_size

                    trades.append({
                        "date": row["date"],
                        "type": "SELL",
                        "price": entry_price,
                        "position": position,
                        "capital": capital
                    })

            # =========================
            # EXIT MANAGEMENT
            # =========================
            else:

                sl_limit = capital * 0.98  # 2% capital protection

                # =========================
                # LONG EXIT
                # =========================
                if entry_type == "LONG":

                    sl_hit = capital <= sl_limit
                    trail_exit = close < prev_open

                    if sl_hit or trail_exit:

                        exit_price = close
                        pnl = (exit_price - entry_price) * position
                        capital += pnl

                        trades.append({
                            "date": row["date"],
                            "type": "SELL",
                            "price": exit_price,
                            "position": position,
                            "pnl": pnl,
                            "capital": capital,
                            "reason": "SL" if sl_hit else "TRAIL_EXIT"
                        })

                        position = 0
                        entry_price = 0
                        entry_type = None

                # =========================
                # SHORT EXIT
                # =========================
                elif entry_type == "SHORT":

                    sl_hit = capital <= sl_limit
                    trail_exit = close > prev_open

                    if sl_hit or trail_exit:

                        exit_price = close
                        pnl = (entry_price - exit_price) * position
                        capital += pnl

                        trades.append({
                            "date": row["date"],
                            "type": "BUY_COVER",
                            "price": exit_price,
                            "position": position,
                            "pnl": pnl,
                            "capital": capital,
                            "reason": "SL" if sl_hit else "TRAIL_EXIT"
                        })

                        position = 0
                        entry_price = 0
                        entry_type = None

        return pd.DataFrame(trades), capital

    # =========================
    # REPORT
    # =========================
    def report(self, trades, final_capital):

        if "pnl" not in trades.columns:
            print("\nNo completed trades (no exits triggered)")
            print("Final Capital:", final_capital)
            return

        exits = trades[trades["type"].isin(["SELL", "BUY_COVER"])]

        if exits.empty:
            print("\nNo exit trades found")
            print("Final Capital:", final_capital)
            return

        total_trades = len(exits)
        wins = exits[exits["pnl"] > 0]
        losses = exits[exits["pnl"] <= 0]

        win_rate = (len(wins) / total_trades * 100) if total_trades else 0

        print("\n================ REPORT ================")
        print("Total Trades:", total_trades)
        print("Wins:", len(wins))
        print("Losses:", len(losses))
        print("Win Rate:", round(win_rate, 2), "%")
        print("Final Capital:", final_capital)

all_trades = []
final_capitals = []
for s in symbols:
    print(f"\nRunning: {s}")
    security_id = symbol_map[s]

    df_1d = fetch_data(security_id)

    if df_1d.empty:
        print("No data, skipping")
        continue

    df_1d = Indicators.calculate(df_1d)

    df_1w = df_1d.resample('1W').agg({
        "open": "first",
        "high": "max",
        "low": "min",
        "close": "last",
        "volume": "sum"
    }).dropna()

    df_1w = Indicators.calculate(df_1w)

    strategy = Strategy(df_1d, df_1w)

    trades, final_capital = strategy.paper_trading(initial_capital=100000)

    # ✅ ADD SYMBOL COLUMN
    trades["symbol"] = s

    # ✅ COLLECT ALL TRADES
    all_trades.append(trades)

    # ✅ STORE FINAL CAPITAL
    final_capitals.append(final_capital)

combined_trades = pd.concat(all_trades, ignore_index=True)

def final_report(trades, final_capitals):

    print("\n=========== FINAL COMBINED REPORT ===========")

    if "pnl" not in trades.columns:
        print("No completed trades found")
        return

    exits = trades[trades["type"].isin(["SELL", "BUY_COVER"])]

    if exits.empty:
        print("No exit trades found")
        return

    total_trades = len(exits)
    wins = exits[exits["pnl"] > 0]
    losses = exits[exits["pnl"] <= 0]

    win_rate = (len(wins) / total_trades) * 100

    total_pnl = exits["pnl"].sum()

    print("Total Trades:", total_trades)
    print("Wins:", len(wins))
    print("Losses:", len(losses))
    print("Win Rate:", round(win_rate, 2), "%")
    print("Total PnL:", round(total_pnl, 2))
    print("Avg PnL per trade:", round(total_pnl / total_trades, 2))

    print("\nCapital Summary:")
    print("Avg Final Capital per Stock:", sum(final_capitals)/len(final_capitals))
    print("Best Stock Capital:", max(final_capitals))
    print("Worst Stock Capital:", min(final_capitals))

final_report(combined_trades, final_capitals)
print("\nTop Performing Stocks:")
print(combined_trades.groupby("symbol")["pnl"].sum().sort_values(ascending=False).head(10))
