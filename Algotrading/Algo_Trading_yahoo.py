import pandas as pd
import numpy as np
import yfinance as yf

INITIAL_CAPITAL = 100000
RISK_PER_TRADE = 0.015
MAX_DRAWDOWN = 0.10
"""
symbols =  [
    'RELIANCE.NS', 'TCS.NS', 'HDFCBANK.NS', 'INFY.NS', 'HINDUNILVR.NS',
    'ICICIBANK.NS', 'SBIN.NS', 'BAJFINANCE.NS', 'BHARTIARTL.NS', 'KOTAKBANK.NS',
    'ITC.NS', 'LT.NS', 'HCLTECH.NS', 'AXISBANK.NS', 'ASIANPAINT.NS',
    'MARUTI.NS', 'SUNPHARMA.NS', 'TITAN.NS', 'BAJAJFINSV.NS', 'ADANIENT.NS',
    'ULTRACEMCO.NS', 'NESTLEIND.NS', 'POWERGRID.NS', 'WIPRO.NS',#'TATAMOTORS.NS',
    'M&M.NS', 'GRASIM.NS', 'ADANIPORTS.NS', 'NTPC.NS', 'HDFCLIFE.NS',
    'BAJAJ-AUTO.NS', 'COALINDIA.NS', 'TATASTEEL.NS', 'SBILIFE.NS', 'IOC.NS',
    'TECHM.NS', 'BRITANNIA.NS', 'HEROMOTOCO.NS', 'ONGC.NS', 'DIVISLAB.NS',
    'INDUSINDBK.NS', 'EICHERMOT.NS', 'CIPLA.NS', 'APOLLOHOSP.NS', 'DRREDDY.NS',
    'UPL.NS', 'TATACONSUM.NS', 'JSWSTEEL.NS', 'HINDALCO.NS', 'BPCL.NS'
]
"""
symbols =  ['GOLD APRIL FUT']
# =========================
# INDICATORS
# =========================
class Indicators:

    @staticmethod
    def calculate(df):
        df = df.copy()

        # Bollinger Bands
        sma = df['close'].rolling(20).mean()
        std = df['close'].rolling(20).std()
        df['bb_upper'] = sma + 2 * std
        df['bb_lower'] = sma - 2 * std

        # EMAs
        for l in [5, 13, 26, 50]:
            df[f'ema{l}'] = df['close'].ewm(span=l, adjust=False).mean()

        # Volume MA
        df['vol_ma'] = df['volume'].rolling(20).mean()

        # MACD
        ema12 = df['close'].ewm(span=12).mean()
        ema26 = df['close'].ewm(span=26).mean()
        df['macd'] = ema12 - ema26
        df['macd_signal'] = df['macd'].ewm(span=9).mean()

        # RSI
        delta = df['close'].diff()
        gain = delta.clip(lower=0).rolling(14).mean()
        loss = -delta.clip(upper=0).rolling(14).mean()
        rs = gain / loss
        df['rsi'] = 100 - (100 / (1 + rs))

        # ADX
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
# SIGNAL ENGINE
# =========================
class Strategy:

    def __init__(self, wave, tide):
        self.wave = Indicators.calculate(wave)
        self.tide = Indicators.calculate(tide)

    def buy_signal(self, i):
        w = self.wave.iloc[i]
        t = self.tide.iloc[i]

        conds = [
            t['close'] > w['bb_upper'] * 0.80,
            t['macd'] > 0,
            t['macd'] > self.tide['macd'].iloc[i-1] and t['macd'] > self.tide['macd'].iloc[i-2],
            t['rsi'] > 50,
            w['rsi'] > 60 and self.wave['rsi'].iloc[i-1] <= 60 ,#self.wave['rsi'].iloc[i-2] < 60,
            w['close'] >= w['bb_upper'] * 0.90 and w['close'] > self.wave['close'].iloc[i-1],
            w['volume'] > w['vol_ma'],
            w['ema5'] >= w['ema13'] or w['ema5'] >= w['ema26'],
            w['plus_di'] > w['minus_di'],
            w['adx'] > 15 and w['adx'] > self.wave['adx'].iloc[i-1],
            w['close'] > w['ema50']
        ]

        return all(conds)

    def sell_signal(self, i):
        w = self.wave.iloc[i]
        t = self.tide.iloc[i]

        conds = [
            t['close'] < w['bb_lower'] * 1.20,
            t['macd'] < 0,
            t['macd'] < self.tide['macd'].iloc[i-1] and t['macd'] < self.tide['macd'].iloc[i-2],
            t['rsi'] < 50,
            w['rsi'] < 40 and self.wave['rsi'].iloc[i-1] >= 40,
            w['close'] <= w['bb_lower'] * 1.10 and w['close'] < self.wave['close'].iloc[i-1],
            w['volume'] > w['vol_ma'],
            w['ema5'] < w['ema13'] or w['ema5'] <= w['ema26'],
            w['plus_di'] < w['minus_di'],
            w['adx'] > 15 and w['adx'] > self.wave['adx'].iloc[i-1],
            w['close'] < w['ema50']
        ]

        return all(conds)


# =========================
# PAPER TRADING (BACKTEST)
# =========================
def paper_trading(symbol):

    capital = INITIAL_CAPITAL
    peak = capital
    trades = []
    trade_dates = []

    data = yf.download(symbol, period="5y", interval="1d")

    if isinstance(data.columns, pd.MultiIndex):
        data.columns = data.columns.get_level_values(0)

    data.columns = [col.lower() for col in data.columns]
    data = data[['open', 'high', 'low', 'close', 'volume']]

    wave = data.copy()
    tide = data.resample('1W').agg({
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last',
        'volume': 'sum'
    }).dropna()

    tide = tide.reindex(wave.index, method='ffill')

    strat = Strategy(wave, tide)

    position = None

    for i in range(50, len(wave)):

        if capital < INITIAL_CAPITAL * (1 - MAX_DRAWDOWN):
            print("Max drawdown hit")
            break

        price = wave['close'].iloc[i]

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

            # Partial exit
            if pnl_pct > 0.01:
                capital += pnl * 0.5

            # Exit condition
            if pnl_pct < -0.015 or abs(pnl_pct) > 0.02:
                capital += pnl
                trades.append(pnl)

                trade_date = wave.index[i]
                trade_dates.append(trade_date)

                position = None

        peak = max(peak, capital)

    return trades, capital, trade_dates


# =========================
# REAL TRADING (DISABLED)
# =========================
def real_trading():
    print("Real trading disabled for safety.")




all_trades = []
final_capital = 100000

for s in symbols:
    all_dates = []
    trades, cap, dates = paper_trading(s)
    all_trades.extend(trades)
    all_dates.extend(dates)
    final_capital += (cap - 100000)

print("Final Capital:", final_capital)

wins = [t for t in all_trades if t > 0]
losses = [t for t in all_trades if t < 0]

print("Wins and total trades:", len(wins) , len(all_trades))
print("Win Rate:", (len(wins)/len(all_trades))*100)
print("Total Profit:", sum(wins))
print("Total Loss:", sum(losses))
unique_days = len(set(all_dates))

print("Total Trades:", len(all_trades))
#print("Trading Days:", unique_days)
#print("Trades per Day:", len(all_trades) / unique_days if unique_days else 0)

df_dates = pd.to_datetime(all_dates)
months = pd.Series(df_dates).dt.to_period("M")

#print(months.value_counts().sort_index())