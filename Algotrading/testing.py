import yfinance as yf
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
# Select stock (NSE example)
# symbol = "RELIANCE.NS"
symbol = "^NSEI"
# Define exact date range (5 days back)
end_date = datetime.today()
start_date = end_date - timedelta(days=150)


# Fetch wave
wave = yf.download(symbol,start=start_date,end=end_date,interval="1d")
# Fetch tide
tide = yf.download(symbol,start=start_date,end=end_date,interval="1wk")


# Keep only OHLCV columns
ohlcv = wave[['Open', 'High', 'Low', 'Close', 'Volume']]
ohlcv = tide[['Open', 'High', 'Low', 'Close', 'Volume']]

# =====================================================================
# EMA 5, 13, 26, 50 & 12 for MACD 
# =====================================================================
#for wave calculation
# Calculate EMA 5
wave['EMA_5'] = wave['Close'].ewm(span=5, adjust=False).mean().round(2)

print('Close' , type(wave['Close']))
print('EMA_5' ,type(wave['EMA_5']))

print(wave.loc[(wave['Close'].squeeze() < wave['EMA_5'].squeeze())])


