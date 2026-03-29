import yfinance as yf
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime, timedelta

# Select stock (NSE example)
symbol = "RELIANCE.NS"

# Define exact date range (5 months back)
end_date = datetime.today()
start_date = end_date - timedelta(days=150)

# Fetch data
data = yf.download(symbol,start=start_date,end=end_date,interval="1d")

# Keep only OHLCV columns
ohlcv = data[['Open', 'High', 'Low', 'Close', 'Volume']]


data['Close'] = data['Close'].round(2)
# Calculate EMA 5
data['EMA_5'] = data['Close'].ewm(span=5, adjust=False).mean().round(2)
# Calculate EMA 13
data['EMA_13'] = data['Close'].ewm(span=13, adjust=False).mean().round(2)
# Calculate EMA 26
data['EMA_26'] = data['Close'].ewm(span=26, adjust=False).mean().round(2)
# Calculate EMA 50
data['EMA_50'] = data['Close'].ewm(span=50, adjust=False).mean().round(2)

#print(data[['Close', 'EMA_5','EMA_13','EMA_26','EMA_50']].tail())


# Bollinger Band settings
length = 20

# SMA (20)
data['SMA_20'] = data['Close'].rolling(window=length).mean()

# Standard Deviation
data['STD_20'] = data['Close'].rolling(window=length).std()

# Upper & Lower Bands
data['BB_Upper'] = data['SMA_20'] + (2 * data['STD_20'])
data['BB_Lower'] = data['SMA_20'] - (2 * data['STD_20'])

# Optional: Round values
cols = ['Close', 'SMA_20', 'BB_Upper', 'BB_Lower']
data[cols] = data[cols].round(2)
print(data[['Close', 'EMA_5','EMA_13','EMA_26','EMA_50','BB_Upper','SMA_20','BB_Lower']].tail())