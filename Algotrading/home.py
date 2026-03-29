import yfinance as yf
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime, timedelta

# Select stock (NSE example)
symbol = "RELIANCE.NS"

# Define exact date range (5 months back)
end_date = datetime.today()
start_date = end_date - timedelta(days=50)

# Fetch data
data = yf.download(symbol,start=start_date,end=end_date,interval="1d")

# Keep only OHLCV columns
ohlcv = data[['Open', 'High', 'Low', 'Close', 'Volume']]

# ==============================
# EMA 5, 13, 26, 50 & 12 for MACD 
# ==============================

data['Close'] = data['Close'].round(2)
# Calculate EMA 5
data['EMA_5'] = data['Close'].ewm(span=5, adjust=False).mean().round(2)
# Fast EMA (12)
data['EMA_12'] = data['Close'].ewm(span=12, adjust=False).mean().round(2)
# Calculate EMA 13
data['EMA_13'] = data['Close'].ewm(span=13, adjust=False).mean().round(2)
# Calculate EMA 26
data['EMA_26'] = data['Close'].ewm(span=26, adjust=False).mean().round(2)
# Calculate EMA 50
data['EMA_50'] = data['Close'].ewm(span=50, adjust=False).mean().round(2)

# ==============================
# EMA 5, 13, 26, 50 & 12 for MACD logic end
# ==============================

#print(data[['Close', 'EMA_5','EMA_13','EMA_26','EMA_50']].tail())

# ==============================
# Bolinger Band (20,2) 
# ==============================

# Bollinger Band settings
length = 20

# SMA (20)
data['SMA_20'] = data['Close'].rolling(window=length).mean()

# Standard Deviation
data['STD_20'] = data['Close'].rolling(window=length).std()

# Upper & Lower Bands
data['BB_Upper'] = data['SMA_20'] + (2 * data['STD_20'])
data['BB_Lower'] = data['SMA_20'] - (2 * data['STD_20'])

# ==============================
# Bolinger Band (20,2) logic end
# ==============================

# Optional: Round values
#cols = ['Close', 'SMA_20', 'BB_Upper', 'BB_Lower']
#data[cols] = data[cols].round(2)
#print(data[['Close', 'EMA_5','EMA_13','EMA_26','EMA_50','BB_Upper','SMA_20','BB_Lower']].tail())


# ==============================
# MACD (12,26 CLOSE 9 EMA) 
# ==============================

# MACD Line
data['MACD'] = data['EMA_12'] - data['EMA_26']

# Signal Line (EMA of MACD, 9 period)
data['Signal'] = data['MACD'].ewm(span=9, adjust=False).mean()

# Histogram
data['Histogram'] = data['MACD'] - data['Signal']

# Optional: round values
cols = ['Close', 'MACD', 'Signal', 'Histogram']
data[cols] = data[cols].round(2)

# ==============================
# MACD (12,26 CLOSE 9 EMA) logic end
# ==============================


# ==============================
# STOCHASTIC (14,3,3)
# ==============================

# Step 1: Lowest Low & Highest High (14)
low_min = data['Low'].rolling(window=14).min()
high_max = data['High'].rolling(window=14).max()

# Step 2: Raw %K
data['%K_raw'] = ((data['Close'] - low_min) / (high_max - low_min)) * 100

# Step 3: Smooth %K (3-period SMA)
data['%K'] = data['%K_raw'].rolling(window=3).mean().round(2)

# Step 4: %D (3-period SMA of %K)
data['%D'] = data['%K'].rolling(window=3).mean().round(2)

# ==============================
# STOCHASTIC (14,3,3) logic end
# ==============================

print(data[['Close', 'EMA_5','EMA_13','EMA_26','EMA_50','BB_Upper','SMA_20','BB_Lower','MACD','Signal','%K', '%D']].tail())
