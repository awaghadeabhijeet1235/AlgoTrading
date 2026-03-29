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

# ========================================================================================================================
# EMA 5, 13, 26, 50 & 12 for MACD 
# ========================================================================================================================
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
# ========================================================================================================================
# EMA 5, 13, 26, 50 & 12 for MACD logic end
# ========================================================================================================================


# ========================================================================================================================
# Bolinger Band (20,2) 
# ========================================================================================================================
# Bollinger Band settings
length = 20

# SMA (20)
data['SMA_20'] = data['Close'].rolling(window=length).mean()

# Standard Deviation
data['STD_20'] = data['Close'].rolling(window=length).std()

# Upper & Lower Bands
data['BB_Upper'] = data['SMA_20'] + (2 * data['STD_20'])
data['BB_Lower'] = data['SMA_20'] - (2 * data['STD_20'])

# ========================================================================================================================
# Bolinger Band (20,2) logic end
# ========================================================================================================================


# ========================================================================================================================
# MACD (12,26 CLOSE 9 EMA) 
# ========================================================================================================================

# MACD Line (Oscillator)
data['MACD'] = data['EMA_12'] - data['EMA_26']

# Signal Line (EMA 9 of MACD)
data['Signal'] = data['MACD'].ewm(span=9, adjust=False).mean()

# Histogram
data['Histogram'] = data['MACD'] - data['Signal']

# ========================================================================================================================
# MACD (12,26 CLOSE 9 EMA) logic end
# ========================================================================================================================

# ========================================================================================================================
# STOCHASTIC (14,3,3)
# ========================================================================================================================

# Step 1: Lowest Low & Highest High (14)
low_min = data['Low'].rolling(window=14).min()
high_max = data['High'].rolling(window=14).max()

# Step 2: Raw %K
data['%K_raw'] = ((data['Close'] - low_min) / (high_max - low_min)) * 100

# Step 3: Smooth %K (3-period SMA)
data['%K'] = data['%K_raw'].rolling(window=3).mean().round(2)

# Step 4: %D (3-period SMA of %K)
data['%D'] = data['%K'].rolling(window=3).mean().round(2)

# ========================================================================================================================
# STOCHASTIC (14,3,3) logic end
# ========================================================================================================================

# ========================================================================================================================
# RSI (14)
# ========================================================================================================================

delta = data['Close'].diff()

gain = delta.clip(lower=0)
loss = -delta.clip(upper=0)

avg_gain = gain.ewm(span=14, adjust=False).mean()
avg_loss = loss.ewm(span=14, adjust=False).mean()

rs = avg_gain / avg_loss
data['RSI_14'] = 100 - (100 / (1 + rs))

# ========================================================================================================================
# RSI (14)logic end
# ========================================================================================================================

# ========================================================================================================================
# ADX (DI length1 = 14, smoothing = 14)
# ========================================================================================================================

length1 = 14

# Step 1: True Range (TR)
data['H-L'] = data['High'] - data['Low']
data['H-PC'] = abs(data['High'] - data['Close'].shift(1))
data['L-PC'] = abs(data['Low'] - data['Close'].shift(1))

data['TR'] = data[['H-L', 'H-PC', 'L-PC']].max(axis=1)

# Step 2: Directional Movement (+DM, -DM)
up_move = data['High'].diff()
down_move = -data['Low'].diff()

data['+DM'] = ((up_move > down_move) & (up_move > 0)) * up_move
data['-DM'] = ((down_move > up_move) & (down_move > 0)) * down_move

# Step 3: Smooth TR, +DM, -DM (EMA 14)
tr_smooth = data['TR'].ewm(span=length1, adjust=False).mean()
plus_dm_smooth = data['+DM'].ewm(span=length1, adjust=False).mean()
minus_dm_smooth = data['-DM'].ewm(span=length1, adjust=False).mean()

# Step 4: +DI and -DI
data['+DI'] = (plus_dm_smooth / tr_smooth) * 100
data['-DI'] = (minus_dm_smooth / tr_smooth) * 100

# Step 5: DX
data['DX'] = (abs(data['+DI'] - data['-DI']) /
              (data['+DI'] + data['-DI'])) * 100

# Step 6: ADX (EMA smoothing 14)
data['ADX'] = data['DX'].ewm(span=length1, adjust=False).mean()

# Round values
cols = ['+DI', '-DI', 'ADX']
data[cols] = data[cols].round(2)

#print(data[['Close', '+DI', '-DI', 'ADX']].tail())

# ========================================================================================================================
# ADX (DI length = 14, smoothing = 14) logic end
# ========================================================================================================================


print(data[['Close', 'EMA_5','EMA_13','EMA_26','EMA_50','BB_Upper','SMA_20','BB_Lower','MACD','Signal','Histogram','%K', '%D','RSI_14','+DI', '-DI', 'ADX']].tail())
