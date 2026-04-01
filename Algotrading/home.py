import yfinance as yf
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
# Select stock (NSE example)
# symbol = "RELIANCE.NS"
symbol = "^NSEI"
# Define exact date range (5 days back)
end_date = datetime.today()
start_date = end_date - timedelta(days=50)

# Fetch wave
wave = yf.download(symbol,start=start_date,end=end_date,interval="1d")
# Fetch tide
tide = yf.download(symbol,start=start_date,end=end_date,interval="1wk")

# Keep only OHLCV columns
ohlcv = wave[['Open', 'High', 'Low', 'Close', 'Volume']]
ohlcv = tide[['Open', 'High', 'Low', 'Close', 'Volume']]
ohlcv = wave.squeeze()
# ========================================================================================================================
# EMA 5, 13, 26, 50 & 12 for MACD 
# ========================================================================================================================
#for wave calculation
wave['Close'] = wave['Close'].round(2)
# Calculate EMA 5
wave['EMA_5'] = wave['Close'].ewm(span=5, adjust=False).mean().round(2)
# Fast EMA (12)
wave['EMA_12'] = wave['Close'].ewm(span=12, adjust=False).mean().round(2)
# Calculate EMA 13
wave['EMA_13'] = wave['Close'].ewm(span=13, adjust=False).mean().round(2)
# Calculate EMA 26
wave['EMA_26'] = wave['Close'].ewm(span=26, adjust=False).mean().round(2)
# Calculate EMA 50
wave['EMA_50'] = wave['Close'].ewm(span=50, adjust=False).mean().round(2)

#for tide calculation
tide['Close'] = tide['Close'].round(2)
# Fast EMA (12) tide
tide['EMA_12'] = tide['Close'].ewm(span=12, adjust=False).mean().round(2)
# Calculate EMA 26
tide['EMA_26'] = tide['Close'].ewm(span=26, adjust=False).mean().round(2)

# ========================================================================================================================
# EMA 5, 13, 26, 50 & 12 for MACD logic end
# ========================================================================================================================


# ========================================================================================================================
# Bolinger Band (20,2) 
# ========================================================================================================================
# BB calculation for wave
length = 20
# SMA (20)
wave['SMA_20'] = wave['Close'].rolling(window=length).mean()
# Standard Deviation
wave['STD_20'] = wave['Close'].rolling(window=length).std()
# Upper & Lower Bands
wave['BB_Upper'] = wave['SMA_20'] + (2 * wave['STD_20'])
wave['BB_Lower'] = wave['SMA_20'] - (2 * wave['STD_20'])

# BB calculation for tide
length = 20
# SMA (20)
tide['SMA_20'] = tide['Close'].rolling(window=length).mean()
# Standard Deviation
tide['STD_20'] = tide['Close'].rolling(window=length).std()
# Upper & Lower Bands
tide['BB_Upper'] = tide['SMA_20'] + (2 * tide['STD_20'])
tide['BB_Lower'] = tide['SMA_20'] - (2 * tide['STD_20'])

# ========================================================================================================================
# Bolinger Band (20,2) logic end
# ========================================================================================================================


# ========================================================================================================================
# MACD (12,26 CLOSE 9 EMA) 
# ========================================================================================================================

# MACD Line (Oscillator) wave calculation
wave['MACD'] = wave['EMA_12'] - wave['EMA_26']
# Signal Line (EMA 9 of MACD)
wave['Signal'] = wave['MACD'].ewm(span=9, adjust=False).mean()
# Histogram
wave['Histogram'] = wave['MACD'] - wave['Signal']

# MACD Line (Oscillator) tide calculation
tide['MACD'] = tide['EMA_12'] - tide['EMA_26']
# Signal Line (EMA 9 of MACD)
tide['Signal'] = tide['MACD'].ewm(span=9, adjust=False).mean()
# Histogram
tide['Histogram'] = tide['MACD'] - tide['Signal']

# ========================================================================================================================
# MACD (12,26 CLOSE 9 EMA) logic end
# ========================================================================================================================

# ========================================================================================================================
# STOCHASTIC (14,3,3)
# ========================================================================================================================

# Step 1: Lowest Low & Highest High (14)
low_min = wave['Low'].rolling(window=14).min()
high_max = wave['High'].rolling(window=14).max()

# Step 2: Raw %K
wave['%K_raw'] = ((wave['Close'] - low_min) / (high_max - low_min)) * 100

# Step 3: Smooth %K (3-period SMA)
wave['%K'] = wave['%K_raw'].rolling(window=3).mean().round(2)

# Step 4: %D (3-period SMA of %K)
wave['%D'] = wave['%K'].rolling(window=3).mean().round(2)

# ========================================================================================================================
# STOCHASTIC (14,3,3) logic end
# ========================================================================================================================

# ========================================================================================================================
# RSI (14)
# ========================================================================================================================

delta = wave['Close'].diff()

gain = delta.clip(lower=0)
loss = -delta.clip(upper=0)

avg_gain = gain.ewm(span=14, adjust=False).mean()
avg_loss = loss.ewm(span=14, adjust=False).mean()

rs = avg_gain / avg_loss
wave['RSI_14'] = 100 - (100 / (1 + rs))

# ========================================================================================================================
# RSI (14)logic end
# ========================================================================================================================

# ========================================================================================================================
# ADX (DI length1 = 14, smoothing = 14)
# ========================================================================================================================

length1 = 14

# Step 1: True Range (TR)
wave['H-L'] = wave['High'] - wave['Low']
wave['H-PC'] = abs(wave['High'] - wave['Close'].shift(1))
wave['L-PC'] = abs(wave['Low'] - wave['Close'].shift(1))

wave['TR'] = wave[['H-L', 'H-PC', 'L-PC']].max(axis=1)

# Step 2: Directional Movement (+DM, -DM)
up_move = wave['High'].diff()
down_move = -wave['Low'].diff()

wave['+DM'] = ((up_move > down_move) & (up_move > 0)) * up_move
wave['-DM'] = ((down_move > up_move) & (down_move > 0)) * down_move

# Step 3: Smooth TR, +DM, -DM (EMA 14)
tr_smooth = wave['TR'].ewm(span=length1, adjust=False).mean()
plus_dm_smooth = wave['+DM'].ewm(span=length1, adjust=False).mean()
minus_dm_smooth = wave['-DM'].ewm(span=length1, adjust=False).mean()

# Step 4: +DI and -DI
wave['+DI'] = (plus_dm_smooth / tr_smooth) * 100
wave['-DI'] = (minus_dm_smooth / tr_smooth) * 100

# Step 5: DX
wave['DX'] = (abs(wave['+DI'] - wave['-DI']) /
              (wave['+DI'] + wave['-DI'])) * 100

# Step 6: ADX (EMA smoothing 14)
wave['ADX'] = wave['DX'].ewm(span=length1, adjust=False).mean()

# Round values
cols = ['+DI', '-DI', 'ADX']
wave[cols] = wave[cols].round(2)

#print(wave[['Close', '+DI', '-DI', 'ADX']].tail())

# ========================================================================================================================
# ADX (DI length = 14, smoothing = 14) logic end
# ========================================================================================================================


#print(wave[['Close', 'EMA_5','EMA_13','EMA_26','EMA_50','BB_Upper','SMA_20','BB_Lower','MACD','Signal','Histogram','%K', '%D','RSI_14','+DI', '-DI', 'ADX']].tail())
#print(wave[['Close','BB_Upper','SMA_20','BB_Lower','RSI_14']])
#print(tide[['Close','BB_Upper','SMA_20','BB_Lower']])

# ========================================================================================================================
# Strategy logic 
# ========================================================================================================================

wave['Signal_Wave'] = 'EXIT'
wave['Signal_Tide'] = 'EXIT'
# no buy and no sell returns -1

# BUY returns 1
#wave.loc[(wave['EMA_5'] > wave['EMA_5'].shift(1)) & (wave['EMA_13'] > wave['EMA_13'].shift(1)) & (wave['EMA_26'] > wave['EMA_26'].shift(1)) & (wave['EMA_5'] > wave['EMA_13']) & (wave['EMA_13'] > wave['EMA_26']) & (wave['EMA_26'] > wave['EMA_50']) ,'Signal_Wave'] = 1

#tide.loc[(tide['BB_Upper'] > tide['BB_Upper'].shift(1)) & (tide['Close'] > tide['SMA_20']),'Signal_Tide'] = 1
#tide.loc[(tide['BB_Upper'] > tide['BB_Upper'].shift(1)) ,'Signal_Tide'] = 1
#print(wave[['Close','BB_Upper','SMA_20','BB_Lower']])
# SELL returns 0
#wave.loc[(wave['EMA_5'] < wave['EMA_5'].shift(1)) & (wave['EMA_13'] < wave['EMA_13'].shift(1)) & (wave['EMA_26'] < wave['EMA_26'].shift(1)) & (wave['EMA_5'] < wave['EMA_13']) & (wave['EMA_13'] < wave['EMA_26']) & (wave['EMA_26'] < wave['EMA_50']) ,'Signal_Wave'] = 0

#print(wave[['Close','EMA_5','EMA_13','EMA_26','EMA_50','Signal_Wave']])
wave['ADX_pre']=wave['ADX'].shift(1)
# Spread Bear Call Swing Buy ATM call and Sell OTM call , view is bearish swing
wave.loc[(wave['RSI_14'] < 60) & (wave['ADX'] < wave['ADX_pre']) & (wave['Close'].squeeze() < wave['BB_Upper'].squeeze()), 'Signal_Wave'] = 'SELL SWING'

# Spread bull put Swing Buy OTM put and Sell ATM put , view is bullish swing
wave.loc[(wave['RSI_14'] > 40) & (wave['ADX'] < wave['ADX_pre']) & (wave['Close'].squeeze() > wave['BB_Lower'].squeeze()), 'Signal_Wave'] = 'BUY SWING'

#print(wave.loc[(wave['RSI_14'] < 60)])
#print only Signal_Wave = 1 , only buy Signal_Waves and Signal_Tide
#print(wave.loc[(wave['Signal_Wave'] == 'BUY SWING') , ['Close', 'Signal_Wave']])
#print(wave.loc[(wave['Signal_Wave'] == 1) & (wave['Signal_Tide'] == 1), ['Close', 'Signal_Wave', 'Signal_Tide']])

#print only Signal_Wave = 0 , only sell Signal_Waves and Signal_Tide
#print(wave.loc[(wave['Signal_Wave'] == 'SELL SWING') , ['Close', 'Signal_Wave']])
#print(wave.loc[(wave['Signal_Wave'] == 0) & (wave['Signal_Tide'] == 0), ['Close', 'Signal_Wave', 'Signal_Tide']])


print(wave.loc[(wave['Signal_Wave'] == 'BUY SWING') | (wave['Signal_Wave'] == 'SELL SWING') | (wave['Signal_Wave'] == 'EXIT'), ['Close', 'BB_Upper', 'BB_Lower', 'RSI_14', 'ADX', 'ADX_pre', 'Signal_Wave']])










