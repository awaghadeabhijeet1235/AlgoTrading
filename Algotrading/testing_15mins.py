import requests
import pandas as pd

# =========================
# CONFIG
# =========================
client_id = "1111077247"
access_token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJpc3MiOiJkaGFuIiwicGFydG5lcklkIjoiIiwiZXhwIjoxNzc1ODk2ODA1LCJpYXQiOjE3NzU4MTA0MDUsInRva2VuQ29uc3VtZXJUeXBlIjoiU0VMRiIsIndlYmhvb2tVcmwiOiIiLCJkaGFuQ2xpZW50SWQiOiIxMTExMDc3MjQ3In0.Pe7X9mTBpxgGU5ATxmQX-XnGnSyx1zBYiS58TdLFrJrXt4elDmUSx4alHN4pUmb674GZVhg8wlZ9ALKr7UD5xA"

security_id = "5900"

url = "https://api.dhan.co/v2/charts/intraday"

headers = {
    "access-token": access_token,
    "client-id": client_id,
    "Content-Type": "application/json"
}

# =========================
# FETCH 1-MIN DATA
# =========================
payload = {
    "securityId": security_id,
    "exchangeSegment": "NSE_EQ",
    "instrument": "EQUITY",
    "interval": "1",
    "oi": False,
    "fromDate": "2026-04-05 09:15:00",
    "toDate": "2026-04-09 15:30:00"
}

response = requests.post(url, json=payload, headers=headers)

print("Status Code:", response.status_code)

data = response.json()

if "open" not in data or len(data["open"]) == 0:
    raise Exception("No data received from Dhan API")

# =========================
# CREATE DATAFRAME (1-min)
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

df = df[["open", "high", "low", "close", "volume"]]

# =========================
# FILTER NSE SESSION
# =========================
df = df.between_time("09:15", "15:30")

# =========================
# CONVERT TO 15-MIN CANDLES
# =========================
df_15 = df.resample("15min", origin="start_day").agg({
    "open": "first",
    "high": "max",
    "low": "min",
    "close": "last",
    "volume": "sum"
}).dropna()

# =========================
# OUTPUT
# =========================
print("\n✅ 15-MIN CANDLES GENERATED")
print(df)

print("\nTotal 15-min candles:", len(df))