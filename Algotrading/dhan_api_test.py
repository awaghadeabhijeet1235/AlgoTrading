import requests
import pandas as pd

# =========================
# CONFIG
# =========================
client_id = "1111077247"
access_token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJpc3MiOiJkaGFuIiwicGFydG5lcklkIjoiIiwiZXhwIjoxNzc1ODk2ODA1LCJpYXQiOjE3NzU4MTA0MDUsInRva2VuQ29uc3VtZXJUeXBlIjoiU0VMRiIsIndlYmhvb2tVcmwiOiIiLCJkaGFuQ2xpZW50SWQiOiIxMTExMDc3MjQ3In0.Pe7X9mTBpxgGU5ATxmQX-XnGnSyx1zBYiS58TdLFrJrXt4elDmUSx4alHN4pUmb674GZVhg8wlZ9ALKr7UD5xA"


# Example: RELIANCE security_id
security_id = "1333"

# =========================
# API CALL
# =========================
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
    "fromDate": "2025-04-01",
    "toDate": "2025-04-09",
    "interval": "15"
}

response = requests.post(url, json=payload, headers=headers)

# =========================
# DEBUG RESPONSE
# =========================
print("Status Code:", response.status_code)
print("Raw Response:", response.text[:500])

# =========================
# PARSE JSON
# =========================
try:
    data = response.json()
except Exception as e:
    print("JSON parse error:", e)
    exit()

# =========================
# VALIDATION (IMPORTANT FIX)
# =========================
if "open" not in data or len(data["open"]) == 0:
    print("❌ No data returned from API")
    exit()

# =========================
# CREATE DATAFRAME
# =========================
df = pd.DataFrame({
    "open": data["open"],
    "high": data["high"],
    "low": data["low"],
    "close": data["close"],
    "volume": data["volume"],
    "timestamp": data["timestamp"]
})

# Convert timestamp (IMPORTANT FIX)
df["datetime"] = pd.to_datetime(df["timestamp"], unit="s")
df.set_index("datetime", inplace=True)

df = df[["open", "high", "low", "close", "volume"]]

# =========================
# OUTPUT
# =========================
print("\n✅ DATA RECEIVED SUCCESSFULLY")
print(df.head())
print("\nTotal Rows:", len(df))