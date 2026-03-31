from dhanhq import dhanhq
import pandas as pd

dhan = dhanhq("1111077247", "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJpc3MiOiJkaGFuIiwicGFydG5lcklkIjoiIiwiZXhwIjoxNzc1MDUyMjYzLCJpYXQiOjE3NzQ5NjU4NjMsInRva2VuQ29uc3VtZXJUeXBlIjoiU0VMRiIsIndlYmhvb2tVcmwiOiIiLCJkaGFuQ2xpZW50SWQiOiIxMTExMDc3MjQ3In0.VWLZNbQKqqZBpOlmip-7q46_ViyAOzS3mUErKIHUlpWvZ0BRgK7JT8sPl_XpTrTWJudPYn1coZ0YfvDr7Jt72w")

data = dhan.historical_daily_data(
    security_id="1333",
    exchange_segment="NSE_EQ",
    instrument_type="EQ",
    from_date="2024-01-01",
    to_date="2024-02-01"
)

print("API Response:", data)

if 'data' in data and isinstance(data['data'], list):
    df = pd.DataFrame(data['data'])
    print(df[['open','high','low','close']].head())
else:
    print("❌ Error in API response")