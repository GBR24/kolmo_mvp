import quandl
import os
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()
quandl.ApiConfig.api_key = os.getenv('NASDAQ_API_KEY')
data = quandl.get("CHRIS/CME_CL1", start_date="2024-01-01", end_date="2025-10-20")
print(data.head())
print("Index name:", data.index.name)
print("Columns:", data.columns.tolist())