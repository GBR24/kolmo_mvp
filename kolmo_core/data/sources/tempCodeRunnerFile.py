import kolmo_core.data.sources.eia
import os
from dotenv import load_dotenv

load_dotenv()
df = fetch_eia_series("PET.RWTC.D", os.getenv("EIA_API_KEY"))
print(df.head())