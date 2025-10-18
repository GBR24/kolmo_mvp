from fastapi import FastAPI
from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime

app = FastAPI(title="Kolmo MCP Tools")

class MarketTick(BaseModel):
    symbol: str
    timestamp: str
    price: float
    source: Optional[str] = None

class ForecastPoint(BaseModel):
    t: str
    value: float
    ci_low: Optional[float] = None
    ci_high: Optional[float] = None

class ForecastOutput(BaseModel):
    symbol: str
    as_of: str
    horizon: str
    method: str
    predictions: List[ForecastPoint]

class InsightCitation(BaseModel):
    title: str
    url: str
    published_at: Optional[str] = None

class InsightSummary(BaseModel):
    topic: str
    as_of: str
    bullets: List[str]
    citations: List[InsightCitation]

@app.get("/health")
def health():
    return {"ok": True, "ts": datetime.utcnow().isoformat() + "Z"}

@app.post("/tools/fetch_market_data", response_model=List[MarketTick])
def fetch_market_data(symbols: List[str], lookback: str = "1d"):
    # TODO Day-2: call kolmo_core.agents.market_data_agent
    return []

@app.post("/tools/run_forecast", response_model=ForecastOutput)
def run_forecast(symbol: str, horizon: str = "1d", method: str = "GBM"):
    # TODO Day-3: call kolmo_core.agents.forecast_agent
    return ForecastOutput(symbol=symbol, as_of=datetime.utcnow().isoformat()+"Z",
                          horizon=horizon, method=method, predictions=[])

@app.post("/tools/retrieve_news", response_model=InsightSummary)
def retrieve_news(topic: str, window: str = "24h"):
    # TODO Day-3/4: call kolmo_core.agents.insight_agent
    return InsightSummary(topic=topic, as_of=datetime.utcnow().isoformat()+"Z",
                          bullets=[], citations=[])