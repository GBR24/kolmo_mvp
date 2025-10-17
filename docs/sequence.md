sequenceDiagram
    participant UI as UI (Ask Kolmo)
    participant MCP as Kolmo MCP Controller
    participant RAG as RAG Context Builder
    participant A1 as Market Data Agent
    participant A2 as Forecast Agent
    participant A3 as Insight Agent
    participant DB as DuckDB/SQLite
    participant IDX as RAG Index

    UI->>MCP: "What moved Brent today? 1d forecast?"
    MCP->>RAG: Build context (symbols=Brent/WTI/NG, horizon=1d)
    RAG->>A1: fetch_latest(Brent, WTI, NG, FX)
    A1->>DB: upsert(prices)
    RAG->>A3: retrieve_news(Brent last 24h)
    A3->>IDX: vector_search("Brent drivers last 24h")
    A3->>DB: store(summary, citations)

    MCP->>A2: forecast(Brent, horizon=1d, model=GBM/ARIMA)
    A2->>DB: store(predictions, confidence)

    MCP->>DB: read(prices, predictions, news)
    MCP-->>UI: structured JSON (prices + 1d forecast + news summary + sources)
