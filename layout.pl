kolmo/
  apps/
    ui/                   # Streamlit or React (Day 5)
  kolmo_core/
    __init__.py
    config.py
    orchestrator.py       # MCP controller
    schemas/
      market_tick.json
      forecast_output.json
      insight_summary.json
    rag/
      build_index.py
      retriever.py
      chunker.py
      store/
        corpus/           # raw txt/jsonl/articles
        index/            # FAISS/LanceDB files
    agents/
      market_data_agent.py
      forecast_agent.py
      insight_agent.py
    data/
      duckdb.db
      cache/
  mcp_server/
    server.py             # exposes tools per MCP
    tools/
      fetch_market_data.py
      run_forecast.py
      retrieve_news.py
  scripts/
    bootstrap_day1.sh
  configs/
    kolmo.yaml
  .env.example
  README.md
