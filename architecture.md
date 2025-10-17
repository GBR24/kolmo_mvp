flowchart LR
    subgraph UI["MVP UI (Streamlit/React)"]
      Chat["Ask Kolmo (chat)"]
      DashPrices["Realtime Prices"]
      DashForecast["Predicted Curve"]
      DashNews["News/RAG Panel"]
    end

    subgraph Orchestrator["Kolmo MCP Controller"]
      Router["Task Router"]
      Ctx["RAG Context Builder"]
      Schema["Schema Validation (Pydantic/JSON Schema)"]
      Log["Obs/Logs"]
    end

    subgraph Agents["Kolmo Agents"]
      A1["Market Data Agent"]
      A2["Forecast Agent"]
      A3["Insight Agent (RAG)"]
    end

    subgraph Data["Data Layer"]
      Duck["DuckDB/SQLite"]
      Cache["Local Cache"]
      Index["RAG Index (FAISS/LanceDB)"]
      Corpus["News/Docs Corpus"]
    end

    subgraph Ext["External APIs"]
      Px["Yahoo/Brent/WTI/NG"]
      FX["FX (USD/EUR)"]
      News["News API / RSS"]
      Fund["EIA/Fundamentals"]
    end

    Chat -- query/intent --> Router
    Router --> Ctx
    Ctx --> A1
    Ctx --> A2
    Ctx --> A3

    A1 -- prices, features --> Duck
    A3 -- retrieved passages --> Index
    A3 -- summaries --> Duck
    A2 -- forecasts/risks --> Duck

    Duck --> DashPrices
    Duck --> DashForecast
    Duck --> DashNews
    Chat <--> Orchestrator

    A1 <-- pulls --> Px & FX
    A3 <-- fetches --> News & Corpus
    A2 -. optional fundamentals .-> Fund

    Index <-- built from --> Corpus
    Cache <-- shared fetch cache --> A1
    Log -.-> UI
