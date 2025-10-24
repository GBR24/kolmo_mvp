# apps/ui/app.py

# 1) Streamlit FIRST — config must be the first Streamlit command
import streamlit as st
st.set_page_config(page_title="Kolmo · Prices vs Predictions", layout="wide")

# 2) Then the rest of imports (none of these should call st.* at import time)
import sys, os
from pathlib import Path
import duckdb
import pandas as pd

# Make project root importable: /.../kolmo/database
ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# Import UI blocks only AFTER set_page_config
from apps.ui.blocks.predictions_panel import predictions_panel

# --- Agent orchestration imports ---
from apps.agents.supervisor import handle as supervisor_handle
from apps.agents.registry import TOOL_REGISTRY
from apps.agents.blackboard import Blackboard

# --- Paths ---
DB = ROOT / "kolmo_core" / "data" / "kolmo.duckdb"

# Header
st.title("🧠 Kolmo — Prices vs Predictions")
st.caption(f"DB: {DB}")

# --- DB helpers ---
def connect():
    return duckdb.connect(str(DB), read_only=False)

def table_exists(con, table: str) -> bool:
    return bool(con.execute(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name=?", [table]
    ).fetchone()[0])

def table_has_column(con, table: str, col: str) -> bool:
    return bool(con.execute(
        "SELECT COUNT(*) FROM information_schema.columns WHERE table_name=? AND column_name=?",
        [table, col]
    ).fetchone()[0])

def ts_col_for(con, table: str) -> str:
    if table_has_column(con, table, "ts"):   return "ts"
    if table_has_column(con, table, "date"): return "date"
    raise RuntimeError(f"Table '{table}' has neither 'ts' nor 'date'")

def run_query(q: str, params=None) -> pd.DataFrame:
    con = connect()
    try:
        return con.execute(q, params or []).fetchdf()
    finally:
        con.close()

# --- Build views (schema-aware, safe to re-run) ---
con = connect()
try:
    PR_TS = ts_col_for(con, "prices")
    con.execute(f"""
        CREATE OR REPLACE VIEW prices_latest AS
        WITH latest AS (
          SELECT symbol, MAX({PR_TS}) AS max_ts
          FROM prices
          GROUP BY symbol
        )
        SELECT p.*
        FROM prices p
        JOIN latest l ON p.symbol = l.symbol AND p.{PR_TS} = l.max_ts
    """)

    if table_exists(con, "predictions"):
        PRED_TS = ts_col_for(con, "predictions")
        has_yhat   = table_has_column(con, "predictions", "yhat")
        has_y_hat  = table_has_column(con, "predictions", "y_hat")
        has_method = table_has_column(con, "predictions", "method")

        if has_yhat or has_y_hat:
            yexpr = "yhat" if has_yhat else "y_hat"
            con.execute(f"""
                CREATE OR REPLACE VIEW predictions_latest AS
                WITH latest AS (
                  SELECT symbol{', method' if has_method else ''}, MAX({PRED_TS}) AS max_ts
                  FROM predictions
                  GROUP BY symbol{', method' if has_method else ''}
                )
                SELECT
                  pr.symbol,
                  {f'pr.method,' if has_method else 'CAST(NULL AS VARCHAR) AS method,'}
                  pr.{PRED_TS} AS ts,
                  pr.{yexpr}   AS yhat_std
                FROM predictions pr
                JOIN latest l
                  ON pr.symbol = l.symbol
                 AND pr.{PRED_TS} = l.max_ts
                 {f'AND pr.method = l.method' if has_method else ''}
            """)
        else:
            con.execute("""
                CREATE OR REPLACE VIEW predictions_latest AS
                SELECT NULL::VARCHAR AS symbol,
                       NULL::VARCHAR AS method,
                       NULL::TIMESTAMP AS ts,
                       NULL::DOUBLE AS yhat_std
                WHERE false
            """)
    else:
        con.execute("""
            CREATE OR REPLACE VIEW predictions_latest AS
            SELECT NULL::VARCHAR AS symbol,
                   NULL::VARCHAR AS method,
                   NULL::TIMESTAMP AS ts,
                   NULL::DOUBLE AS yhat_std
            WHERE false
        """)
finally:
    con.close()

# --- Panel (table of recent predictions) ---
predictions_panel()

# --- Summary table ---
QUERY = """
WITH syms AS (SELECT DISTINCT symbol FROM prices)
SELECT
  s.symbol,
  p.price AS last_price,
  pr.yhat_std AS pred_1d,
  pr.method
FROM syms s
LEFT JOIN prices_latest      p  USING(symbol)
LEFT JOIN predictions_latest pr USING(symbol)
ORDER BY s.symbol, pr.method
"""

try:
    df = run_query(QUERY)
    st.dataframe(df, use_container_width=True)
except Exception as e:
    st.error(f"Query failed: {e}")
    st.stop()

# --- Symbol picker + 60-day chart (unique key to avoid collision) ---
symbols = sorted(df["symbol"].dropna().unique().tolist())
if symbols:
    sym = st.selectbox("Symbol", symbols, index=0, key="chart_symbol")
    hist = run_query(f"""
        SELECT {PR_TS} AS ts, price
        FROM prices
        WHERE symbol = ?
          AND {PR_TS} >= now() - INTERVAL 60 DAY
        ORDER BY ts
    """, [sym])

    if hist.empty:
        st.info("No history for selected symbol in the last 60 days.")
    else:
        # Ensure proper dtypes and ordering for the chart
        hist["ts"] = pd.to_datetime(hist["ts"], errors="coerce")
        hist = hist.dropna(subset=["ts"]).sort_values("ts")
        hist["price"] = pd.to_numeric(hist["price"], errors="coerce")
        hist = hist.dropna(subset=["price"])
        st.line_chart(hist.set_index("ts")["price"], height=260)
else:
    st.info("No symbols found in prices yet.")

st.markdown("---")

# =========================
# Chat-style agent section
# =========================
st.subheader("💬 Ask Kolmo (multi-agent)")
user_prompt = st.chat_input("Ask for an analysis, e.g. 'Forecast & news for BRENT'")

if user_prompt:
    bb = Blackboard()
    result = supervisor_handle(user_prompt, tools=TOOL_REGISTRY, blackboard=bb)

    if not result.get("ok"):
        st.error("Supervisor failed to produce a report.")
    else:
        md_path = (result.get("report") or {}).get("md_path")
        if md_path and Path(md_path).exists():
            st.success(f"Report generated: {md_path}")
            with open(md_path, "r", encoding="utf-8") as f:
                st.markdown(f.read())
            with open(md_path, "rb") as fh:
                st.download_button(
                    "Download report (Markdown)",
                    data=fh.read(),
                    file_name=Path(md_path).name,
                    mime="text/markdown",
                )

        # Transparency: plan + confidence + citations
        with st.expander("Agent plan & audit"):
            st.json({"plan": result.get("plan"), "confidence": result.get("confidence")})
        citations = result.get("citations") or []
        if citations:
            st.markdown("**Citations:**")
            for c in citations:
                title = c.get("title","(untitled)")
                link = c.get("link","")
                source = c.get("source","")
                if link:
                    st.markdown(f"- [{title}]({link}) — _{source}_")
                else:
                    st.markdown(f"- {title} — _{source}_")
