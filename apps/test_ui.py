import duckdb, pandas as pd, streamlit as st

DB = "kolmo_core/data/kolmo.duckdb"
con = duckdb.connect(DB)

st.title("Kolmo · Prices & Predictions (Smoke Test)")

prices = con.execute("SELECT date, symbol, price FROM prices").fetchdf()
preds  = con.execute("SELECT date, symbol, method, y_hat, y_last FROM predictions").fetchdf()

st.subheader("Latest predictions")
latest = con.execute("""
  WITH latest AS (
    SELECT symbol, method, MAX(date) AS maxd FROM predictions GROUP BY 1,2
  )
  SELECT p.symbol, p.method, p.date, p.y_hat, p.y_last
  FROM predictions p JOIN latest l
    ON p.symbol=l.symbol AND p.method=l.method AND p.date=l.maxd
  ORDER BY p.symbol, p.method
""").fetchdf()
st.dataframe(latest, use_container_width=True)

sym = st.selectbox("Symbol", sorted(prices["symbol"].unique()))
dfp = prices[prices.symbol==sym].sort_values("date")
st.line_chart(dfp.set_index("date")["price"], height=260)
