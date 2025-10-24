# apps/ui/predictions_panel.py
import os
from pathlib import Path

import altair as alt
import duckdb
import pandas as pd
import streamlit as st


def _resolve_db_path() -> str:
    db_url = os.getenv("DB_URL", "duckdb:///kolmo_core/data/kolmo.duckdb")
    if db_url.startswith("duckdb:///"):
        db_path = db_url.replace("duckdb:///", "")
    else:
        db_path = db_url
    return str(Path(db_path).resolve())


def predictions_panel():
    con = duckdb.connect(_resolve_db_path())

    st.subheader("Forecasts")

    symbols = con.execute("SELECT DISTINCT symbol FROM prices ORDER BY symbol").df()["symbol"].tolist()
    if not symbols:
        st.info("No symbols found in prices.")
        return

    col1, col2, col3 = st.columns([1,1,1])
    with col1:
        symbol = st.selectbox("Symbol", symbols, index=0)
    with col2:
        horizon = st.number_input("Horizon (days)", min_value=1, max_value=30, value=5)
    with col3:
        use_best = st.checkbox("Use best method (by RMSE)", value=True)

    # Pull last 60 actuals
    actuals = con.execute("""
        SELECT ts, price
        FROM prices
        WHERE symbol = ?
        ORDER BY ts DESC
        LIMIT 60
    """, [symbol]).df().sort_values("ts")

    if actuals.empty:
        st.warning(f"No prices for {symbol}.")
        return

    # Determine method
    method = None
    if use_best:
        df_best = con.execute("""
            SELECT best_method
            FROM model_selection
            WHERE symbol = ?
            ORDER BY asof_ts DESC
            LIMIT 1
        """, [symbol]).df()
        if not df_best.empty:
            method = df_best["best_method"].iloc[0]

    # fallback to dropdown
    methods = con.execute("""
        SELECT DISTINCT method
        FROM predictions
        WHERE symbol = ?
        ORDER BY method
    """, [symbol]).df()["method"].tolist()
    if not methods:
        st.info("No predictions yet. Run the predictive pipeline first.")
        return

    if not method or method not in methods:
        method = st.selectbox("Method", methods, index=0)
    else:
        st.caption(f"Selected best method: **{method}**")

    # Get the latest asof_ts for that method & symbol
    df_asof = con.execute("""
        SELECT max(asof_ts) AS asof_ts
        FROM predictions
        WHERE symbol = ? AND method = ?
    """, [symbol, method]).df()

    if df_asof.empty or pd.isna(df_asof["asof_ts"].iloc[0]):
        st.info("No forecasts found for selection.")
        return

    asof = df_asof["asof_ts"].iloc[0]

    preds = con.execute("""
        SELECT ts, horizon, yhat, yhat_lower, yhat_upper
        FROM predictions
        WHERE symbol = ? AND method = ? AND asof_ts = ?
        ORDER BY horizon
    """, [symbol, method, asof]).df()

    if preds.empty:
        st.info("No forecasts found for selection.")
        return

    preds = preds.head(horizon)

    # Build combined dataframe for Altair
    # Build combined dataframe for Altair
    actuals_df = actuals.rename(columns={"price": "value"}).assign(series="Actual")
    fc_df = preds.rename(columns={"yhat": "value"}).assign(series="Forecast")[["ts", "value", "series"]]
    band_df = preds[["ts", "yhat_lower", "yhat_upper"]]
    lines_df = pd.concat([actuals_df, fc_df], ignore_index=True)

# Confidence band (forecast only)
    band = (
        alt.Chart(band_df)
        .mark_area(opacity=0.2)
        .encode(
            x=alt.X("ts:T", title="Date"),
            y=alt.Y("yhat_lower:Q", title=f"{symbol} price"),
            y2="yhat_upper:Q",
            tooltip=["ts:T", "yhat_lower:Q", "yhat_upper:Q"],
        )
    )

    # Actual line
    actual_line = (
        alt.Chart(lines_df)
        .transform_filter(alt.datum.series == "Actual")
        .mark_line()
        .encode(
            x="ts:T",
            y="value:Q",
            color=alt.value("#4c78a8"),
        )
    )

    # Forecast line
    forecast_line = (
        alt.Chart(lines_df)
        .transform_filter(alt.datum.series == "Forecast")
        .mark_line(strokeDash=[5, 3])
        .encode(
            x="ts:T",
            y="value:Q",
            color=alt.value("#f58518"),
        )
    )

    title_txt = f"{symbol} — {method} (as of {pd.to_datetime(asof).strftime('%Y-%m-%d %H:%M UTC')})"
    comb = alt.layer(band, actual_line, forecast_line).properties(title=title_txt).resolve_scale(y="shared")

    st.altair_chart(comb, use_container_width=True)

    # Tabular preview
    with st.expander("Forecast table"):
        st.dataframe(preds, use_container_width=True)

