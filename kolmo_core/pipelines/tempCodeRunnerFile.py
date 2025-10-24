# kolmo_core/pipelines/run_predictive.py
import argparse
import duckdb

from kolmo_core.agents.predictive_agent import _connect, predict_for_symbol

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--symbol", required=True, help="e.g., BRENT")
    parser.add_argument("--horizon", type=int, default=5)
    args = parser.parse_args()

    con: duckdb.DuckDBPyConnection = _connect()
    predict_for_symbol(con, args.symbol, horizon=args.horizon)
    print(f"[run_predictive] OK symbol={args.symbol} horizon={args.horizon}")

if __name__ == "__main__":
    main()
