
import duckdb
con = duckdb.connect("kolmo_core/data/kolmo.duckdb")
print(con.execute("SELECT DISTINCT symbol FROM prices ORDER BY 1").fetchdf())
print(con.execute("SELECT COUNT(*) AS rows FROM prices").fetchdf())
