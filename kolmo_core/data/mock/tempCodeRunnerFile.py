import duckdb
con = duckdb.connect("kolmo_core/data/kolmo.duckdb")
print(con.execute("SELECT DISTINCT symbol FROM prices").fetchdf())
