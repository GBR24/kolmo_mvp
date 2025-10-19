from os import getenv

CONFIG = {
    "db": {
        "url": getenv("DB_URL", "duckdb:///kolmo_core/data/kolmo.duckdb")
    },
    "market": {
        # You can switch provider per product later (e.g., "eia")
        "symbols": {
            # Futures via Yahoo Finance (liquid + easy)
            "BRN":   {"name": "Brent Crude",            "provider": "yahoo", "id": "BZ=F", "asset": "crude"},
            "WTI":   {"name": "WTI Crude",              "provider": "yahoo", "id": "CL=F", "asset": "crude"},
            "RBOB":  {"name": "RBOB Gasoline",          "provider": "yahoo", "id": "RB=F", "asset": "gasoline"},
            "HO":    {"name": "Heating Oil (ICE Gasoil proxy)", "provider": "yahoo", "id": "HO=F", "asset": "gasoil"},
            "NG":    {"name": "Henry Hub Natural Gas",  "provider": "yahoo", "id": "NG=F", "asset": "natgas"},
            # Jet fuel doesn’t have a liquid public ticker; use Heating Oil as a proxy,
            # or later replace with EIA series if you have the exact series id.
            "JET":   {"name": "Jet Fuel (proxy = HO)",  "provider": "proxy", "id": "HO=F", "asset": "jet"},
        }
    },
    "news": {
        "default_queries": ["oil", "brent", "wti", "gasoline", "diesel", "gasoil", "natural gas", "opec", "refinery", "jet fuel"],
        "max_per_query": 25,
    }
}
