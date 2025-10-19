import requests, pandas as pd
from datetime import datetime, timezone

def fetch_news(query: str, api_key: str, page_size: int = 25) -> pd.DataFrame:
    """
    Returns columns: ['id','headline','description','url','published_at','source','keywords']
    """
    url = "https://newsapi.org/v2/everything"
    params = {
        "q": query,
        "language": "en",
        "pageSize": page_size,
        "sortBy": "publishedAt",
        "apiKey": api_key
    }
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    articles = r.json().get("articles", [])
    rows = []
    for a in articles:
        src = (a.get("source") or {}).get("name")
        published = a.get("publishedAt")
        rows.append({
            "id": a.get("url"),  # use URL as stable-ish id
            "headline": a.get("title"),
            "description": a.get("description"),
            "url": a.get("url"),
            "published_at": published,
            "source": src,
            "keywords": query
        })
    return pd.DataFrame(rows)
