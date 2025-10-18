import requests

API = "http://127.0.0.1:8000"

def call_tool(tool: str, **params):
    url = f"{API}/tools/{tool}"
    resp = requests.post(url, params=params)
    resp.raise_for_status()
    return resp.json()
