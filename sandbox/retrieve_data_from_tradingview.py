import requests

# TradingView API endpoint and query parameters
url = "https://api.tradingview.com/v1/comments/{symbol}"
querystring = {"symbol": "SPX500USD", "sort_dir": "desc", "limit": "100"}

# TradingView API key
headers = {"X-TV-API-KEY": "YOUR_API_KEY_HERE"}

# Send GET request to TradingView API to retrieve comments
response = requests.request("GET", url, headers=headers, params=querystring)