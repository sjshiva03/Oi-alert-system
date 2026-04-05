import requests

TOKEN = "YOUR_TOKEN"
CHAT_ID = "YOUR_CHAT_ID"

url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"

data = {
    "chat_id": CHAT_ID,
    "text": "Working test"
}

print(requests.post(url, data=data).text)
