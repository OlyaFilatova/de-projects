import requests

def load_json(url: str):
  response = requests.get(
    url,
    timeout=30,
  )
  response.raise_for_status()

  return response.json()
