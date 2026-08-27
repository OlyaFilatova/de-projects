from typing import TypedDict

import requests

class Stats(TypedDict):
  downloads: int | str

def collect_stats(image_name: str) -> Stats:
  url = f"https://hub.docker.com/v2/repositories/{image_name}/"

  response = requests.get(url, timeout=30)
  response.raise_for_status()

  data = response.json()
  downloads = data.get("pull_count", "Not available")

  return {
    "downloads": downloads,
  }
