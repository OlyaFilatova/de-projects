from __future__ import annotations

import requests


def embed(
  *,
  url: str,
  model: str,
  text: str,
  timeout: int = 120,
) -> list[float]:

  response = requests.post(
    f"{url}/api/embed",
    json={
      "model": model,
      "input": text,
    },
    timeout=timeout,
  )

  response.raise_for_status()

  return response.json()["embeddings"][0]


def chat(
  *,
  url: str,
  model: str,
  prompt: str,
  think: bool = True,
  timeout: int = 300,
) -> str:

  response = requests.post(
    f"{url}/api/chat",
    json={
      "model": model,
      "messages": [
        {
          "role": "user",
          "content": prompt,
        }
      ],
      "stream": False,
      "think": think,
    },
    timeout=timeout,
  )

  response.raise_for_status()

  message = response.json()["message"]

  thinking = message.get("thinking", "")
  answer = message["content"]

  print("=== Thinking ===")
  print(thinking)

  print("=== Response ===")
  print(answer)

  return answer
