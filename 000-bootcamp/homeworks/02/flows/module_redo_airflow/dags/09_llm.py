from __future__ import annotations

import requests

from airflow.sdk import dag, task
from pendulum import datetime


OLLAMA_URL = "http://host.docker.internal:11434"
OLLAMA_MODEL = "qwen3:8b"

PROMPT = """
Which features were released in Kestra 1.1?
Please list at least 5 major features with brief descriptions.
"""


@dag(
  dag_id="09_chat_without_rag",
  start_date=datetime(2024, 1, 1),
  schedule=None,
  catchup=False,
  tags=["ai", "ollama", "llm"],
)
def chat_without_rag():

  @task
  def chat() -> str:
    response = requests.post(
      f"{OLLAMA_URL}/api/chat",
      json={
        "model": OLLAMA_MODEL,
        "messages": [
          {
            "role": "user",
            "content": PROMPT,
          }
        ],
        "stream": False,
        "think": True,
      },
      timeout=300,
    )

    response.raise_for_status()

    result = response.json()

    thinking = result.get("message", {}).get("thinking", "")
    answer = result["message"]["content"]

    print("=== Thinking ===")
    print(thinking)

    print("=== Response ===")
    print(answer)

    return answer

  response = chat()

  @task
  def log_results(response: str):
    print("Response without RAG:")
    print(response)

  log_results(response)


chat_without_rag()
