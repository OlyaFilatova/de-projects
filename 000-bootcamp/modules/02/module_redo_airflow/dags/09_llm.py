from airflow.sdk import dag, task
from pendulum import datetime

from config.ollama import OLLAMA_URL, CHAT_MODEL, PROMPT

from lib.ollama import chat


@dag(
  dag_id="09_chat_without_rag",
  start_date=datetime(2024, 1, 1),
  schedule=None,
  catchup=False,
  tags=["ai", "ollama", "llm"],
)
def chat_without_rag():

  @task
  def chat_task() -> str:
    return chat(
      url=OLLAMA_URL,
      model=CHAT_MODEL,
      prompt=PROMPT,
      think=True,
    )

  @task
  def log_results(response: str):
    print("Response without RAG:")
    print(response)

  response = chat_task()

  log_results(response)


chat_without_rag()
