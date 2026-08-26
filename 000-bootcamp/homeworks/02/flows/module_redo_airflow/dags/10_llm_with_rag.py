from __future__ import annotations

import requests

from airflow.sdk import dag, task
from pendulum import datetime

from config.ollama import OLLAMA_URL, CHAT_MODEL, EMBEDDING_MODEL, RAG_PROMPT, VECTOR_TABLE, RELEASE_NOTES_URL, PROMPT

from lib.ollama import chat, embed
from lib.rag_duckdb import create_vector_store, retrieve


DUCKDB_PATH = "/tmp/rag.duckdb"


@dag(
  dag_id="10_chat_with_rag",
  start_date=datetime(2024, 1, 1),
  schedule=None,
  catchup=False,
  tags=["ai", "rag", "ollama", "duckdb"],
)
def chat_with_rag():

  @task
  def ingest_release_notes() -> int:

    response = requests.get(
      RELEASE_NOTES_URL,
      timeout=30,
    )
    response.raise_for_status()

    document = response.text

    chunks = [
      document[i:i + 1500]
      for i in range(0, len(document), 1500)
    ]

    embeddings = [
      embed(
        url=OLLAMA_URL,
        model=EMBEDDING_MODEL,
        text=chunk,
      )
      for chunk in chunks
    ]

    create_vector_store(
      db_path=DUCKDB_PATH,
      table=VECTOR_TABLE,
      documents=chunks,
      embeddings=embeddings,
    )

    return len(chunks)

  @task
  def retrieve_context() -> list[str]:

    query_embedding = embed(
      url=OLLAMA_URL,
      model=EMBEDDING_MODEL,
      text=PROMPT,
    )

    return retrieve(
      db_path=DUCKDB_PATH,
      table=VECTOR_TABLE,
      query_embedding=query_embedding,
      limit=5,
    )

  @task
  def query_llm(context: list[str]) -> str:

    context_text = "\n\n---\n\n".join(context)

    prompt = RAG_PROMPT.format(
      context_text=context_text,
      prompt=PROMPT
    )

    return chat(
      url=OLLAMA_URL,
      model=CHAT_MODEL,
      prompt=prompt,
      think=True,
    )

  @task
  def log_results(response: str):
    print("Response with RAG:")
    print(response)

  ingestion = ingest_release_notes()

  context = retrieve_context()

  ingestion >> context

  response = query_llm(context)

  log_results(response)


chat_with_rag()
