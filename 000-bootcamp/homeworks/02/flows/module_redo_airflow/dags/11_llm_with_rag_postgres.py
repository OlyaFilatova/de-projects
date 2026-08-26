from __future__ import annotations

import requests

from airflow.sdk import dag, task
from pendulum import datetime

from lib.ollama import chat, embed
from lib.rag_postgres import upsert_chunks, retrieve


OLLAMA_URL = "http://host.docker.internal:11434"

CHAT_MODEL = "qwen3:8b"
EMBEDDING_MODEL = "embeddinggemma"

POSTGRES_CONN_ID = "homework_postgres"

VECTOR_TABLE = "release_notes"

DOCUMENT_ID = "kestra-1.1-release-notes"

RELEASE_NOTES_URL = (
  "https://raw.githubusercontent.com/kestra-io/docs/"
  "refs/heads/main/src/contents/blogs/release-1-1/index.md"
)

PROMPT = """
Which features were released in Kestra 1.1?
Please list at least 5 major features with brief descriptions.
"""


@dag(
  dag_id="11_chat_with_rag_postgres",
  start_date=datetime(2024, 1, 1),
  schedule=None,
  catchup=False,
  tags=[
    "ai",
    "rag",
    "ollama",
    "postgres",
    "pgvector",
  ],
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
      for i in range(
        0,
        len(document),
        1500,
      )
    ]

    embeddings = [
      embed(
        url=OLLAMA_URL,
        model=EMBEDDING_MODEL,
        text=chunk,
      )
      for chunk in chunks
    ]

    upsert_chunks(
      conn_id=POSTGRES_CONN_ID,
      table=VECTOR_TABLE,
      document_id=DOCUMENT_ID,
      source_url=RELEASE_NOTES_URL,
      chunks=chunks,
      embeddings=embeddings,
    )

    print(
      f"Ingested {len(chunks)} chunks"
    )

    return len(chunks)

  @task
  def retrieve_context() -> list[str]:

    query_embedding = embed(
      url=OLLAMA_URL,
      model=EMBEDDING_MODEL,
      text=PROMPT,
    )

    context = retrieve(
      conn_id=POSTGRES_CONN_ID,
      table=VECTOR_TABLE,
      query_embedding=query_embedding,
      limit=5,
    )

    print(
      f"Retrieved {len(context)} chunks"
    )

    return context

  @task
  def query_llm(
    context: list[str],
  ) -> str:

    context_text = "\n\n---\n\n".join(
      context
    )

    prompt = f"""
Use the following Kestra 1.1 release notes
to answer the question.

Context:
{context_text}

Question:
{PROMPT}

Answer using only the provided context.

List at least 5 major features with brief
descriptions.
"""

    return chat(
      url=OLLAMA_URL,
      model=CHAT_MODEL,
      prompt=prompt,
      think=True,
    )

  @task
  def log_results(
      response: str,
  ) -> None:

    print(
      "Response with RAG:"
    )

    print(response)

  ingestion = ingest_release_notes()

  context = retrieve_context()

  ingestion >> context

  response = query_llm(context)

  log_results(response)


chat_with_rag()
