from __future__ import annotations

import requests
import duckdb

from airflow.sdk import dag, task
from pendulum import datetime


OLLAMA_URL = "http://host.docker.internal:11434"
CHAT_MODEL = "qwen3:8b"
EMBEDDING_MODEL = "embeddinggemma"

RELEASE_NOTES_URL = (
  "https://raw.githubusercontent.com/kestra-io/docs/"
  "refs/heads/main/src/contents/blogs/release-1-1/index.md"
)

DUCKDB_PATH = "/tmp/rag.duckdb"

PROMPT = """
Which features were released in Kestra 1.1?
Please list at least 5 major features with brief descriptions.
"""


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

    conn = duckdb.connect(DUCKDB_PATH)

    try:
      conn.execute("INSTALL vss")
      conn.execute("LOAD vss")

      conn.execute("DROP TABLE IF EXISTS release_notes")

      for chunk_id, chunk in enumerate(chunks):
        response = requests.post(
          f"{OLLAMA_URL}/api/embed",
          json={
            "model": EMBEDDING_MODEL,
            "input": chunk,
          },
          timeout=120,
        )
        response.raise_for_status()

        embedding = response.json()["embeddings"][0]

        if chunk_id == 0:
          dimension = len(embedding)

          conn.execute(
            f"""
            CREATE TABLE release_notes (
              id INTEGER,
              content VARCHAR,
              embedding FLOAT[{dimension}]
            )
            """
          )

        conn.execute(
          """
          INSERT INTO release_notes
          VALUES (?, ?, ?::FLOAT[])
          """,
          [chunk_id, chunk, embedding],
        )

    finally:
      conn.close()

    return len(chunks)

  @task
  def retrieve_context() -> list[str]:
    embedding_response = requests.post(
      f"{OLLAMA_URL}/api/embed",
      json={
        "model": EMBEDDING_MODEL,
        "input": PROMPT,
      },
      timeout=120,
    )

    embedding_response.raise_for_status()

    query_embedding = embedding_response.json()["embeddings"][0]

    if len(query_embedding) != 768:
      raise ValueError(
        f"Expected 768 dimensions, got {len(query_embedding)}"
      )

    conn = duckdb.connect(DUCKDB_PATH)

    try:
      rows = conn.execute(
        """
        SELECT
          content,
          array_cosine_similarity(
            embedding,
            ?::FLOAT[768]
          ) AS similarity
        FROM release_notes
        ORDER BY similarity DESC
        LIMIT 5
        """,
        [query_embedding],
      ).fetchall()

      return [row[0] for row in rows]

    finally:
      conn.close()

  @task
  def chat_with_rag(context: list[str]) -> str:
    """
    Ask the LLM using the retrieved release-note context.
    """

    context_text = "\n\n---\n\n".join(context)

    prompt = f"""
Use the following Kestra 1.1 release notes to answer the question.

Context:
{context_text}

Question:
{PROMPT}

Answer using only the provided context.
List at least 5 major features with brief descriptions.
"""

    response = requests.post(
      f"{OLLAMA_URL}/api/chat",
      json={
        "model": CHAT_MODEL,
        "messages": [
          {
            "role": "user",
            "content": prompt,
          }
        ],
        "stream": False,
        "think": True,
      },
      timeout=300,
    )

    response.raise_for_status()

    message = response.json()["message"]

    thinking = message.get("thinking", "")
    answer = message["content"]

    print("=== Thinking ===")
    print(thinking)

    print("=== Response with RAG ===")
    print(answer)

    return answer

  @task
  def log_results(response: str):
    print("Response with RAG:")
    print(response)

  ingestion = ingest_release_notes()

  context = retrieve_context.override(
    trigger_rule="all_success"
  )()

  ingestion >> context

  response = chat_with_rag(context)

  log_results(response)


chat_with_rag()
