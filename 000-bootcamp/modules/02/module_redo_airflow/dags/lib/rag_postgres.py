from __future__ import annotations

import hashlib

from airflow.providers.postgres.hooks.postgres import PostgresHook


def get_content_hash(content: str) -> str:
  return hashlib.sha256(
    content.encode("utf-8")
  ).hexdigest()


def ensure_vector_store(
  *,
  conn_id: str,
  table: str,
  dimension: int,
) -> None:

  hook = PostgresHook(
    postgres_conn_id=conn_id,
  )

  hook.run(
    "CREATE EXTENSION IF NOT EXISTS vector"
  )

  hook.run(
    f"""
    CREATE TABLE IF NOT EXISTS {table} (
      document_id TEXT NOT NULL,
      chunk_id INTEGER NOT NULL,
      content TEXT NOT NULL,
      embedding vector({dimension}) NOT NULL,
      source_url TEXT NOT NULL,
      content_hash TEXT NOT NULL,

      PRIMARY KEY (
        document_id,
        chunk_id
      )
    )
    """
  )


def upsert_chunks(
  *,
  conn_id: str,
  table: str,
  document_id: str,
  source_url: str,
  chunks: list[str],
  embeddings: list[list[float]],
) -> None:

  if not chunks:
    raise ValueError("No chunks provided")

  if len(chunks) != len(embeddings):
    raise ValueError(
      "Chunks and embeddings must have the same length"
    )

  dimension = len(embeddings[0])

  ensure_vector_store(
    conn_id=conn_id,
    table=table,
    dimension=dimension,
  )

  hook = PostgresHook(
    postgres_conn_id=conn_id,
  )

  conn = hook.get_conn()

  try:
    with conn.cursor() as cursor:

      for chunk_id, (chunk, embedding) in enumerate(
        zip(chunks, embeddings)
      ):
        cursor.execute(
          f"""
          INSERT INTO {table} (
            document_id,
            chunk_id,
            content,
            embedding,
            source_url,
            content_hash
          )
          VALUES (
            %s,
            %s,
            %s,
            %s::vector,
            %s,
            %s
          )
          ON CONFLICT (
            document_id,
            chunk_id
          )
          DO UPDATE SET
            content = EXCLUDED.content,
            embedding = EXCLUDED.embedding,
            source_url = EXCLUDED.source_url,
            content_hash = EXCLUDED.content_hash
          """,
          (
            document_id,
            chunk_id,
            chunk,
            str(embedding),
            source_url,
            get_content_hash(chunk),
          ),
        )

    conn.commit()

  finally:
    conn.close()


def retrieve(
  *,
  conn_id: str,
  table: str,
  query_embedding: list[float],
  limit: int = 5,
) -> list[str]:

  hook = PostgresHook(
    postgres_conn_id=conn_id,
  )

  embedding = str(query_embedding)

  rows = hook.get_records(
    f"""
    SELECT
      content,
      1 - (
        embedding <=> %s::vector
      ) AS similarity
    FROM {table}
    ORDER BY embedding <=> %s::vector
    LIMIT %s
    """,
    parameters=(
      embedding,
      embedding,
      limit,
    ),
  )

  return [
    content
    for content, similarity in rows
  ]
