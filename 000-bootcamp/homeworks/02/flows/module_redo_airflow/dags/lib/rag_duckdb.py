from __future__ import annotations

import duckdb


def create_vector_store(
  *,
  db_path: str,
  table: str,
  documents: list[str],
  embeddings: list[list[float]],
) -> None:

  if not embeddings:
    raise ValueError("No embeddings provided")

  dimension = len(embeddings[0])

  conn = duckdb.connect(db_path)

  try:
    conn.execute("INSTALL vss")
    conn.execute("LOAD vss")

    conn.execute(f"DROP TABLE IF EXISTS {table}")

    conn.execute(
      f"""
      CREATE TABLE {table} (
        id INTEGER,
        content VARCHAR,
        embedding FLOAT[{dimension}]
      )
      """
    )

    for document_id, (document, embedding) in enumerate(
      zip(documents, embeddings)
    ):
      conn.execute(
        f"""
        INSERT INTO {table}
        VALUES (?, ?, ?::FLOAT[{dimension}])
        """,
        [
          document_id,
          document,
          embedding,
        ],
      )

  finally:
    conn.close()


def retrieve(
  *,
  db_path: str,
  table: str,
  query_embedding: list[float],
  limit: int = 5,
) -> list[str]:

  dimension = len(query_embedding)

  conn = duckdb.connect(db_path)

  try:
    rows = conn.execute(
      f"""
      SELECT
        content,
        array_cosine_similarity(
          embedding,
          ?::FLOAT[{dimension}]
        ) AS similarity
      FROM {table}
      ORDER BY similarity DESC
      LIMIT {limit}
      """,
      [query_embedding],
    ).fetchall()

    return [row[0] for row in rows]

  finally:
    conn.close()
