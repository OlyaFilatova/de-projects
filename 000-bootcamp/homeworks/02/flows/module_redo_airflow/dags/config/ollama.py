OLLAMA_URL = "http://host.docker.internal:11434"

CHAT_MODEL = "qwen3:8b"
EMBEDDING_MODEL = "embeddinggemma"

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

RAG_PROMPT = """
Use the following Kestra 1.1 release notes
to answer the question.

Context:
{context_text}

Question:
{prompt}

Answer using only the provided context.

List at least 5 major features with brief
descriptions.
"""