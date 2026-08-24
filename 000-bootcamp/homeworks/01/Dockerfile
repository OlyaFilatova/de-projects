FROM python:3.13.11-slim

ARG SCRIPT
ARG FILES

RUN pip install --no-cache-dir uv

WORKDIR /app

COPY uv.lock pyproject.toml .python-version ./

RUN uv sync --locked

COPY config.py common.py ./

COPY ${FILES} ./

COPY ${SCRIPT} pipeline.py

ENTRYPOINT ["uv", "run", "python", "pipeline.py"]
