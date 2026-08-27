FROM python:3.13.11-slim

ARG SCRIPT
ARG FILES

RUN pip install --no-cache-dir uv

WORKDIR /app

RUN mkdir -p /app/.data

COPY uv.lock pyproject.toml .python-version ./

RUN uv sync --locked

COPY config.py common.py ./

COPY . /tmp/context/

RUN for file in $FILES; do \
      cp "/tmp/context/$file" ./; \
    done

COPY ${SCRIPT} pipeline.py

ENTRYPOINT ["uv", "run", "python", "pipeline.py"]
