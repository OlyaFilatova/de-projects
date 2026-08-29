FROM apache/airflow:3.3.1

USER airflow

RUN pip install --no-cache-dir \
    apache-airflow-providers-fab \
    uv

COPY pyproject.toml uv.lock .python-version ./

RUN uv export --locked --no-dev --format requirements.txt \
    --output-file /tmp/requirements.txt

RUN pip install --no-cache-dir \
    -r /tmp/requirements.txt
