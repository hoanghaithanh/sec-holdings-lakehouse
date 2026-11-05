# One image for every role: Dagster webserver, Dagster daemon, Spark master and Spark workers.
# The same Python, PySpark and Delta versions run everywhere, so dev and UAT stay in parity.
FROM python:3.11-slim-bookworm

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps tini \
    && apt-get clean

RUN pip install --no-cache-dir uv==0.11.32

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    VIRTUAL_ENV=/app/.venv \
    PATH=/app/.venv/bin:$PATH \
    PYSPARK_PYTHON=/app/.venv/bin/python \
    DAGSTER_HOME=/opt/dagster/home

WORKDIR /app

# Dependencies first, for layer caching.
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project

COPY src ./src
COPY README.md ./
RUN uv sync --frozen --no-dev

# Bake the Delta and S3A jars into the image so containers never download them at start-up.
RUN python -c "from sec_lakehouse.resources import SparkResource as R; \
R(master='local[1]', s3_endpoint='http://unused:9000').get_session().stop()"

COPY docker/dagster.yaml docker/workspace.yaml ${DAGSTER_HOME}/

ENTRYPOINT ["tini", "--"]
CMD ["dagster-webserver", "-h", "0.0.0.0", "-p", "3000", "-w", "/opt/dagster/home/workspace.yaml"]
