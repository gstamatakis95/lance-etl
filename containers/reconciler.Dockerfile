ARG UV_IMAGE=ghcr.io/astral-sh/uv:0.9.8@sha256:08f409e1d53e77dfb5b65c788491f8ca70fe1d2d459f41c89afa2fcbef998abe
ARG JRE_IMAGE=eclipse-temurin:17.0.19_10-jre-noble@sha256:543aebd60ff1deb9e906a8d4b117a7eda68a7f8e0d71041db2b5839d7fa057b8

FROM ${UV_IMAGE} AS uv

FROM ${JRE_IMAGE} AS runtime

ARG GIT_REVISION
LABEL org.opencontainers.image.source="https://github.com/gstamatakis95/lance-etl" \
      org.opencontainers.image.revision="${GIT_REVISION}" \
      org.opencontainers.image.version="${GIT_REVISION}" \
      io.lance-etl.pylance.version="8.0.0" \
      io.lance-etl.python.version="3.14.0" \
      io.lance-etl.java.version="17.0.19+10" \
      io.lance-etl.spark.version="4.0.1" \
      io.lance-etl.iceberg.version="1.10.0" \
      io.lance-etl.iceberg.sha256="0480f1248e0a8b50ae2a730d7ad3e1a727351c362ca63f4a0c35182087a49323"
COPY --from=uv /uv /uvx /usr/local/bin/
WORKDIR /opt/lance-etl
COPY pyproject.toml uv.lock README.md ./
COPY src ./src
COPY alembic.ini ./
COPY migrations ./migrations
COPY deploy/reconciler ./deploy/reconciler
ADD --checksum=sha256:0480f1248e0a8b50ae2a730d7ad3e1a727351c362ca63f4a0c35182087a49323 \
    https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-4.0_2.13/1.10.0/iceberg-spark-runtime-4.0_2.13-1.10.0.jar \
    /opt/iceberg/iceberg-spark-runtime-4.0_2.13-1.10.0.jar
ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_INSTALL_DIR=/opt/python \
    UV_PROJECT_ENVIRONMENT=/opt/lance-etl/.venv
RUN test -n "${GIT_REVISION}" \
    && uv python install 3.14.0 \
    && uv sync --locked --no-dev --no-group bench --no-group airflow --python 3.14.0 \
    && cp /opt/iceberg/iceberg-spark-runtime-4.0_2.13-1.10.0.jar .venv/lib/python3.14/site-packages/pyspark/jars/ \
    && .venv/bin/python -c "import importlib.metadata as metadata, pyspark, sys; import lance_etl; assert sys.version.split()[0] == '3.14.0'; assert pyspark.__version__ == '4.0.1'; assert metadata.version('pylance') == '8.0.0'" \
    && .venv/bin/spark-submit --version
RUN mkdir -p /tmp/spark \
    && chown -R 185:185 /tmp/spark
ENV HOME=/tmp \
    PATH=/opt/lance-etl/.venv/bin:${PATH} \
    PYTHONPATH=/opt/lance-etl/src \
    SPARK_LOCAL_DIRS=/tmp/spark \
    UV_PYTHON_DOWNLOADS=never
USER 185:185
ENTRYPOINT ["lance-etl-reconcile"]
