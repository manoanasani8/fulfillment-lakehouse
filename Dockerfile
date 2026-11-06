FROM python:3.12-slim-bookworm

LABEL org.opencontainers.image.source="https://github.com/manoanasani8/fulfillment-lakehouse" \
      org.opencontainers.image.description="PySpark and Delta Lake fulfillment analytics demo"

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --create-home --uid 10001 lakehouse

ENV PYTHONUNBUFFERED=1 SPARK_LOCAL_IP=127.0.0.1
WORKDIR /app
COPY requirements.txt ./
RUN python -m pip install --no-cache-dir -r requirements.txt
COPY --chown=10001:10001 lakehouse.py test_lakehouse.py ./
COPY --chown=10001:10001 examples/ ./examples/
RUN mkdir /app/artifacts && chown 10001:10001 /app/artifacts
USER 10001
ENTRYPOINT ["python", "lakehouse.py"]
