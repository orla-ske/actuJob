# custom airflow image with the pipeline's python deps baked in at build time.
# this replaces the per-container `pip install -r /requirements.txt` on startup,
# which re-resolved the whole dependency tree on every `docker compose up`.
# rebuild after editing requirements.txt:  docker compose build
FROM apache/airflow:2.9.0

# libgomp1 is the gnu openmp runtime that lightgbm's compiled lib links against
# (libgomp.so.1). it isn't in the base image, so install it as root before deps.
USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends libgomp1 \
    && rm -rf /var/lib/apt/lists/*
USER airflow

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
