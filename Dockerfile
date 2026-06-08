# custom airflow image with the pipeline's python deps baked in at build time.
# this replaces the per-container `pip install -r /requirements.txt` on startup,
# which re-resolved the whole dependency tree on every `docker compose up`.
# rebuild after editing requirements.txt:  docker compose build
FROM apache/airflow:2.9.0

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
