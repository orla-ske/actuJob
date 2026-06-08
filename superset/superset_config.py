import os

SECRET_KEY = os.environ.get("SUPERSET_SECRET_KEY", "changeme_superset_secret_32chars!!")
SQLALCHEMY_DATABASE_URI = os.environ.get(
    "SQLALCHEMY_DATABASE_URI",
    "postgresql+psycopg2://airflow:airflow@postgres/superset",
)

# allow duckdb connections from the ui
PREVENT_UNSAFE_DB_CONNECTIONS = False

# store uploaded files alongside the superset home
UPLOAD_FOLDER = "/app/superset_home/uploads/"
