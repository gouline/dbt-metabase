FROM python:3.11-slim-bullseye

RUN python3 -m pip install dbt-postgres

WORKDIR /app

COPY dbt_project.yml ./
COPY models/ ./models/
COPY profiles.yml ./

CMD dbt run --profiles-dir .
