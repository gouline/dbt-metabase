FROM python:3.11-slim-bullseye

WORKDIR /app

COPY requirements.txt ./

RUN python3 -m pip install -r requirements.txt

COPY entrypoint.py ./
COPY dbt_project.yml ./
COPY models/ ./models/
COPY profiles.yml ./

ENTRYPOINT ["python3", "entrypoint.py"]
