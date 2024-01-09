FROM python:3.11-slim-bullseye

WORKDIR /app

COPY requirements.txt ./

RUN python3 -m pip install -r requirements.txt

COPY entrypoint.py ./
COPY profiles.yml ./

COPY models/ ./models/
COPY seeds/ ./seeds/
COPY dbt_project.yml ./

ENTRYPOINT ["python3", "entrypoint.py"]
