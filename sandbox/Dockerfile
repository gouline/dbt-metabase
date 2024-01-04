FROM python:3.11-slim-bullseye

WORKDIR /app

COPY requirements.txt ./

RUN python3 -m pip install -r requirements.txt

COPY entrypoint.py ./
COPY profiles.yml ./

COPY --from=sample_project etc/ ./etc/
COPY --from=sample_project models/ ./models/
COPY --from=sample_project seeds/ ./seeds/
COPY --from=sample_project dbt_project.yml ./

ENTRYPOINT ["python3", "entrypoint.py"]
