FROM python:3.11-slim-bullseye

RUN apt-get update && \
    apt-get install -y gcc libpq-dev

WORKDIR /app

COPY --from=root requirements.txt ./
COPY --from=root requirements-test.txt ./

RUN pip install --no-cache-dir -r requirements.txt -r requirements-test.txt

WORKDIR /app/sandbox

ENTRYPOINT ["python3", "entrypoint.py"]
