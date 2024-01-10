FROM python:3.11-slim-bullseye

WORKDIR /app

COPY --from=root requirements.txt ./
COPY --from=root requirements-test.txt ./

RUN pip install -r requirements.txt -r requirements-test.txt

WORKDIR /app/sandbox

ENTRYPOINT ["python3", "entrypoint.py"]
