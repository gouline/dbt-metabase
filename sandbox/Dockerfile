FROM python:3.12-slim-bullseye

RUN apt-get update && \
    apt-get install -y gcc libpq-dev

COPY --from=ghcr.io/astral-sh/uv:0.7.13 /uv /bin/uv

WORKDIR /app

COPY --from=root pyproject.toml uv.lock ./
RUN uv sync --frozen --no-install-project --all-extras

ENV PATH="/app/.venv/bin:$PATH"

WORKDIR /app/sandbox

ENTRYPOINT ["python3", "entrypoint.py"]
