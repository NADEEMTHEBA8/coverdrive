FROM python:3.11-slim

WORKDIR /app

COPY pyproject.toml README.md ./
COPY src/ ./src/

RUN --mount=type=cache,target=/root/.cache/pip \
    pip install -e .

EXPOSE 8000

CMD ["python", "-m", "coverdrive.api"]
