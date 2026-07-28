FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app
COPY pyproject.toml README.md alembic.ini ./
COPY migrations ./migrations
COPY src ./src
RUN pip install --upgrade pip && pip install ".[live,backtest]"

RUN useradd --create-home --uid 10001 syncopy
USER syncopy

EXPOSE 8080
CMD ["syncopy", "run"]

