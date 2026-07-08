FROM python:3.12-slim

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /app
ENV UV_PYTHON_DOWNLOADS=never UV_PYTHON=python3.12

COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project

COPY src ./src
COPY README.md LICENSE ./
RUN uv sync --frozen --no-dev

RUN useradd --system --uid 10001 gateway
USER gateway

ENV PATH="/app/.venv/bin:$PATH"
EXPOSE 8080
CMD ["sh", "-c", "uvicorn --factory push_gateway.main:create_app --host 0.0.0.0 --port ${PORT:-8080}"]
