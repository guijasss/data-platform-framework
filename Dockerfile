FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV UV_LINK_MODE=copy
ENV UV_NO_SYNC=1
ENV PATH="/app/.venv/bin:${PATH}"

WORKDIR /app

RUN pip install --no-cache-dir uv

COPY pyproject.toml uv.lock README.md ./
COPY src ./src
COPY demo.py ./demo.py
COPY metadata ./metadata
COPY tasks ./tasks

RUN uv sync --frozen --no-dev

CMD ["uv", "run", "python", "demo.py"]
