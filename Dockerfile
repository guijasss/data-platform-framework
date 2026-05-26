FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY pyproject.toml README.md ./
RUN pip install --no-cache-dir \
    "pandas>=2.2.3" \
    "polars>=1.40.1" \
    "pyarrow>=18.1.0" \
    "sqlalchemy>=2.0.49" \
    "pyyaml>=6.0.2" \
    "psycopg[binary]>=3.2.9" \
    "connectorx>=0.4.5"

COPY src ./src
COPY demo.py ./demo.py

CMD ["python", "demo.py"]
