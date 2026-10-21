FROM python:3.12-slim

WORKDIR /app
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    QUAKEFLOW_DATA_DIR=/app/data

COPY pyproject.toml README.md ./
COPY src ./src
RUN pip install --no-cache-dir ".[dashboard]"

COPY transform ./transform
COPY dashboard ./dashboard

VOLUME ["/app/data"]
EXPOSE 8501

# Default: run the pipeline once. Override to serve the dashboard (see README).
CMD ["quakeflow", "run"]
