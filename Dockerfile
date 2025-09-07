FROM python:3.12-slim

# Faster, cleaner Python runtime
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Copy package source
COPY smart_scanner/ /app/smart_scanner/
COPY README.md /app/README.md

# Install dependencies and the package
RUN pip install --upgrade pip && \
    pip install --no-cache-dir /app/smart_scanner

# Default persistent data paths (can be overridden at runtime)
ENV METRICS_PATH=/data/scanner_metrics.jsonl \
    BANDIT_STATE_PATH=/data/bandit_state.json \
    UNIVERSE_CACHE_PATH=/data/.universe_cache.json \
    PYTHONPATH=/app

# Non-root user for safety
RUN useradd -m appuser && mkdir -p /data && chown -R appuser:appuser /data /app
USER appuser

VOLUME ["/data"]

# Default command runs the continuous scanning loop
CMD ["python", "-m", "smart_scanner.scanner_runner", "--loop"]

