# ─────────────────────────────────────────────
# Lightweight ARM64 image for Raspberry Pi 4B
# Base: python:3.11-slim-bookworm (~50MB)
# ─────────────────────────────────────────────
FROM python:3.11-slim-bookworm

# Keeps Python from buffering stdout/stderr (important for logging)
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

# Install only what's needed for httpx[http2] (h2 lib needs gcc briefly)
RUN apt-get update && apt-get install -y --no-install-recommends \
        gcc \
    && rm -rf /var/lib/apt/lists/*

# Copy and install dependencies first (layer caching)
COPY requirements_docker.txt .
RUN pip install --no-cache-dir -r requirements_docker.txt \
    && apt-get purge -y --auto-remove gcc   # remove build tool after install

# Copy application source
COPY main.py .
COPY required_data.json .
# config.ini is intentionally NOT copied — mounted as a volume at runtime

# Create output/log directories (will be overridden by volume mounts)
RUN mkdir -p log_files Seoudi_tags Carrefour_tags

# Run as non-root user for security
RUN useradd -m osiris && chown -R osiris:osiris /app
USER osiris

CMD ["python", "main.py"]
