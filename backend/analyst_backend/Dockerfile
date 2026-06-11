# ============================================================
# Analyst Agent — Dockerfile
# Sprint 0 | Task 1.4
#
# Multi-stage build for optimized production image
# ============================================================
 
# ── Build stage ────────────────────────────────────────────
FROM python:3.11-slim as builder
 
WORKDIR /build
 
# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
&& rm -rf /var/lib/apt/lists/*
 
# Copy requirements
COPY requirements.txt .
 
COPY wheels ./wheels

RUN pip install --user --no-cache-dir -r requirements.txt --find-links ./wheels
 
# ── Runtime stage ──────────────────────────────────────────
FROM python:3.11-slim
 
WORKDIR /app
 
# Install runtime dependencies only
# libpq5       — PostgreSQL client library (psycopg2)
# curl         — health check
# chromium deps — required by Playwright to run Chromium headless
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq5 \
    curl \
    libnss3 \
    libnspr4 \
    libatk1.0-0 \
    libatk-bridge2.0-0 \
    libcups2 \
    libdrm2 \
    libdbus-1-3 \
    libxkbcommon0 \
    libxcomposite1 \
    libxdamage1 \
    libxfixes3 \
    libxrandr2 \
    libgbm1 \
    libasound2 \
    libpango-1.0-0 \
    libcairo2 \
&& rm -rf /var/lib/apt/lists/*
 
# Copy Python dependencies from builder
COPY --from=builder /root/.local /root/.local
 
# Set PATH to use local pip packages
ENV PATH=/root/.local/bin:$PATH \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1
 
# Copy application code
COPY . .
 
# Install Playwright Chromium browser into a fixed path
RUN PLAYWRIGHT_BROWSERS_PATH=/playwright-browsers \
    python -m playwright install --with-deps chromium
 
# Tell Playwright where the browsers are at runtime
ENV PLAYWRIGHT_BROWSERS_PATH=/playwright-browsers
 
# Health check — port must match CMD port (8017)
HEALTHCHECK --interval=30s --timeout=10s --start-period=40s --retries=3 \
    CMD curl -f http://localhost:8017/docs || exit 1
 
# Expose port
EXPOSE 8017
 
# Run application
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8017"]