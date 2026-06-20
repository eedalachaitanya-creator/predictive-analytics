FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# System deps:
#   gcc libpq-dev  — compile psycopg2
#   libpq5         — postgres client runtime
#   build-essential ca-certificates — general build tools
#   The rest       — Chromium runtime libs for Playwright scraper
RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential \
        gcc \
        libpq-dev \
        libpq5 \
        curl \
        ca-certificates \
        libnss3 \
        libatk1.0-0 \
        libatk-bridge2.0-0 \
        libcups2 \
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

# Install Python dependencies (includes playwright==1.58.0)
COPY requirements.txt ./
COPY wheels ./wheels

RUN pip install --no-cache-dir -r requirements.txt

# Download Chromium browser for Playwright
# playwright package is already installed above via requirements.txt
RUN PLAYWRIGHT_BROWSERS_PATH=/playwright-browsers \
    python -m playwright install --with-deps chromium

# Tell Playwright where the browsers are at runtime
ENV PLAYWRIGHT_BROWSERS_PATH=/playwright-browsers

# Copy all application code
COPY . .

EXPOSE 8017

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8017", "--log-level", "info"]