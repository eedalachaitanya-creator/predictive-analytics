FROM python:3.12-slim
 
ENV PYTHONDONTWRITEBYTECODE=1

ENV PYTHONUNBUFFERED=1
 
WORKDIR /app
 
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

        libdrm2 \

        libxkbcommon0 \

        libxcomposite1 \

        libxdamage1 \

        libxfixes3 \

        libxrandr2 \

        libgbm1 \

        libgtk-3-0 \

        libxcb1 \

        libx11-xcb1 \

        libxss1 \

        libasound2 \

        libpango-1.0-0 \

        libcairo2 \
&& rm -rf /var/lib/apt/lists/*
 
COPY requirements.txt ./

COPY wheels ./wheels
 
RUN pip install --upgrade pip setuptools \
&& pip install --no-cache-dir --find-links=./wheels -r requirements.txt
 
RUN PLAYWRIGHT_BROWSERS_PATH=/playwright-browsers \

    python -m playwright install --with-deps chromium \
&& python -m playwright install-deps
 
ENV PLAYWRIGHT_BROWSERS_PATH=/playwright-browsers
 
COPY . .
 
EXPOSE 8017
 
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8017", "--proxy-headers", "--log-level", "info"]
 