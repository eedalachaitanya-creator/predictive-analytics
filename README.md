# CRP Analyst Agent — Churn Prediction & Retention Platform

Multi-tenant churn prediction platform for retail businesses. Ingests customer transaction data, reviews, and support tickets, then runs an ML pipeline that produces churn scores, RFM segmentation, risk tiers, and personalized outreach emails.

## Architecture

```
UI/UI/          Angular 21 frontend (standalone components, signals)
app/            FastAPI backend (routers, config, database)
ml/             ML pipeline (feature engineering, training, prediction, sentiment, outreach)
db/             PostgreSQL schema & migrations
```

## Prerequisites

- Python 3.11+
- Node.js 20+ and npm
- PostgreSQL 15+
- Redis (for caching)

## Setup

### 1. Clone & install Python dependencies

```bash
git clone <repo-url> && cd analyst_agent_v3
python -m venv venv
source venv/bin/activate          # Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### 2. Create the database

```bash
createdb walmart_crp              # or use pgAdmin / psql
psql -d walmart_crp -f db/schema_postgresql.sql
```

If the schema file doesn't cover all tables (some were added via migrations), also run:

```bash
psql -d walmart_crp -f db/migration_multi_tenant.sql
psql -d walmart_crp -f db/migration_users_table.sql
psql -d walmart_crp -f db/migration_active_tokens.sql
```

### 3. Configure environment

```bash
cp .env.example .env
# Edit .env with your PostgreSQL credentials, Groq API key, etc.
```

Key variables:
- `DATABASE_URL` — PostgreSQL connection string
- `GROQ_API_KEY` — For LLM-powered outreach emails (get at https://console.groq.com)
- `SECRET_KEY` — JWT signing key (change from default)
- `REDIS_URL` — Redis connection (default: redis://localhost:6379/0)

### 4. Start Redis

```bash
redis-server
```

### 5. Start the backend

```bash
uvicorn app.main:app --reload --port 8000
```

Verify: open http://localhost:8000/health — should return `{"status": "healthy"}`.

### 6. Start the frontend

```bash
cd UI/UI
npm install
npm start
```

Opens at http://localhost:4200. Login and select a client to begin.

## Running the Pipeline

1. Go to **Upload** page and upload the 13 master files (Customer, Order, Line Items, Product, Price, Category, Sub-Category, Sub-Sub-Category, Brand, Vendor, Vendor-Map, Reviews, Support Tickets).
2. Go to **Analyst Agent** page, select "Full Pipeline" mode, click **PROCESS DATA**.
3. Pipeline runs 11 stages: DB connect, sentiment analysis, materialized view, feature engineering, model training, prediction with tier weighting, risk summary, purchase cycles, refill alerts + outreach generation, finalize.

## Pipeline Stages

| Stage | What it does |
|-------|-------------|
| 1 | Database connectivity check |
| 2 | VADER sentiment scoring on customer reviews |
| 3 | Refresh mv_customer_features materialized view |
| 4 | Feature engineering (30+ features from orders, reviews, tickets) |
| 5 | Train 3 models (XGBoost, Random Forest, Logistic Regression), pick best by ROC-AUC |
| 6 | Predict churn + apply tier-based business weighting |
| 7 | Risk summary (HIGH >= 0.65, MEDIUM >= 0.35, LOW < 0.35) |
| 8 | Compute purchase cycles for subscription products |
| 9 | Subscription refill alerts + outreach |
| 10 | Generate churn-based outreach emails using templates |
| 11 | Finalize outputs |

## Key Features

- **Multi-tenant**: Client-isolated data via `client_id` filtering everywhere
- **3-model ensemble**: Automatically selects best-performing model per run
- **Tier weighting**: Platinum x1.25, Gold x1.15, Silver x1.00, Bronze x0.90
- **RFM segmentation**: 8 segments using individual R, F, M scores
- **VADER sentiment**: Scores customer reviews, cross-validated with star ratings
- **Outreach emails**: Auto-generated after pipeline + manual trigger from UI
- **Dashboard drill-down**: Clickable segment bars expand to show customer details

## Project Structure

```
app/
  main.py                  FastAPI entry point
  config.py                Pydantic settings
  database.py              SQLAlchemy engine + session
  pipeline_router.py       Pipeline execution endpoints
  messages_router.py       Outreach template CRUD + email generation
  dashboard_router.py      Dashboard KPIs, segments, drill-down
  ...                      Other CRUD routers

ml/
  compute_rfm.py           RFM feature computation + segmentation
  train_model.py           Model training (XGB, RF, LR)
  predict.py               Prediction + tier weighting + risk assignment
  sentiment.py             VADER sentiment analysis
  alerts.py                Subscription refill alerts + LLM outreach
  feature_engineering.py   30+ feature computation

UI/UI/src/app/
  pages/                   Angular page components
  services/                API service layer
  models/                  TypeScript interfaces matching DB schema
```
