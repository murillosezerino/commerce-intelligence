# Commerce Intelligence

> Technical study: customer segmentation (RFM) and churn prediction over synthetic e-commerce data, with dbt staging and an analytical dashboard.

A focused exercise in applying classical analytics techniques to retail data. The project covers the full path from raw transactions to executive dashboard: data modeling in PostgreSQL, dbt staging views, RFM segmentation, churn prediction with Gradient Boosting, and a Plotly dashboard.

## What this project explores

- **Dimensional modeling** in PostgreSQL with dbt staging layer
- **RFM segmentation** into 6 customer categories
- **Churn prediction** using Gradient Boosting on engineered features
- **Analytical dashboard** with Plotly for cohort and segment views
- **2,000 synthetic customers** and **15,000 orders** as the working dataset

## Stack

`Python` · `PostgreSQL` · `SQLAlchemy` · `dbt` · `Scikit-Learn` · `Pandas` · `Plotly`

## Architecture

```
raw orders/customers (PostgreSQL)
        ↓
dbt staging views  →  feature engineering  →  RFM + churn model
        ↓
Plotly dashboard
```

## What's inside

- `dbt/` — staging models and source definitions
- `src/segmentation/` — RFM logic and category assignment
- `src/churn/` — feature engineering and Gradient Boosting model
- `dashboard/` — Plotly visualizations
- `tests/` — unit tests for segmentation and feature logic

## How to run

```bash
pip install -r requirements.txt
docker-compose up -d                  # PostgreSQL
python scripts/seed_database.py       # Seed synthetic data
dbt run --project-dir dbt/            # Build staging views
python main.py                        # Run analytics pipeline
```

## Notes on the dataset

The synthetic data was generated to mimic a B2C retail distribution: realistic order frequency curves, seasonal variation, and a deliberately injected churn signal so the model has something to learn. It is not real commercial data.

## Status

Study repository. The patterns (dbt + segmentation + churn) are common in production e-commerce stacks; here they are exercised on a controlled dataset.

## Author

Murillo Sezerino — Analytics Engineer · Data Engineer
[murillosezerino.com](https://murillosezerino.com) · [LinkedIn](https://linkedin.com/in/murillosezerino)
