# Commerce Intelligence

[![CI](https://github.com/murillosezerino/commerce-intelligence/actions/workflows/ci.yml/badge.svg)](https://github.com/murillosezerino/commerce-intelligence/actions/workflows/ci.yml)
![Python](https://img.shields.io/badge/python-3.11%2B-blue)
![PostgreSQL](https://img.shields.io/badge/db-PostgreSQL-336791)
![dbt](https://img.shields.io/badge/transform-dbt-FF694B)

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

```
commerce-intelligence/
├── pipeline.py          # orquestrador: mock data -> staging SQL -> churn -> RFM
├── data/mock_data.py    # geracao de dados sinteticos (2k clientes, 15k pedidos)
├── ml/                  # modelo de churn (Gradient Boosting)
├── dashboard/           # visualizacoes Plotly
├── dbt/                 # dbt_project.yml + models de staging
├── check_db.py          # inspeciona o estado do banco
├── reset_db.py          # limpa as tabelas
└── tests/               # test_mock_data.py
```

## How to run

```bash
pip install -r requirements.txt

# PostgreSQL via variaveis de ambiente (.env ou export)
export POSTGRES_HOST=localhost POSTGRES_PORT=5432 \
       POSTGRES_DB=commerce_intelligence \
       POSTGRES_USER=postgres POSTGRES_PASSWORD=changeme

python pipeline.py       # gera dados sinteticos, roda staging SQL, churn e RFM
```

## Notes on the dataset

The synthetic data was generated to mimic a B2C retail distribution: realistic order frequency curves, seasonal variation, and a deliberately injected churn signal so the model has something to learn. It is not real commercial data.

## Status

Study repository. The patterns (dbt + segmentation + churn) are common in production e-commerce stacks; here they are exercised on a controlled dataset.

## Author

Murillo Sezerino — Data Engineer & Analytics
[murillosezerino.com](https://murillosezerino.com) · [LinkedIn](https://linkedin.com/in/murillosezerino)
