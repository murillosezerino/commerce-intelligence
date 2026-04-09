# Commerce Intelligence

![Python](https://img.shields.io/badge/Python-3.11-3776AB?logo=python)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-4169E1?logo=postgresql)
![Scikit-Learn](https://img.shields.io/badge/Scikit--Learn-1.4-F7931E?logo=scikitlearn)
![dbt](https://img.shields.io/badge/dbt-1.7-FF694B?logo=dbt)

Sistema de inteligencia comercial com **segmentacao RFM**, **churn prediction** e integracao com PostgreSQL. Pipeline end-to-end que gera dados mock, cria modelos de staging, calcula metricas de cliente e treina modelo preditivo de churn.

## Arquitetura

```
Mock Data (Faker)
    │
    ▼
PostgreSQL (raw_customers, raw_orders, raw_order_items)
    │
    ▼
Staging Views (stg_customers, stg_orders, stg_order_items)
    │
    ├──► RFM Segmentation (mart_rfm)
    │       Campeao, Cliente Fiel, Cliente Recente,
    │       Em Risco, Perdido, Potencial
    │
    └──► Churn Prediction (mart_churn)
            Gradient Boosting → AUC-ROC
            Baixo / Medio / Alto Risco
```

## Stack Tecnica

| Tecnologia | Uso |
|---|---|
| Python | Linguagem principal |
| Pandas / NumPy | Manipulacao de dados |
| Scikit-learn | Modelo de churn (Gradient Boosting) |
| SQLAlchemy / psycopg2 | Conexao com PostgreSQL |
| dbt | Transformacoes SQL |
| Streamlit / Plotly | Dashboard (em desenvolvimento) |
| Faker | Geracao de dados mock |

## Dados Mock

O sistema gera automaticamente:
- **2.000 clientes** com perfil completo (nome, email, cidade, estado)
- **15.000 pedidos** com status (entregue, cancelado, processando, devolvido)
- **15 produtos** em 7 categorias (Eletronicos, Moda, Casa, Beleza, Esportes, Livros, Alimentos)
- **30% de clientes churned** (sem pedido ha mais de 90 dias)

## Segmentacao RFM

| Segmento | Criterio |
|---|---|
| Campeao | Recencia >= 4, Frequencia >= 4 |
| Cliente Fiel | Recencia >= 3, Frequencia >= 3 |
| Cliente Recente | Recencia >= 4, Frequencia <= 2 |
| Em Risco | Recencia <= 2, Frequencia >= 3 |
| Perdido | Recencia <= 2, Frequencia <= 2 |
| Potencial | Demais combinacoes |

## Churn Prediction — Features

| Feature | Descricao |
|---|---|
| customer_age_days | Dias desde o cadastro |
| total_orders | Total de pedidos realizados |
| total_spent | Valor total gasto |
| avg_order_value | Ticket medio |
| days_since_last_order | Dias desde o ultimo pedido |
| cancelled_orders | Pedidos cancelados |
| returned_orders | Pedidos devolvidos |
| completed_orders | Pedidos entregues |
| total_items | Itens comprados |
| unique_products | Produtos unicos |

## Estrutura do Projeto

```
├── pipeline.py          # Orquestrador principal
├── data/
│   └── mock_data.py     # Geracao de dados fake
├── dashboard/           # Dashboard Streamlit (em desenvolvimento)
├── dbt/
│   └── dbt_project.yml  # Configuracao dbt
├── ml/                  # Modelos de ML
├── check_db.py          # Verificacao de conexao
├── reset_db.py          # Reset do banco
├── requirements.txt
└── .github/workflows/
    └── ci.yml           # CI/CD automatizado
```

## Como Rodar

### Pre-requisitos

- Python 3.11+
- PostgreSQL rodando localmente

### Setup

```bash
# Clonar o repositorio
git clone https://github.com/murillosezerino/commerce-intelligence.git
cd commerce-intelligence

# Criar ambiente virtual
python -m venv venv
source venv/bin/activate  # Linux/Mac
venv\Scripts\activate     # Windows

# Instalar dependencias
pip install -r requirements.txt
```

### Configurar Variaveis de Ambiente

Crie um arquivo `.env` na raiz:

```env
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=commerce_intelligence
POSTGRES_USER=postgres
POSTGRES_PASSWORD=sua_senha
```

### Executar

```bash
# 1. Carregar dados mock no PostgreSQL
python data/mock_data.py

# 2. Rodar o pipeline completo (staging + RFM + churn)
python pipeline.py
```

## Licenca

MIT
