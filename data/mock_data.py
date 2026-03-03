import pandas as pd
import numpy as np
from faker import Faker
from datetime import datetime, timedelta
import random
from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv

load_dotenv()

fake = Faker("pt_BR")
np.random.seed(42)
random.seed(42)

PRODUTOS = [
    {"nome": "Smartphone Samsung", "categoria": "Eletronicos", "preco": 1899.90},
    {"nome": "Notebook Dell",      "categoria": "Eletronicos", "preco": 3499.90},
    {"nome": "Fone Bluetooth",     "categoria": "Eletronicos", "preco": 299.90},
    {"nome": "Camiseta Nike",      "categoria": "Moda",        "preco": 129.90},
    {"nome": "Tenis Adidas",       "categoria": "Moda",        "preco": 399.90},
    {"nome": "Vestido Floral",     "categoria": "Moda",        "preco": 189.90},
    {"nome": "Sofa 3 lugares",     "categoria": "Casa",        "preco": 2199.90},
    {"nome": "Luminaria LED",      "categoria": "Casa",        "preco": 89.90},
    {"nome": "Kit Skincare",       "categoria": "Beleza",      "preco": 249.90},
    {"nome": "Perfume Importado",  "categoria": "Beleza",      "preco": 349.90},
    {"nome": "Whey Protein",       "categoria": "Esportes",    "preco": 189.90},
    {"nome": "Tapete Yoga",        "categoria": "Esportes",    "preco": 99.90},
    {"nome": "Box Harry Potter",   "categoria": "Livros",      "preco": 299.90},
    {"nome": "Cafe Especial 1kg",  "categoria": "Alimentos",   "preco": 89.90},
    {"nome": "Azeite Premium",     "categoria": "Alimentos",   "preco": 69.90},
]

STATUS_PEDIDO  = ["entregue", "cancelado", "processando", "devolvido"]
STATUS_WEIGHTS = [0.75, 0.10, 0.10, 0.05]


def get_engine():
    host     = os.getenv("POSTGRES_HOST", "localhost")
    port     = os.getenv("POSTGRES_PORT", "5432")
    db       = os.getenv("POSTGRES_DB", "commerce_intelligence")
    user     = os.getenv("POSTGRES_USER", "postgres")
    password = os.getenv("POSTGRES_PASSWORD", "")
    return create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}")


def generate_customers(n=2000):
    records = []
    for i in range(n):
        signup = fake.date_time_between(start_date="-2y", end_date="-1m")
        records.append({
            "customer_id": i + 1,
            "nome":        fake.name(),
            "email":       fake.email(),
            "cidade":      fake.city(),
            "estado":      fake.state_abbr(),
            "signup_date": signup,
            "is_active":   random.choices([True, False], weights=[0.8, 0.2])[0],
        })
    return pd.DataFrame(records)


def generate_products():
    return pd.DataFrame([
        {"product_id": i + 1, **p} for i, p in enumerate(PRODUTOS)
    ])


def generate_orders(customers, n=15000):
    orders, items = [], []
    order_id, item_id = 1, 1

    # 30% dos clientes serao churned (ultimo pedido ha mais de 90 dias)
    all_customer_ids  = customers["customer_id"].tolist()
    churned_customers = set(random.sample(all_customer_ids, int(len(all_customer_ids) * 0.3)))

    for _ in range(n):
        customer   = customers.sample(1).iloc[0]
        cid        = customer["customer_id"]

        # Clientes churned compram apenas entre 91 e 24 meses atras
        if cid in churned_customers:
            created = fake.date_time_between(start_date="-24m", end_date="-91d")
        else:
            created = fake.date_time_between(start_date="-90d", end_date="-1d")

        status   = random.choices(STATUS_PEDIDO, weights=STATUS_WEIGHTS)[0]
        produtos = random.sample(PRODUTOS, random.randint(1, 5))
        total    = sum(p["preco"] * random.randint(1, 3) for p in produtos)

        orders.append({
            "order_id":    order_id,
            "customer_id": cid,
            "status":      status,
            "total":       round(total, 2),
            "created_at":  created,
        })

        for p in produtos:
            qty = random.randint(1, 3)
            items.append({
                "item_id":    item_id,
                "order_id":   order_id,
                "product_id": PRODUTOS.index(p) + 1,
                "quantidade": qty,
                "preco_unit": p["preco"],
                "subtotal":   round(p["preco"] * qty, 2),
            })
            item_id += 1

        order_id += 1

    return pd.DataFrame(orders), pd.DataFrame(items)


def load_to_postgres(engine):
    print("Gerando dados mock...")
    customers     = generate_customers(2000)
    products      = generate_products()
    orders, items = generate_orders(customers, 15000)

    print(f"  Clientes: {len(customers):,}")
    print(f"  Produtos: {len(products):,}")
    print(f"  Pedidos:  {len(orders):,}")
    print(f"  Itens:    {len(items):,}")

    churned = orders.groupby("customer_id")["created_at"].max()
    n_churned = (churned < pd.Timestamp.now() - pd.Timedelta(days=90)).sum()
    print(f"  Clientes churned (sem pedido 90d+): {n_churned:,}")

    print("Carregando no PostgreSQL...")
    with engine.connect() as conn:
        for view in ["mart_churn", "mart_rfm", "stg_order_items", "stg_orders", "stg_customers"]:
            conn.execute(text(f"DROP VIEW IF EXISTS {view} CASCADE"))
        conn.commit()

    customers.to_sql("raw_customers",   engine, if_exists="replace", index=False)
    products.to_sql("raw_products",     engine, if_exists="replace", index=False)
    orders.to_sql("raw_orders",         engine, if_exists="replace", index=False)
    items.to_sql("raw_order_items",     engine, if_exists="replace", index=False)
    print("Dados carregados com sucesso.")


if __name__ == "__main__":
    engine = get_engine()
    load_to_postgres(engine)