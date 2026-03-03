from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os

load_dotenv()

engine = create_engine(
    f"postgresql+psycopg2://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}"
    f"@{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_DB')}"
)

with engine.connect() as conn:
    conn.execute(text("DROP VIEW IF EXISTS mart_churn CASCADE"))
    conn.execute(text("DROP VIEW IF EXISTS mart_rfm CASCADE"))
    conn.execute(text("DROP VIEW IF EXISTS stg_order_items CASCADE"))
    conn.execute(text("DROP VIEW IF EXISTS stg_orders CASCADE"))
    conn.execute(text("DROP VIEW IF EXISTS stg_customers CASCADE"))
    conn.execute(text("DROP TABLE IF EXISTS raw_customers CASCADE"))
    conn.execute(text("DROP TABLE IF EXISTS raw_orders CASCADE"))
    conn.execute(text("DROP TABLE IF EXISTS raw_order_items CASCADE"))
    conn.execute(text("DROP TABLE IF EXISTS raw_products CASCADE"))
    conn.execute(text("DROP TABLE IF EXISTS mart_churn CASCADE"))
    conn.commit()

print("Tudo dropado.")