from sqlalchemy import create_engine, inspect, text
from dotenv import load_dotenv
import os

load_dotenv()

engine = create_engine(
    f"postgresql+psycopg2://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}"
    f"@{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_DB')}"
)

with engine.connect() as conn:
    result = conn.execute(text("SELECT current_database()"))
    print(f"Banco conectado: {result.fetchone()[0]}")

    tables = inspect(engine).get_table_names()
    print(f"Tabelas: {tables}")

    r = conn.execute(text("SELECT COUNT(*) FROM raw_orders WHERE created_at < NOW() - INTERVAL '30 days'"))
    print("Pedidos com mais de 30 dias:", r.fetchone()[0])

    r2 = conn.execute(text("SELECT MIN(created_at), MAX(created_at) FROM raw_orders"))
    print("Range de datas:", r2.fetchone())