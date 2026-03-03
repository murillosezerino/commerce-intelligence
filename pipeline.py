"""
pipeline.py — Orquestrador do Commerce Intelligence.
Executa: mock data -> modelos SQL -> churn prediction -> RFM -> salva resultados
"""

import logging
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import roc_auc_score, classification_report
from sklearn.preprocessing import StandardScaler
import joblib
import os
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(module)s — %(message)s",
)
logger = logging.getLogger(__name__)


def get_engine():
    host     = os.getenv("POSTGRES_HOST", "localhost")
    port     = os.getenv("POSTGRES_PORT", "5432")
    db       = os.getenv("POSTGRES_DB", "commerce_intelligence")
    user     = os.getenv("POSTGRES_USER", "postgres")
    password = os.getenv("POSTGRES_PASSWORD", "")
    return create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}")


def run_staging(engine):
    logger.info("[1/4] Rodando modelos de staging...")
    with engine.connect() as conn:

        conn.execute(text("""
            CREATE OR REPLACE VIEW stg_customers AS
            SELECT
                customer_id,
                nome AS customer_name,
                email,
                cidade AS city,
                estado AS state,
                signup_date::timestamp AS signup_date,
                is_active
            FROM raw_customers
            WHERE email IS NOT NULL AND customer_id IS NOT NULL
        """))

        conn.execute(text("""
            CREATE OR REPLACE VIEW stg_orders AS
            SELECT
                order_id,
                customer_id,
                status,
                total AS order_total,
                created_at::timestamp AS order_date,
                date_trunc('month', created_at) AS order_month,
                CASE WHEN status = 'entregue' THEN true ELSE false END AS is_completed
            FROM raw_orders
            WHERE order_id IS NOT NULL AND customer_id IS NOT NULL
        """))

        conn.execute(text("""
            CREATE OR REPLACE VIEW stg_order_items AS
            SELECT
                item_id,
                order_id,
                product_id,
                quantidade AS quantity,
                preco_unit AS unit_price,
                subtotal
            FROM raw_order_items
            WHERE item_id IS NOT NULL AND order_id IS NOT NULL
        """))

        conn.commit()
    logger.info("  Views de staging criadas.")


def run_rfm(engine):
    logger.info("[2/4] Calculando segmentacao RFM...")
    with engine.connect() as conn:
        conn.execute(text("""
            CREATE OR REPLACE VIEW mart_rfm AS
            WITH rfm_base AS (
                SELECT
                    customer_id,
                    MAX(order_date) AS last_order_date,
                    COUNT(order_id) AS frequency,
                    SUM(order_total) AS monetary,
                    EXTRACT(DAY FROM (NOW() - MAX(order_date))) AS recency_days
                FROM stg_orders
                WHERE is_completed = true
                GROUP BY customer_id
            ),
            rfm_scores AS (
                SELECT *,
                    NTILE(5) OVER (ORDER BY recency_days ASC)  AS r_score,
                    NTILE(5) OVER (ORDER BY frequency ASC)     AS f_score,
                    NTILE(5) OVER (ORDER BY monetary ASC)      AS m_score
                FROM rfm_base
            )
            SELECT *,
                (r_score + f_score + m_score) AS rfm_total,
                CASE
                    WHEN r_score >= 4 AND f_score >= 4 THEN 'Campiao'
                    WHEN r_score >= 3 AND f_score >= 3 THEN 'Cliente Fiel'
                    WHEN r_score >= 4 AND f_score <= 2 THEN 'Cliente Recente'
                    WHEN r_score <= 2 AND f_score >= 3 THEN 'Em Risco'
                    WHEN r_score <= 2 AND f_score <= 2 THEN 'Perdido'
                    ELSE 'Potencial'
                END AS segment
            FROM rfm_scores
        """))
        conn.commit()

    rfm = pd.read_sql("SELECT * FROM mart_rfm", engine)
    logger.info(f"  RFM calculado: {len(rfm):,} clientes")
    logger.info(f"  Segmentos: {rfm['segment'].value_counts().to_dict()}")
    return rfm


def build_churn_features(engine) -> pd.DataFrame:
    query = """
        WITH customer_orders AS (
            SELECT
                o.customer_id,
                COUNT(o.order_id)                                      AS total_orders,
                SUM(o.order_total)                                     AS total_spent,
                AVG(o.order_total)                                     AS avg_order_value,
                EXTRACT(DAY FROM (NOW() - MAX(o.order_date)))          AS days_since_last_order,
                SUM(CASE WHEN o.status = 'cancelado' THEN 1 ELSE 0 END) AS cancelled_orders,
                SUM(CASE WHEN o.status = 'devolvido' THEN 1 ELSE 0 END) AS returned_orders,
                SUM(CASE WHEN o.is_completed THEN 1 ELSE 0 END)        AS completed_orders
            FROM stg_orders o
            GROUP BY o.customer_id
        ),
        customer_items AS (
            SELECT
                o.customer_id,
                SUM(i.quantity)            AS total_items,
                COUNT(DISTINCT i.product_id) AS unique_products
            FROM stg_orders o
            JOIN stg_order_items i ON o.order_id = i.order_id
            GROUP BY o.customer_id
        )
        SELECT
            c.customer_id,
            EXTRACT(DAY FROM (NOW() - c.signup_date))     AS customer_age_days,
            COALESCE(co.total_orders, 0)                  AS total_orders,
            COALESCE(co.total_spent, 0)                   AS total_spent,
            COALESCE(co.avg_order_value, 0)               AS avg_order_value,
            COALESCE(co.days_since_last_order, 999)        AS days_since_last_order,
            COALESCE(co.cancelled_orders, 0)              AS cancelled_orders,
            COALESCE(co.returned_orders, 0)               AS returned_orders,
            COALESCE(co.completed_orders, 0)              AS completed_orders,
            COALESCE(ci.total_items, 0)                   AS total_items,
            COALESCE(ci.unique_products, 0)               AS unique_products,
            CASE WHEN COALESCE(co.days_since_last_order, 999) > 30 THEN 1 ELSE 0 END AS is_churned
        FROM stg_customers c
        LEFT JOIN customer_orders co ON c.customer_id = co.customer_id
        LEFT JOIN customer_items  ci ON c.customer_id = ci.customer_id
    """
    return pd.read_sql(query, engine)


def run_churn_model(engine):
    logger.info("[3/4] Treinando modelo de churn prediction...")
    df = build_churn_features(engine)

    feature_cols = [
        "customer_age_days", "total_orders", "total_spent", "avg_order_value",
        "days_since_last_order", "cancelled_orders", "returned_orders",
        "completed_orders", "total_items", "unique_products"
    ]

    X = df[feature_cols].fillna(0)
    y = df["is_churned"]

    logger.info(f"  Total clientes: {len(df):,}")
    logger.info(f"  Churned: {y.sum():,} ({y.mean()*100:.1f}%)")

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )

    scaler = StandardScaler()
    X_train = pd.DataFrame(scaler.fit_transform(X_train), columns=feature_cols)
    X_test  = pd.DataFrame(scaler.transform(X_test), columns=feature_cols)

    model = GradientBoostingClassifier(
        n_estimators=200, max_depth=4, learning_rate=0.05, random_state=42
    )
    model.fit(X_train, y_train)

    y_proba = model.predict_proba(X_test)[:, 1]
    y_pred  = model.predict(X_test)
    auc     = roc_auc_score(y_test, y_proba)

    logger.info(f"  AUC-ROC: {auc:.4f}")
    logger.info("\n" + classification_report(y_test, y_pred))

    os.makedirs("models", exist_ok=True)
    joblib.dump(model,  "models/churn_model.pkl")
    joblib.dump(scaler, "models/churn_scaler.pkl")
    logger.info("  Modelo salvo: models/churn_model.pkl")

    # Salva predicoes no PostgreSQL
    df_all = build_churn_features(engine)
    X_all  = pd.DataFrame(
        scaler.transform(df_all[feature_cols].fillna(0)), columns=feature_cols
    )
    df_all["churn_probability"] = model.predict_proba(X_all)[:, 1]
    df_all["churn_segment"] = pd.cut(
        df_all["churn_probability"],
        bins=[0, 0.3, 0.6, 1.0],
        labels=["Baixo Risco", "Medio Risco", "Alto Risco"]
    )
    df_all[["customer_id", "churn_probability", "churn_segment", "is_churned"]].to_sql(
        "mart_churn", engine, if_exists="replace", index=False
    )
    logger.info("  Predicoes salvas: mart_churn")
    return auc


def main():
    logger.info("=" * 55)
    logger.info("Commerce Intelligence Pipeline")
    logger.info("=" * 55)

    engine = get_engine()
    run_staging(engine)
    rfm = run_rfm(engine)
    auc = run_churn_model(engine)

    logger.info("=" * 55)
    logger.info(f"Pipeline concluido.")
    logger.info(f"RFM: {rfm['segment'].value_counts().to_dict()}")
    logger.info(f"Churn AUC-ROC: {auc:.4f}")
    logger.info("=" * 55)


if __name__ == "__main__":
    main()