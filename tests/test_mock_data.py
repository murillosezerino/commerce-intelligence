import pandas as pd
from data.mock_data import generate_customers, generate_products, generate_orders


class TestGenerateCustomers:
    def test_returns_dataframe(self):
        df = generate_customers(10)
        assert isinstance(df, pd.DataFrame)

    def test_correct_row_count(self):
        df = generate_customers(50)
        assert len(df) == 50

    def test_required_columns(self):
        df = generate_customers(5)
        expected = {"customer_id", "nome", "email", "cidade", "estado", "signup_date", "is_active"}
        assert expected.issubset(set(df.columns))

    def test_unique_customer_ids(self):
        df = generate_customers(100)
        assert df["customer_id"].nunique() == 100


class TestGenerateProducts:
    def test_returns_dataframe(self):
        df = generate_products()
        assert isinstance(df, pd.DataFrame)

    def test_has_product_id(self):
        df = generate_products()
        assert "product_id" in df.columns
        assert len(df) > 0

    def test_all_prices_positive(self):
        df = generate_products()
        assert (df["preco"] > 0).all()


class TestGenerateOrders:
    def test_returns_orders_and_items(self):
        customers = generate_customers(20)
        orders, items = generate_orders(customers, 50)
        assert isinstance(orders, pd.DataFrame)
        assert isinstance(items, pd.DataFrame)

    def test_order_count(self):
        customers = generate_customers(20)
        orders, _ = generate_orders(customers, 50)
        assert len(orders) == 50

    def test_order_has_valid_customer(self):
        customers = generate_customers(20)
        orders, _ = generate_orders(customers, 50)
        valid_ids = set(customers["customer_id"])
        assert orders["customer_id"].isin(valid_ids).all()

    def test_items_reference_valid_orders(self):
        customers = generate_customers(20)
        orders, items = generate_orders(customers, 50)
        valid_order_ids = set(orders["order_id"])
        assert items["order_id"].isin(valid_order_ids).all()

    def test_totals_are_positive(self):
        customers = generate_customers(20)
        orders, _ = generate_orders(customers, 50)
        assert (orders["total"] > 0).all()
