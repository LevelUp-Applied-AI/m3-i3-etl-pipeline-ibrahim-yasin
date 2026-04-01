"""ETL Pipeline — Amman Digital Market Customer Analytics
"""
from sqlalchemy import create_engine
import pandas as pd
import os


def extract(engine):
    customers = pd.read_sql("SELECT * FROM customers", engine)
    products = pd.read_sql("SELECT * FROM products", engine)
    orders = pd.read_sql("SELECT * FROM orders", engine)
    order_items = pd.read_sql("SELECT * FROM order_items", engine)

    return {
        "customers": customers,
        "products": products,
        "orders": orders,
        "order_items": order_items
    }


def transform(data_dict):
    customers = data_dict["customers"]
    products = data_dict["products"]
    orders = data_dict["orders"]
    order_items = data_dict["order_items"]

    orders = orders[orders["status"] != "cancelled"]
    order_items = order_items[order_items["quantity"] <= 100]

    merged = (
        order_items
        .merge(orders, on="order_id")
        .merge(products, on="product_id")
        .merge(customers, on="customer_id")
    )

    merged["line_total"] = merged["quantity"] * merged["unit_price"]

    category_rev = (
        merged.groupby(["customer_id", "category"])["line_total"]
        .sum()
        .reset_index()
    )

    top_category = (
        category_rev
        .sort_values(["customer_id", "line_total"], ascending=[True, False])
        .drop_duplicates("customer_id")
        [["customer_id", "category"]]
        .rename(columns={"category": "top_category"})
    )

    summary = (
        merged.groupby(["customer_id", "customer_name"])
        .agg(
            total_orders=("order_id", "nunique"),
            total_revenue=("line_total", "sum")
        )
        .reset_index()
    )

    summary["avg_order_value"] = summary["total_revenue"] / summary["total_orders"]

    summary = summary.merge(top_category, on="customer_id", how="left")

    return summary


def validate(df):
    checks = {}
    checks["no_null_ids"] = df["customer_id"].isna().sum() == 0
    checks["no_null_names"] = df["customer_name"].isna().sum() == 0
    checks["positive_revenue"] = (df["total_revenue"] > 0).all()
    checks["no_duplicates"] = df["customer_id"].is_unique
    checks["positive_orders"] = (df["total_orders"] > 0).all()

    for check, passed in checks.items():
        print(f"[{'PASS' if passed else 'FAIL'}] {check}")

    if not all(checks.values()):
        raise ValueError("Data quality checks failed")

    return checks


def load(df, engine, csv_path):
    os.makedirs(os.path.dirname(csv_path), exist_ok=True)
    df.to_sql("customer_analytics", engine, if_exists="replace", index=False)
    df.to_csv(csv_path, index=False)
    print(f"Loaded {len(df)} rows")


def main():
    engine = create_engine("postgresql://postgres:postgres@localhost:5432/amman_market")

    print("Extracting...")
    data = extract(engine)

    print("Transforming...")
    summary = transform(data)

    print("Validating...")
    validate(summary)

    print("Loading...")
    load(summary, engine, "output/customer_analytics.csv")

    print("Done")


if __name__ == "__main__":
    main()