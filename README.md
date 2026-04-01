[![Review Assignment Due Date](https://classroom.github.com/assets/deadline-readme-button-22041afd0340ce965d47ae6ef1cefeee28c7c493a6346c4f15d667ab976d596c.svg)](https://classroom.github.com/a/Nvxy3054)
# ETL Pipeline — Amman Digital Market

## Overview

This project implements an ETL pipeline for customer analytics using data from the Amman Digital Market PostgreSQL database.

The pipeline performs four main steps:

1. **Extract** data from the source PostgreSQL tables:
   - `customers`
   - `products`
   - `orders`
   - `order_items`

2. **Transform** the raw data into customer-level analytics by:
   - joining orders, order items, products, and customers
   - calculating item-level revenue
   - excluding cancelled orders
   - excluding suspicious quantities greater than 100
   - aggregating results to the customer level

3. **Validate** the transformed data with quality checks

4. **Load** the final customer summary into:
   - a PostgreSQL table called `customer_analytics`
   - a CSV file at `output/customer_analytics.csv`

## Setup

1. Start PostgreSQL container:
   ```bash
   docker run -d --name postgres-m3-int \
     -e POSTGRES_USER=postgres -e POSTGRES_PASSWORD=postgres \
     -e POSTGRES_DB=amman_market \
     -p 5432:5432 -v pgdata_m3_int:/var/lib/postgresql/data \
     postgres:15-alpine
   ```
2. Load schema and data:
   ```bash
   psql -h localhost -U postgres -d amman_market -f schema.sql
   psql -h localhost -U postgres -d amman_market -f seed_data.sql
   ```
3. Install dependencies: `pip install -r requirements.txt`

## How to Run

```bash
python etl_pipeline.py
```

## Output

<!-- What does customer_analytics.csv contain? -->

## Quality Checks

The pipeline performs several validation checks on the transformed data before loading it to ensure data quality and reliability.

### Checks performed

- **No null customer_id**
  - Ensures every record has a valid unique identifier

- **No null customer_name**
  - Ensures all records are readable and complete

- **Positive total_revenue**
  - Ensures each customer has meaningful transaction data

- **No duplicate customer_id**
  - Ensures each customer appears only once in the final output

- **Positive total_orders**
  - Ensures each customer has at least one valid order

### Why these checks matter

These validations help catch data issues early and prevent invalid or misleading data from being loaded into the database or exported to the CSV file. They ensure the final dataset is clean, consistent, and suitable for analytics.