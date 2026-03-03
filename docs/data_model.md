# Data Model - Star Schema

## Fact Table
### fact_sales
**Grain:** order_id + order_item_id

Key columns:
- order_id
- order_item_id
- customer_id
- store_id
- product_id
- order_date, date_key
- quantity
- gross_amount, discount_amount, net_amount
- cost_amount, profit_amount

Partition:
- order_date

---

## Dimensions
### dim_customer (PK: customer_id)
- customer_name, email, phone
- city, state, country
- customer_created_date, customer_updated_ts

### dim_product (PK: product_id)
- product_name, category, brand
- unit_price, cost_price
- product_created_date, product_updated_ts

### dim_store (PK: store_id)
- store_name, store_type
- city, state, country
- opened_date, store_updated_ts

### dim_date (PK: date_key)
- date, year, month, day, day_of_week
