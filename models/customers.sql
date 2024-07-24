{{
    config(
        materialized='table'
    )
}}

with customers as (
    select * from {{ ref('stg_customers') }}
),

orders as (
    select * from {{ ref('stg_orders') }}
),

customer_orders as ( 
    select
        customer_id,
        min(order_date) as first_order_date,
        max(order_date) as most_recent_order_date,
        count(distinct order_id) as order_count
    from orders
    group by 1
)

select
    c.*,
    co.first_order_date,
    co.most_recent_order_date,
    co.order_count
from customers c
left join customer_orders co
    on c.customer_id = co.customer_id