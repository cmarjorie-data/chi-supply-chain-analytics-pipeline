from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
from airflow.utils.dates import timedelta

# Define default arguments for the DAG
default_args = {
    'owner': 'Chinyere',
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG definition
dag = DAG(
    dag_id="chi_supplychain_analytics_single_schema",
    description="Supply Chain Analytics Transformation & Loading Pipeline DAG (using single schema supplychaindataset)",
    default_args=default_args,
    schedule_interval='0 1 * * *', # Daily at 1 AM
    catchup=False,
    tags=["supply_chain", "analytics", "single_schema", "postgres"],
)

POSTGRES_CONN_ID = "hag_postgres_cmo"
SCHEMA = "supplychaindataset"


# ----------------------------------------------------------------------------------------------------
# ANALYTICS TABLE 1: dim_plant
# (plant_id + capacity + products + #vmi_customers
# Business question: “What capabilities does each plant (warehouse / manufacturing facility) have?”
# ----------------------------------------------------------------------------------------------------
create_dim_plant = PostgresOperator(
    task_id="create_dim_plant",
    postgres_conn_id='hag_postgres_cmo',
    sql=f"""
    DROP TABLE IF EXISTS {SCHEMA}.dim_plant CASCADE;

    CREATE TABLE {SCHEMA}.dim_plant AS
    SELECT
        wc.plant_id,
        wc.daily_capacity,
        COALESCE(p.num_products, 0)      AS num_products,
        COALESCE(v.num_vmi_customers, 0) AS num_vmi_customers
    FROM {SCHEMA}.whcapacities wc
    LEFT JOIN (
        SELECT
            plant_code,
            COUNT(DISTINCT product_id) AS num_products
        FROM {SCHEMA}.productsperplant
        GROUP BY plant_code
    ) p
        ON wc.plant_id = p.plant_code
    LEFT JOIN (
        SELECT
            plant_code,
            COUNT(DISTINCT customer_id) AS num_vmi_customers
        FROM {SCHEMA}.vmicustomers
        GROUP BY plant_code
    ) v
        ON wc.plant_id = v.plant_code
    ;
    """,
    dag=dag,
)
# dim_plant VIEW
create_dim_plant_view = PostgresOperator(
    task_id="create_dim_plant_view",
    postgres_conn_id='hag_postgres_cmo',
    sql=f"""
    CREATE OR REPLACE VIEW {SCHEMA}.vw_dim_plant AS
    SELECT *
    FROM {SCHEMA}.dim_plant;
    """,
    dag=dag,
)

# ------------------------------------------------------------------------------------
# ANALYTICS TABLE 2: dim_customer
# (customer_id + order counts + first/last order dates)
# Business question: “What are the ordering patterns of/how active are our customers?”
# ------------------------------------------------------------------------------------
create_dim_customer = PostgresOperator(
    task_id="create_dim_customer",
    postgres_conn_id='hag_postgres_cmo',
    sql=f"""
    DROP TABLE IF EXISTS {SCHEMA}.dim_customer CASCADE;

    CREATE TABLE {SCHEMA}.dim_customer AS
    SELECT
        customer_id,
        COUNT(DISTINCT order_id) AS order_count,
        MIN(order_date)          AS first_order_date,
        MAX(order_date)          AS last_order_date
    FROM {SCHEMA}.orderlist
    GROUP BY customer_id
    ;
    """,
    dag=dag,
)

# dim_customer VIEW
create_dim_customer_view = PostgresOperator(
    task_id="create_dim_customer_view",
    postgres_conn_id='hag_postgres_cmo',
    sql=f"""
    CREATE OR REPLACE VIEW {SCHEMA}.vw_dim_customer AS
    SELECT *
    FROM {SCHEMA}.dim_customer;
    """,
    dag=dag,
)

# ---------------------------------------------------------------------
# ANALYTICS TABLE 3: dim_product
# product_id grain, with simple usage stats
# Business question: “How important is each product (SKU) in the supply chain?”
# ---------------------------------------------------------------------
create_dim_product = PostgresOperator(
    task_id="create_dim_product",
    postgres_conn_id='hag_postgres_cmo',
    sql=f"""
    DROP TABLE IF EXISTS {SCHEMA}.dim_product CASCADE;

    CREATE TABLE {SCHEMA}.dim_product AS
    SELECT
        p.product_id,
        COUNT(DISTINCT p.plant_code)  AS num_plants,
        COUNT(DISTINCT o.customer_id) AS num_customers,
        COUNT(DISTINCT o.order_id)    AS num_orders,
        SUM(o.unit_quantity)          AS total_units,
        SUM(o.weight)                 AS total_weight
    FROM {SCHEMA}.productsperplant p
    LEFT JOIN {SCHEMA}.orderlist o
        ON p.product_id = o.product_id
       AND p.plant_code = o.plant_code
    GROUP BY p.product_id
    ;
    """,
    dag=dag,
)

# dim_product VIEW
create_dim_product_view = PostgresOperator(
    task_id="create_dim_product_view",
    postgres_conn_id='hag_postgres_cmo',
    sql=f"""
    CREATE OR REPLACE VIEW {SCHEMA}.vw_dim_product AS
    SELECT *
    FROM {SCHEMA}.dim_product;
    """,
    dag=dag,
)


# ---------------------------------------------------------------------
# ANALYTICS TABLE 4: dim_carrier
# carrier grain, with simple route and cost stats
# Business question: “How do carriers perform across routes in terms of cost, routes, and speed?”
# ---------------------------------------------------------------------
create_dim_carrier = PostgresOperator(
    task_id="create_dim_carrier",
    postgres_conn_id=POSTGRES_CONN_ID,
    sql=f"""
    DROP TABLE IF EXISTS {SCHEMA}.dim_carrier CASCADE;

    CREATE TABLE {SCHEMA}.dim_carrier AS
    SELECT
        fr.carrier,
        COUNT(DISTINCT fr.orig_port_cd || '-' || fr.dest_port_cd) AS num_routes,
        MIN(fr.minimum_cost)                                     AS min_minimum_cost,
        MAX(fr.minimum_cost)                                     AS max_minimum_cost,
        MIN(fr.rate)                                             AS min_rate,
        MAX(fr.rate)                                             AS max_rate,
        AVG(fr.tpt_day_cnt)                                      AS avg_transit_days,
        COUNT(DISTINCT o.order_id)                               AS num_orders
    FROM {SCHEMA}.freightrates fr
    LEFT JOIN {SCHEMA}.orderlist o
        ON fr.carrier      = o.carrier
       AND fr.orig_port_cd = o.origin_port
       AND fr.dest_port_cd = o.destination_port
    GROUP BY fr.carrier
    ;
    """,
    dag=dag,
)

# dim_carrier VIEW
create_dim_carrier_view = PostgresOperator(
    task_id="create_dim_carrier_view",
    postgres_conn_id=POSTGRES_CONN_ID,
    sql=f"""
    CREATE OR REPLACE VIEW {SCHEMA}.vw_dim_carrier AS
    SELECT *
    FROM {SCHEMA}.dim_carrier;
    """,
    dag=dag,
)

# ---------------------------------------------------------------------
# ANALYTICS TABLE 5: fact_orders
# order_id grain, with full order details and shipping cost calculation
# Business question: “What is the full detail for each order, including shipping cost performance?”
# ---------------------------------------------------------------------
create_fact_orders = PostgresOperator(
    task_id="create_fact_orders",
    postgres_conn_id=POSTGRES_CONN_ID,
    sql=f"""
    DROP TABLE IF EXISTS {SCHEMA}.fact_orders CASCADE;

    CREATE TABLE {SCHEMA}.fact_orders AS
    WITH rate_match AS (
        SELECT
            o.order_id,
            o.order_date,
            o.customer_id,
            o.product_id,
            o.plant_code,
            o.origin_port,
            o.destination_port,
            o.carrier,
            o.unit_quantity,
            o.weight,
            o.service_level,
            o.ship_ahead_day_count,
            o.ship_late_day_count,
            fr.minimum_cost,
            fr.rate,
            fr.tpt_day_cnt,
            fr.carrier_type,
            GREATEST(fr.minimum_cost, fr.rate * o.weight) AS shipping_cost_calc
        FROM {SCHEMA}.orderlist o
        LEFT JOIN {SCHEMA}.freightrates fr
          ON fr.carrier      = o.carrier
         AND fr.orig_port_cd = o.origin_port
         AND fr.dest_port_cd = o.destination_port
    )
    SELECT
        order_id,
        order_date,
        customer_id,
        product_id,
        plant_code,
        origin_port,
        destination_port,
        carrier,
        unit_quantity,
        weight,
        service_level,
        ship_ahead_day_count,
        ship_late_day_count,
        minimum_cost,
        rate,
        tpt_day_cnt,
        carrier_type,
        shipping_cost_calc AS shipping_cost
    FROM rate_match
    ;
    """,
    dag=dag,
)

# fact_orders VIEW
create_fact_orders_view = PostgresOperator(
    task_id="create_fact_orders_view",
    postgres_conn_id=POSTGRES_CONN_ID,
    sql=f"""
    CREATE OR REPLACE VIEW {SCHEMA}.vw_fact_orders AS
    SELECT *
    FROM {SCHEMA}.fact_orders;
    """,
    dag=dag,
)

# ---------------------------------------------------------------------
# Task Dependencies: tables first, then views, then fact view last
# ---------------------------------------------------------------------

# views depend on their tables
create_dim_plant >> create_dim_plant_view
create_dim_customer >> create_dim_customer_view
create_dim_product >> create_dim_product_view
create_dim_carrier >> create_dim_carrier_view
create_fact_orders >> create_fact_orders_view

# fact_orders depends on all dimension tables (so joins are valid)
[create_dim_plant, create_dim_customer, create_dim_product, create_dim_carrier] >> create_fact_orders


