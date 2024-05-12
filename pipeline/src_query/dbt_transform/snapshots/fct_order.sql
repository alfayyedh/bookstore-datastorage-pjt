{% snapshot fct_order %}

{{
    config(
      target_database='bookstore-dwh',
      target_schema='final',
      unique_key='sk_order_id',

      strategy='check',
      check_cols=[
        'order_id',
        'customer_id',
        'book_id',
        'price',
        'status_date',
        'status_value']
    )
}}

with stg_cust_order as (
    select *
    from {{ source("bookstore-dwh","cust_order") }}
),

stg_order_history as (
    select *
    from {{ source("bookstore-dwh","order_history") }}
),

stg_order_status as (
    select *
    from {{ source("bookstore-dwh","order_status") }}
),

dim_customer as (
    select *
    from {{ ref("dim_customer") }}
),

dim_book as (
    select *
    from {{ ref("dim_book") }}
),

dim_date as (
    select *
    from {{ ref("dim_date") }}
),

fct_order as (
    select 
        sco.order_id as nk_order_id,
        dc.sk_customer_id,
        db.sk_book_id,
        sos.status_value,
        dd.date_id as order_date_id,
        dd2.date_id as status_date_id,
    from stg_cust_order sco
    join dim_customer dc
        on dc.nk_customer_id = sco.customer_id
    join stg_order_history soh
        on soh.order_id = sco.order_id
    join stg_order_status sos
        on sos.status_id = soh.status_id
    join dim_date dd
        on dd.date_actual = DATE(sco.order_date)
    join dim_date dd2
        on dd2.date_actual = DATE(soh.status_date)
),

final_fct_order as (
    select
        {{ dbt_utils.generate_surrogate_key( ["nk_order_id"] ) }} as sk_order_id,  
        *
    from fct_order
)

select * from final_fct_order

{% endsnapshot %}