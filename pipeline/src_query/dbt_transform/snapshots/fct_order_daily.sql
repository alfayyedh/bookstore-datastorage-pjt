{% snapshot fct_order_daily %}

{{
    config(
      target_database='bookstore-dwh',
      target_schema='final',
      unique_key='sk_order_daily_id',

      strategy='check',
      check_cols=[
        'order_id',
        'order_date',
        'book_id',
        'price']
    )
}}

with stg_cust_order as (
    select *
    from {{ source("bookstore-dwh","cust_order") }}
),

stg_order_line as (
    select *
    from {{ source("bookstore-dwh","order_line") }}
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
        db.sk_book_id,
        sol.price,
        dd.date_id as order_date_id
    from stg_cust_order sco
    join order_line sol
        on sco.nk_order_id = sol.order_id
    join dim_book db
        on db.nk_book_id = sol.book_id
    join dim_date dd
        on dd.date_actual = DATE(sco.order_date)
),

final_fct_order_daily as (
    select
        {{ dbt_utils.generate_surrogate_key( ["nk_order_id"] ) }} as sk_order_daily_id,  
        *
    from fct_order_daily
)

select * from final_fct_order_daily

{% endsnapshot %}