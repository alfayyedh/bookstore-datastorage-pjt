{% snapshot dim_customer %}

{{
    config(
      target_database='bookstore-dwh',
      target_schema='final',
      unique_key='sk_customer_id',

      strategy='check',
      check_cols=[
			'first_name',
			'last_name',
			'email',
			'phone',
			'street_number',
			'street_name',
			'city',
			'address_status',
			'country_name'
		]
    )
}}

with stg__customer as (
	select *
	from {{ source("bookstore-dwh","customer") }} 
),

stg__customer_address as (
	select *
	from {{ source("bookstore-dwh","customer_address") }} 
),

stg__address as (
	select *
	from {{ source("bookstore-dwh","address") }}  
),

stg__address_status as (
	select *
	from {{ source("bookstore-dwh","address_status") }}
),

stg__country as (
	select *
	from {{ source("bookstore-dwh","country") }}
),

dim_customer as (
	select 
		sc.customer_id as nk_customer_id,
		sc.first_name,
		sc.last_name,
		sc.email,
		sa.street_number,
		sa.street_name,
		sa.city,
		sas.address_status,
		sc2.country_name
	from stg__customer sc 
	join stg__customer_address ca  
		on sc.customer_id = ca.customer_id
	join stg__address sa 
		on sa.address_id = ca.address_id
	join stg__address_status as1
		on ca.status_id = as1.status_id
	join stg__country sc3 
		on sc3.country_id = sa.country_id 
),

final_dim_customer as (
	select
		{{ dbt_utils.generate_surrogate_key( ["nk_customer_id"] ) }} as sk_customer_id, 
		* 
	from dim_customer
)

select * from final_dim_customer

{% endsnapshot %}