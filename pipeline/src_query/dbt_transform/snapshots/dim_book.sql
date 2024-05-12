{% snapshot dim_book %}

{{
    config(
      target_database='bookstore-dwh',
      target_schema='final',
      unique_key='sk_book_id',

      strategy='check',
      check_cols=[
            'title',
            'publication_date',
            'author_name',
            'language_name',
            'publisher'
		]
    )
}}

with stg__book as (
    select 
        book_id,
        title,
        publication_date
    from {{ source("bookstore-dwh","book") }}
),

stg__book_author as (
    select 
        book_id,
        author_id
    from {{ source("bookstore-dwh","book_author") }}
),

stg__author as (
    select 
        author_id,
        author_name
    from {{ source("bookstore-dwh","author") }}
),

stg__book_language as (
    select 
        language_id,
        language_name
    from {{ source("bookstore-dwh","book_language") }}
),

stg__publisher as (
    select 
        pusblisher_id,
        publisher_name
    from {{ source("bookstore-dwh","publisher") }}
),

dim_book as (
    select 
        sb.book_id as nk_book_id,
        sb.title,
        sb.publication_date,
        sa.author_name,
        sbl.language_name,
        sp.publisher_name
    from 
        stg__book sb 
    join stg__book_author sba 
        on sb.book_id = sba.book_id 
    join stg__author sa 
        on sa.author_id = sba.author_id 
    join stg__book_language sbl
        on sbl.language_id = sb.language_id 
    join stg__publisher sp
        on sp.publisher_id = sb.publisher_id 
),

final_dim_book as (
    select
		{{ dbt_utils.generate_surrogate_key( ["nk_book_id"] ) }} as sk_book_id, 
		* 
    from dim_book
)

select * from final_dim_book

{% endsnapshot %}