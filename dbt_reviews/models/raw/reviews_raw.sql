{{ config(materialized='view') }}
select * from dbt_reviews.raw_reviews
