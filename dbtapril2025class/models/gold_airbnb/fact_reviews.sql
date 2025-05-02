
{{config(materialized='table')}}


select * from {{ref('dbtapril2025class','stg_reviews')}}