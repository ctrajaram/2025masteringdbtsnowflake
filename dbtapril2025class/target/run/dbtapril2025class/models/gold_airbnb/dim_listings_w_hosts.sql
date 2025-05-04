
  
    

        create or replace  table DEV.gold_airbnb.dim_listings_w_hosts
         as
        (

with __dbt__cte__dim_listings as (


SELECT
  listing_id,
  listing_name,
  room_type,
  CASE
    WHEN minimum_nights = 0 THEN 1
    ELSE minimum_nights
  END AS minimum_nights,
  host_id,
  REPLACE( price_str, '$') :: NUMBER(10,2) AS price,
  created_at,
  updated_at
FROM
  DEV.silver_airbnb.stg_listings
),  __dbt__cte__dim_hosts as (


SELECT
    host_id,
    NVL( host_name, 'Anonymous') AS host_name,
    is_superhost,
    created_at,
    updated_at
FROM
    DEV.silver_airbnb.stg_hosts
) SELECT
    l.listing_id,
    l.listing_name,
    l.room_type,
    l.minimum_nights,
    l.price,
    l.host_id,
    h.host_name,
    h.is_superhost as host_is_superhost,
    l.created_at,
    GREATEST(l.updated_at, h.updated_at) as updated_at
FROM __dbt__cte__dim_listings l
LEFT JOIN __dbt__cte__dim_hosts h ON (h.host_id = l.host_id)
        );
      
  