
  
    

        create or replace  table DEV.silver_airbnb.stg_listings
         as
        (WITH src_listings AS
( SELECT
*
FROM
    DEV.bronze_airbnb.src_listings
)
SELECT
id AS listing_id, name AS listing_name, listing_url, room_type, minimum_nights, host_id,
price AS price_str, created_at, updated_at
FROM
src_listings
        );
      
  