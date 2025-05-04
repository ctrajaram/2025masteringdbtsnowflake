WITH src_hosts AS (
    SELECT
*
FROM
      DEV.bronze_airbnb.src_hosts
)
SELECT
id AS host_id,  
NAME AS host_name, is_superhost, created_at, updated_at
FROM
src_hosts