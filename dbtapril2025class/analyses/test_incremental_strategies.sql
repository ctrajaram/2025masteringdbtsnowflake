-- This file contains SQL commands to test different incremental strategies
-- Run these commands in Snowflake to simulate updates to review data
-- Then run dbt to see how each incremental strategy handles the changes

-- 1. First, let's update some existing review records (simulating changes)
UPDATE {{ source('bronze_airbnb', 'reviews') }}
SET sentiment = 'positive',
    comments = comments || ' (Updated on ' || CURRENT_DATE() || ')'
WHERE date > DATEADD(year, -1, CURRENT_DATE())
AND sentiment = 'neutral'
LIMIT 10;

-- 2. Insert some new reviews (simulating new data)
-- Note: In a real scenario, you would insert actual data rather than this example
-- This is just to demonstrate the concept
INSERT INTO {{ source('bronze_airbnb', 'reviews') }} (
    listing_id,
    date,
    reviewer_name,
    comments,
    sentiment
)
SELECT
    listing_id,
    CURRENT_DATE() as date,
    'Test User ' || UNIFORM(1, 100, RANDOM()) as reviewer_name,
    'This is a test review added on ' || CURRENT_DATE() as comments,
    CASE WHEN UNIFORM(0, 1, RANDOM()) > 0.5 THEN 'positive' ELSE 'negative' END as sentiment
FROM {{ source('bronze_airbnb', 'reviews') }}
LIMIT 5;

-- After running these updates, run the following dbt commands to test each strategy:
--
-- 1. Using merge strategy (updates existing records and adds new ones):
--    dbt run -s stg_reviews --full-refresh
--    dbt run -s stg_reviews
--
-- 2. Using delete+insert strategy (deletes and re-inserts matched records):
--    dbt run -s stg_reviews_delete_insert --full-refresh
--    dbt run -s stg_reviews_delete_insert
--
-- 3. Using insert_overwrite strategy (overwrites partitions completely):
--    dbt run -s stg_reviews_insert_overwrite --full-refresh
--    dbt run -s stg_reviews_insert_overwrite
--
-- 4. Using append strategy (only adds new records):
--    dbt run -s stg_reviews_append --full-refresh
--    dbt run -s stg_reviews_append
--
-- Compare the results to see how each strategy handled the updates and inserts