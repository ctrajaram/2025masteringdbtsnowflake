

-- Get data from the staging model with filtering and deduplication
WITH source_data AS (
    SELECT *
    FROM DEV.silver_airbnb.stg_reviews
    
    -- This filter is applied during incremental runs
    WHERE review_date > DATE('2018-01-01') -- Changed date as requested
    
),

-- Ensure we don't have duplicates
deduplicated AS (
    SELECT
        listing_id,
        reviewer_name,
        review_date,
        MAX(review_sentiment) as review_sentiment,
        MAX(review_text) as review_text
    FROM source_data
    GROUP BY 1, 2, 3
)

SELECT * FROM deduplicated