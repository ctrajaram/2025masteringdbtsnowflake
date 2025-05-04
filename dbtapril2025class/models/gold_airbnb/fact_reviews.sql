{{
    config(
        materialized='incremental',
        unique_key=['listing_id', 'reviewer_name', 'review_date'],
        incremental_strategy=var('strategy', 'merge')
    )
}}

-- Get data from the staging model with filtering and deduplication
WITH source_data AS (
    SELECT *
    FROM {{ref('stg_reviews')}}
    {% if is_incremental() %}
    -- This filter is applied during incremental runs
    -- Only process records from the last 2 years (the predicate window)
    WHERE review_date > DATE('2019-01-01')
    {% endif %}
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