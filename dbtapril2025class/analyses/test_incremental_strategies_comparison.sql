-- Analysis file for testing incremental strategies with fact_reviews
-- This file will help you test and understand different incremental strategies
-- and see how incremental predicates affect which records get updated

--------------------------------------------------------------
-- STEP 1: PREPARE TEST DATA
--------------------------------------------------------------

-- 1A: Update records INSIDE the 2-year window (2019-01-01 to 2021-12-31)
--     These records SHOULD be updated by all incremental strategies
UPDATE {{ source('bronze_airbnb', 'reviews') }}
SET sentiment = 'inside_window_positive',
    comments = comments || ' [Inside 2-year window test]'
WHERE date > DATE('2019-01-01')  -- Inside the 2-year window (2019-2021)
AND date <= DATE('2021-12-31')   -- Up to the most recent year
AND sentiment = 'neutral'
AND listing_id IN ('49897931',
'49034569',
'39022709',
'49070135',
'37558386'
);

-- 1B: Update records OUTSIDE the 2-year window but INSIDE the 3-year is_incremental() filter
--     These records should be EXCLUDED by the incremental predicate
UPDATE {{ source('bronze_airbnb', 'reviews') }}
SET sentiment = 'outside_window_negative',
    comments = comments || ' [Outside 2-year window test]'
WHERE date <= DATE('2019-01-01')  -- Outside the 2-year predicate window
AND date > DATE('2018-01-01')     -- But inside the 3-year is_incremental() filter
AND sentiment = 'neutral'
AND listing_id IN ('10122555',
'20858',
'22677',
'22099180',
'27869338'
);

-- 1C: Add some completely new records with 2021 dates (these will be added by all strategies)
INSERT INTO {{ source('bronze_airbnb', 'reviews') }} (
    listing_id,
    date,
    reviewer_name,
    comments,
    sentiment
)
VALUES
('49897931', DATE('2021-12-15'), 'Test User 1', 'New test review for incremental testing', 'new_record_positive'),
('49034569', DATE('2021-12-15'), 'Test User 2', 'New test review for incremental testing', 'new_record_positive'),
('39022709', DATE('2021-12-15'), 'Test User 3', 'New test review for incremental testing', 'new_record_positive');

--------------------------------------------------------------
-- STEP 2: TEST INSTRUCTIONS
--------------------------------------------------------------

-- After setting up the test data, follow these steps:
--
-- 1. First, build the fact_reviews with a full refresh using the merge strategy:
--    dbt run -s fact_reviews --full-refresh --vars 'strategy: merge'
--
-- 2. Then run the incremental update with each strategy and compare results:
--
--    a) Test MERGE strategy:
--       dbt run -s fact_reviews --vars 'strategy: merge'
--
--    b) Test DELETE+INSERT strategy:
--       dbt run -s fact_reviews --vars 'strategy: delete_insert'
--
--    c) Test APPEND strategy:
--       dbt run -s fact_reviews --vars 'strategy: append'
--
-- 3. After each run, execute the verification queries below to see which
--    records were updated and which were ignored due to the predicate

--------------------------------------------------------------
-- STEP 3: VERIFICATION QUERIES
--------------------------------------------------------------

/*
-- After running each incremental strategy, run this query to see the differences:

WITH strategy_results AS (
    SELECT
        'INSIDE 2-year window (should be updated)' as record_type,
        review_date,
        review_sentiment,
        SUBSTRING(review_text, 1, 100) as review_text_sample
    FROM DEV.gold_airbnb.fact_reviews
    WHERE review_sentiment = 'inside_window_positive'

    UNION ALL

    SELECT
        'OUTSIDE 2-year window (should NOT be updated)' as record_type,
        review_date,
        review_sentiment,
        SUBSTRING(review_text, 1, 100) as review_text_sample
    FROM DEV.gold_airbnb.fact_reviews
    WHERE review_sentiment = 'outside_window_negative'

    UNION ALL

    SELECT
        'NEW records (should be added by all strategies)' as record_type,
        review_date,
        review_sentiment,
        SUBSTRING(review_text, 1, 100) as review_text_sample
    FROM DEV.gold_airbnb.fact_reviews
    WHERE review_sentiment = 'new_record_positive'
)

SELECT
    record_type,
    COUNT(*) as record_count
FROM strategy_results
GROUP BY record_type
ORDER BY record_type;

-- To see individual records for more detailed analysis:
SELECT
    review_date,
    listing_id,
    reviewer_name,
    review_sentiment,
    SUBSTRING(review_text, 1, 100) as review_text_sample
FROM DEV.gold_airbnb.fact_reviews
WHERE review_sentiment IN ('inside_window_positive', 'outside_window_negative', 'new_record_positive')
ORDER BY review_date DESC;
*/

--------------------------------------------------------------
-- STEP 4: EXPECTED RESULTS FOR EACH STRATEGY
--------------------------------------------------------------

/*
Explanation of expected results for each strategy:

1. MERGE Strategy:
   - Records INSIDE 2-year window: WILL be updated
   - Records OUTSIDE 2-year window: Will NOT be updated (due to incremental predicate)
   - New records: Will be added

2. DELETE+INSERT Strategy:
   - Records INSIDE 2-year window: WILL be updated
   - Records OUTSIDE 2-year window: Will NOT be updated (due to incremental predicate)
   - New records: Will be added

3. APPEND Strategy:
   - Records INSIDE 2-year window: Will NOT be updated (append never updates)
   - Records OUTSIDE 2-year window: Will NOT be updated (due to both append strategy and predicate)
   - New records: Will be added

Key differences:
- The merge strategy updates existing records within the predicate window
- The delete+insert strategy deletes and re-inserts records within the predicate window
- The append strategy only adds new records, never updates existing ones

The incremental predicate of 2 years ensures that for ALL strategies, records older than 2 years
are not processed at all, even if they match the 3-year filter in the SQL. This improves
performance by limiting the scope of the incremental run.
*/