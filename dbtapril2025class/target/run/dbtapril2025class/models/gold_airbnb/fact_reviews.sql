-- back compat for old kwarg name
  
  begin;
    
        
            
                
                
            
                
                
            
                
                
            
        
    

    

    merge into DEV.gold_airbnb.fact_reviews as DBT_INTERNAL_DEST
        using DEV.gold_airbnb.fact_reviews__dbt_tmp as DBT_INTERNAL_SOURCE
        on (DBT_INTERNAL_DEST.review_date > DATE('2019-01-01')) and (
                    DBT_INTERNAL_SOURCE.listing_id = DBT_INTERNAL_DEST.listing_id
                ) and (
                    DBT_INTERNAL_SOURCE.reviewer_name = DBT_INTERNAL_DEST.reviewer_name
                ) and (
                    DBT_INTERNAL_SOURCE.review_date = DBT_INTERNAL_DEST.review_date
                )

    
    when matched then update set
        "LISTING_ID" = DBT_INTERNAL_SOURCE."LISTING_ID","REVIEWER_NAME" = DBT_INTERNAL_SOURCE."REVIEWER_NAME","REVIEW_DATE" = DBT_INTERNAL_SOURCE."REVIEW_DATE","REVIEW_SENTIMENT" = DBT_INTERNAL_SOURCE."REVIEW_SENTIMENT","REVIEW_TEXT" = DBT_INTERNAL_SOURCE."REVIEW_TEXT"
    

    when not matched then insert
        ("LISTING_ID", "REVIEWER_NAME", "REVIEW_DATE", "REVIEW_SENTIMENT", "REVIEW_TEXT")
    values
        ("LISTING_ID", "REVIEWER_NAME", "REVIEW_DATE", "REVIEW_SENTIMENT", "REVIEW_TEXT")

;
    commit;