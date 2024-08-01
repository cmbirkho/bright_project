fact_leads_1 = "DROP TABLE IF EXISTS pbright.fact_leads;"

fact_leads_2 = """
CREATE TABLE pbright.fact_leads
    
    WITH (
      partitioned_by = ARRAY['created_date'],
      format = 'parquet'
    ) AS
    WITH 
    cte AS (
        SELECT
            created_date
            , COUNT(id) AS new_leads_created
            , SUM(CASE WHEN created_date >= DATE_ADD('day', -7, CURRENT_DATE) THEN 1 ELSE 0 END) AS new_leads_last_7_days
            , SUM(CASE WHEN created_date >= DATE_ADD('day', -14, CURRENT_DATE) THEN 1 ELSE 0 END) AS new_leads_last_14_days
            , SUM(CASE WHEN created_date >= DATE_ADD('day', -21, CURRENT_DATE) THEN 1 ELSE 0 END) AS new_leads_last_21_days
            , SUM(CASE WHEN created_date >= DATE_ADD('day', -365, CURRENT_DATE) THEN 1 ELSE 0 END) AS new_leads_last_365_days
            
            , SUM(CASE WHEN converted_date IS NOT NULL THEN 1 ELSE 0 END) AS leads_converted
            , SUM(CASE WHEN converted_date >= DATE_ADD('day', -7, CURRENT_DATE) THEN 1 ELSE 0 END) AS leads_converted_last_7_days
            , SUM(CASE WHEN converted_date >= DATE_ADD('day', -14, CURRENT_DATE) THEN 1 ELSE 0 END) AS leads_converted_last_14_days
            , SUM(CASE WHEN converted_date >= DATE_ADD('day', -21, CURRENT_DATE) THEN 1 ELSE 0 END) AS leads_converted_last_21_days
            , SUM(CASE WHEN converted_date >= DATE_ADD('day', -365, CURRENT_DATE) THEN 1 ELSE 0 END) AS leads_converted_last_365_days
            , SUM(CASE WHEN status = 'Working' THEN 1 ELSE 0 END) AS number_of_open_leads
            , ROUND(AVG(DATE_DIFF('day', converted_date, created_date)), 0) AS avg_days_to_convert
            
        FROM pbright.int_salesforce_leads
        GROUP BY 1
    )
    SELECT 
        new_leads_created
        , new_leads_last_7_days
        , new_leads_last_14_days
        , new_leads_last_21_days
        , new_leads_last_365_days
        , leads_converted
        , leads_converted_last_7_days
        , leads_converted_last_14_days
        , leads_converted_last_21_days
        , leads_converted_last_365_days
        , number_of_open_leads
        , avg_days_to_convert
        , created_date
    FROM cte
;
"""


