fact_leads_1 = "DROP TABLE IF EXISTS pbright.fact_leads;"

fact_leads_2 = """
CREATE TABLE pbright.fact_leads
    
    WITH (
      partitioned_by = ARRAY['year', 'month', 'week'],
      format = 'parquet'
    ) AS
    WITH 
    cte AS (
        SELECT
            YEAR(created_date) AS year
            , MONTH(created_date) AS month
            , DATE_TRUNC('week', created_date) AS week
            , COUNT(id) AS number_of_leads_created
            , SUM(CASE WHEN converted_date IS NOT NULL THEN 1 ELSE 0 END) AS number_of_leads_converted
            , SUM(CASE WHEN status = 'Working' THEN 1 ELSE 0 END) AS number_of_open_leads
            , ROUND(AVG(DATE_DIFF('day', converted_date, created_date)), 0) AS avg_days_to_convert
            
        FROM pbright.int_salesforce_leads
        GROUP BY 1, 2, 3
    )
    SELECT 
        number_of_leads_created
        , number_of_leads_converted
        , number_of_open_leads
        , avg_days_to_convert
        , year 
        , month
        , week 
    FROM cte
;
"""


