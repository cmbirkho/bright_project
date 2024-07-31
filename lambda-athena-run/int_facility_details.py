
int_facility_details_1 = 'DROP TABLE IF EXISTS pbright.int_facility_details;'

int_facility_details_2 = """
CREATE TABLE int_facility_details AS 
    
    WITH 
    cte AS (
        SELECT
            company
            , "type license" AS license_type
            , address1
            , address2
            , city 
            , state 
            , zip 
            , phone 
            , email
            , CASE WHEN UPPER("accepts subsidy") = 'ACCEPTS SUBSIDY' THEN 'Y' ELSE 'N' END AS accepts_financial_aid
            , "year round" AS year_round -- could be curriculum_type
            , CASE 
                WHEN UPPER(REGEXP_EXTRACT("Ages Accepted 1", '^([^(]+)')) LIKE '%TODDLERS%' THEN 'Toddlers'
                WHEN UPPER(REGEXP_EXTRACT("Ages Accepted 1", '^([^(]+)')) LIKE '%PRESCHOOL%' THEN 'Preschool'
                WHEN UPPER(REGEXP_EXTRACT("Ages Accepted 1", '^([^(]+)')) LIKE '%SCHOOL-AGE%' THEN 'School-Age'
                WHEN UPPER(REGEXP_EXTRACT("Ages Accepted 1", '^([^(]+)')) LIKE '%INFANTS%' THEN 'Infants'
                END AS age_type
            , CONCAT_WS(', ', 
            CASE 
                WHEN REGEXP_LIKE("Mon", '\d') THEN 'Mon: Open' 
                ELSE 'Mon: Closed' 
            END,
            CASE 
                WHEN REGEXP_LIKE("Tues", '\d') THEN 'Tues: Open' 
                ELSE 'Tues: Closed' 
            END,
            CASE 
                WHEN REGEXP_LIKE("Wed", '\d') THEN 'Wed: Open' 
                ELSE 'Wed: Closed' 
            END,
            CASE 
                WHEN REGEXP_LIKE("Thurs", '\d') THEN 'Thurs: Open' 
                ELSE 'Thurs: Closed' 
            END,
            CASE 
                WHEN REGEXP_LIKE("Friday", '\d') THEN 'Fri: Open' 
                ELSE 'Fri: Closed' 
            END,
            CASE 
                WHEN REGEXP_LIKE("Saturday", '\d') THEN 'Sat: Open' 
                ELSE 'Sat: Closed' 
            END,
            CASE 
                WHEN REGEXP_LIKE("Sunday", '\d') THEN 'Sun: Open' 
                ELSE 'Sun: Closed' 
            END
        ) AS schedule
        
        FROM raw_source_2
    )
    SELECT * 
        , CASE 
            WHEN age_type = 'Toddlers' THEN 12 
            WHEN age_type = 'Preschool' THEN 24 
            WHEN age_type = 'School-Age' THEN 60
            WHEN age_type = 'Infants' THEN 0
            END AS min_age_months
        , CASE 
            WHEN age_type = 'Toddlers' THEN 23 
            WHEN age_type = 'Preschool' THEN 48 
            WHEN age_type = 'School-Age' THEN NULL
            WHEN age_type = 'Infants' THEN 11
            END AS max_age_months
    FROM cte
;

"""
