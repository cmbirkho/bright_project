
int_operations_1 = 'DROP TABLE IF EXISTS pbright.int_operations;'

int_operations_2 = """
CREATE TABLE pbright.int_operations AS 

    SELECT 
        operation AS operator 
        , type AS facility_type 
        , "facility id" AS facility_id
        , "operation name" AS operation_name
        , status 
        , address 
        , city 
        , state 
        , zip 
        , county
        , phone
        , CASE 
            WHEN REGEXP_LIKE("email address", '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$') THEN "email address"
            END AS email_address
        , CAST(try(parse_datetime("issue date", 'MM/dd/yy')) AS DATE) AS issued_date
        , capacity
        , CASE 
            WHEN infant = 'Y' THEN 'Infants'
            WHEN toddler = 'Y' THEN 'Toddlers'
            WHEN preschool = 'Y' THEN 'Preschool'
            WHEN school = 'Y' THEN 'School-Age'
            END AS age_type
        
    FROM pbright.raw_source_3

;
"""
