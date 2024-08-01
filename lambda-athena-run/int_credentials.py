
int_credentials_1 = 'DROP TABLE IF EXISTS pbright.int_credentials;'

int_credentials_2 = """
CREATE TABLE pbright.int_credentials AS
    SELECT 
        name AS licensee_name
        , "credential number" AS license_number
        , status AS license_status 
        , "first issue date" AS license_issued_data
        , "expiration date" AS license_expiration_date
        , "credential type" AS license_type 
        , "disciplinary action" AS disciplinary_action
        , address 
        , state 
        , county 
        , phone 
        , "primary contact name" AS primary_contact_name 
        , "primary contact role" AS primary_contact_role
        , CURRENT_DATE AS ingestion_date_utc
    FROM pbright.raw_source_1
;

"""
