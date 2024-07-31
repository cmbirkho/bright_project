# -- Done - need to check mobile_phone for email address and if valid then replace into `email`
# -- Done - change is_converted to `converted_date`
# -- Done - make `last_modified_date`, `created_date`, `last_activity_date`, `last_referenced_date` all dates
# -- Needed - create a `days_to_convert` field 
# -- Done - clean `status`
# -- Needed - need logic to clean email_bouned_reason, email_bouced_date, outreach_stage_c
# -- Done - create a clean `website_address` field

# -- note the order of the CASE WHEN statements is important because we are using the same 
# --- column names on the outputs of these statements, so the statement after is referencing the changed data


int_salesforce_leads_1 = "DROP TABLE IF EXISTS pbright.int_salesforce_leads;"

int_salesforce_leads_2 = """
CREATE TABLE pbright.int_salesforce_leads
    
    WITH (
      partitioned_by = ARRAY['created_date'],
      format = 'parquet'
    ) AS
    SELECT 
        id 
        , first_name 
        , last_name 
        , title 
        , company 
        , phone 
        
        , CASE 
            WHEN REGEXP_LIKE(mobile_phone, '^\+?[1-9]\d{1,14}$|^(\(\d{3}\)\s*|\d{3}[-.\s]?)?\d{3}[-.\s]?\d{4}$') THEN mobile_phone
            ELSE NULL END AS mobile_phone
        , CASE
            WHEN REGEXP_LIKE(mobile_phone, '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$') THEN mobile_phone
            WHEN REGEXP_LIKE(email, '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$') THEN email
            ELSE NULL END AS email
        , street
        , city 
        , state
        , postal_code
        , CASE 
            WHEN UPPER(lead_source) IN ('WORKING', 'ASSIGNED', 'UNQUALIFIED', 'CONNECTED', 'RECYCLED') THEN lead_source
            ELSE NULL END AS status
        , CASE 
            WHEN NOT regexp_like(website, '^(https?://)?([a-zA-Z0-9-]+\.)+[a-zA-Z]{2,6}(/.*)?$') THEN website
            ELSE NULL END AS lead_source
        , CASE 
            WHEN regexp_like(website, '^(https?://)?([a-zA-Z0-9-]+\.)+[a-zA-Z]{2,6}(/.*)?$') THEN website
            ELSE NULL END AS website
        , cast(coalesce(
                try(parse_datetime(is_converted, 'MM/dd/yy HH:mm')),
                try(parse_datetime(is_converted, 'MM/dd/yy'))) AS date) AS converted_date
        , cast(coalesce(
                try(parse_datetime(last_modified_date, 'MM/dd/yy HH:mm')),
                try(parse_datetime(last_modified_date, 'MM/dd/yy'))) AS date) AS last_modified_date
        , cast(coalesce(
                try(parse_datetime(last_activity_date, 'MM/dd/yy HH:mm')),
                try(parse_datetime(last_activity_date, 'MM/dd/yy'))) AS date) AS last_activity_date
        , cast(coalesce(
                try(parse_datetime(last_referenced_date, 'MM/dd/yy HH:mm')),
                try(parse_datetime(last_referenced_date, 'MM/dd/yy'))) AS date) AS last_referenced_date
        , cast(coalesce(
                try(parse_datetime(created_date, 'MM/dd/yy HH:mm')),
                try(parse_datetime(created_date, 'MM/dd/yy'))) AS date) AS created_date
        
    FROM pbright.raw_salesforce_leads
    WHERE is_deleted = false
;
"""