with 
dm_table_data_client as (
      select * from  {{ source('dm_table_data', 'CASE_CLIENT') }}
), 
dm_table_data_alias as (
      select * from  {{ source('dm_table_data', 'CASE_ALIAS') }}
), 
final as (
select 
    ct.case_id as client_case_id 
    , ca.case_id as alias_case_id
    , ct.date_opened as client_date_opened
    , ct.date_closed as client_date_closed
    , ct.date_modified as client_date_modified
    , ct.social_security_number as client_social_security_number
    , ct.closed_by as client_closed_by
    , ct.closed as client_closed
    , ct.case_name as client_case_name
    , ct.last_name as client_last_name
    , ct.first_name as client_first_name
    , ct.dob as client_dob 
    , ca.case_name as alias_name
    , ca.date_opened as alias_date_opened
    , ca.date_closed as alias_date_closed
    , ca.date_modified as alias_date_modified
    
from dm_table_data_client ct left join dm_table_data_alias ca on ca.parent_case_id = ct.case_id)

select 
	CLIENT_CASE_ID,
	ALIAS_CASE_ID,
	CLIENT_DATE_OPENED,
	CLIENT_DATE_CLOSED,
	CLIENT_DATE_MODIFIED,
	CLIENT_SOCIAL_SECURITY_NUMBER,
	CLIENT_CLOSED_BY,
	CLIENT_CLOSED,
	CLIENT_CASE_NAME,
	CLIENT_LAST_NAME,
	CLIENT_FIRST_NAME,
	CLIENT_DOB,
	ALIAS_NAME,
	ALIAS_DATE_OPENED,
	ALIAS_DATE_CLOSED,
	ALIAS_DATE_MODIFIED
from final