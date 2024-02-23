with 
dl_schema_table_data_web_users_raw_stage as (
      select * from  {{ source('dl_schema_table_data', 'WEB_USERS_RAW_STAGE') }}
), 

final as (
SELECT 
	DOMAIN,
    METADATA,
    METADATA_FILENAME,
	JSON,
	ID,
	SYSTEM_QUERY_TS,
    TASK_ID,
    EXECUTION_ID
FROM 
    dl_schema_table_data_web_users_raw_stage
)

select 
	DOMAIN,
	METADATA,
	METADATA_FILENAME,
	JSON,
	ID,
	SYSTEM_QUERY_TS,
	TASK_ID,
	EXECUTION_ID
from final