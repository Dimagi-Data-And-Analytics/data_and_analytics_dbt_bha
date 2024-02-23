with 
dl_schema_table_data_web_users_raw as (
      select * from  {{ source('dl_schema_table_data', 'WEB_USERS_RAW') }}
), 

final as (
SELECT 
     DOMAIN
     ,METADATA
     ,METADATA_FILENAME
     ,JSON
     ,ID
     ,SYSTEM_QUERY_TS
     ,SYSTEM_CREATE_TS
     ,TASK_ID
     ,EXECUTION_ID
FROM 
    dl_schema_table_data_web_users_raw
)

select
	DOMAIN,
	METADATA,
	METADATA_FILENAME,
	JSON,
	ID,
	SYSTEM_QUERY_TS,
	SYSTEM_CREATE_TS,
	TASK_ID,
	EXECUTION_ID
from final