with 
dl_schema_table_data_fixtures_raw as (
      select * from  {{ source('dl_schema_table_data', 'FIXTURES_RAW') }}
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
    dl_schema_table_data_fixtures_raw
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