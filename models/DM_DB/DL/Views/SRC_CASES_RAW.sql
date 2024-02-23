with 
dl_schema_table_data_cases_raw as (
      select * from  {{ source('dl_schema_table_data', 'CASES_RAW') }}
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
     ,DELETED_FLAG
     ,DELETED_DATE
     ,DELETED_FORMID
FROM 
    dl_schema_table_data_cases_raw
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
	EXECUTION_ID,
	DELETED_FLAG,
	DELETED_DATE,
	DELETED_FORMID
from final
