with 
dl_schema_table_data_cases_raw_stage as (
      select * from  {{ source('dl_schema_table_data', 'CASES_RAW_STAGE') }}
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
    dl_schema_table_data_cases_raw_stage
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