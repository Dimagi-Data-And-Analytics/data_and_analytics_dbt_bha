with 
dm_table_data_service as (
      select * from  {{ source('dm_table_data', 'CASE_SERVICE') }}
), 

final as (
select case_id service_case_id, INITCAP(replace(a.value::string, '_', ' '), ' ') treatment_type
from dm_table_data_service, lateral flatten(input=> split(treatment_type, ' ')) a
)

select 
	SERVICE_CASE_ID,
	TREATMENT_TYPE
from final