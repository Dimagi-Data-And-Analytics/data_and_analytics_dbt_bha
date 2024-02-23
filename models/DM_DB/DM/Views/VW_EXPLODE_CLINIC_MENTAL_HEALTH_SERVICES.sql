with 
dm_table_data_clinic as (
      select * from  {{ source('dm_table_data', 'CASE_CLINIC') }}
), 
final as (
select case_id CLINIC_CASE_ID, INITCAP(replace(a.value::string, '_', ' '), ' ') MENTAL_HEALTH_SERVICES
from dm_table_data_clinic, lateral flatten(input=> split(mental_health_services, ' ')) a
)

select 
	CLINIC_CASE_ID,
	MENTAL_HEALTH_SERVICES
from final