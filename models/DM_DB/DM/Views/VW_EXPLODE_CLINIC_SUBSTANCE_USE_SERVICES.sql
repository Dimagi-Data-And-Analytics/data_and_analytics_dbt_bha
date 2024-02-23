with 
dm_table_data_clinic as (
      select * from  {{ source('dm_table_data', 'CASE_CLINIC') }}
), 
final as (
select case_id CLINIC_CASE_ID, INITCAP(replace(a.value::string, '_', ' '), ' ') SUBSTANCE_USE_SERVICES
from dm_table_data_clinic, lateral flatten(input=> split(substance_use_services, ' ')) a
)

select 
	CLINIC_CASE_ID,
	SUBSTANCE_USE_SERVICES
from final