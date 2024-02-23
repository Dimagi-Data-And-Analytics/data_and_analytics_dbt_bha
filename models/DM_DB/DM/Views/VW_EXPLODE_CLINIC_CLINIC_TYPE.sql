with 
dm_table_data_clinic as (
      select * from  {{ source('dm_table_data', 'CASE_CLINIC') }}
), 
final as (
select case_id CLINIC_CASE_ID, INITCAP(replace(a.value::string, '_', ' '), ' ') CLINIC_TYPE
from dm_table_data_clinic, lateral flatten(input=> split(clinic_type, ' ')) a
)

select 
	CLINIC_CASE_ID,
	CLINIC_TYPE
from final