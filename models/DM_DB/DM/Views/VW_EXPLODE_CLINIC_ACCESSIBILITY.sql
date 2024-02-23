with 
dm_table_data_clinic as (
      select * from  {{ source('dm_table_data', 'CASE_CLINIC') }}
), 
final as (
select case_id CLINIC_CASE_ID, INITCAP(replace(a.value::string, '_', ' '), ' ') ACCESSIBILITY
from dm_table_data_clinic, lateral flatten(input=> split(accessibility, ' ')) a
)

select 
	CLINIC_CASE_ID,
	ACCESSIBILITY
from final