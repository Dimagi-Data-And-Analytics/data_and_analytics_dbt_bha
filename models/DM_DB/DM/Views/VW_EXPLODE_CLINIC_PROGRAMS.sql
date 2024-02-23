with 
dm_table_data_clinic as (
      select * from  {{ source('dm_table_data', 'CASE_CLINIC') }}
), 
final as (
select case_id CLINIC_CASE_ID, INITCAP(replace(a.value::string, '_', ' '), ' ') PROGRAMS
from dm_table_data_clinic, lateral flatten(input=> split(programs, ' ')) a
)

select 
	CLINIC_CASE_ID,
	PROGRAMS
from final