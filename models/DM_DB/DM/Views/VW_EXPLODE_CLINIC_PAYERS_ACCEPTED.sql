with 
dm_table_data_clinic as (
      select * from  {{ source('dm_table_data', 'CASE_CLINIC') }}
), 
final as (
select case_id CLINIC_CASE_ID, INITCAP(replace(a.value::string, '_', ' '), ' ') PAYERS_ACCEPTED
from dm_table_data_clinic, lateral flatten(input=> split(payers_accepted, ' ')) a
)

select 
	CLINIC_CASE_ID,
	PAYERS_ACCEPTED
from final