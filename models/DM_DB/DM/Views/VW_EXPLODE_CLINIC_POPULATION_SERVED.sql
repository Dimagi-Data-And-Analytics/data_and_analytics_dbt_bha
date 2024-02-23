with 
dm_table_data_clinic as (
      select * from  {{ source('dm_table_data', 'CASE_CLINIC') }}
), 
final as (
select case_id CLINIC_CASE_ID, INITCAP(replace(a.value::string, '_', ' '), ' ') POPULATION_SERVED
from dm_table_data_clinic, lateral flatten(input=> split(population_served, ' ')) a
)

select 
	CLINIC_CASE_ID,
	POPULATION_SERVED
from final