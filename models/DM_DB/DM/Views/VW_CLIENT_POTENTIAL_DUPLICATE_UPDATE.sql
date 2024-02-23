with 
dm_table_data_client as (
      select * from  {{ source('dm_table_data', 'CASE_CLIENT') }}
), 

--closed cases
    cc as (

    select case_id from dm_table_data_client where closed = 1

    ), 
--all potential_duplicate_case_ids flattened
    dupes as
    (
        select distinct case_id, a.value::string as duplicate_case_id, potential_duplicate_case_ids, closed as closed_index 
        from dm_table_data_client, lateral flatten(input=> split(potential_duplicate_case_ids, ' ')) a --where case_id = 'bac987e7-42d9-4a88-a7eb-f47807ef562a'
        order by case_id, a.value::string desc
    ),
-- mark dupes as closed
    ddupes as
    (
        select dupes.case_id as index_case_id, dupes.duplicate_case_id, case when dupes.duplicate_case_id = cc.case_id then 1 else null end as closed_flag, 		closed_index  
        from dupes left join cc on dupes.duplicate_case_id = cc.case_id 
    
    ),

-- only get index_case_ids that have a closed case_id in their current potential_duplicate_case_ids    
    flag as 
    
    ( 
        select distinct index_case_id from ddupes where closed_flag = 1
    
    ),
    

-- get all of the case_ids that have closed duplicate case_ids and their duplicate_case_ids 
final as (
select  ddupes.index_case_id, 
        ddupes.duplicate_case_id, 
        ddupes.closed_flag, 
        ddupes.closed_index
from ddupes 
    inner join flag on ddupes.index_case_id = flag.index_case_id 
    order by ddupes.index_case_id, duplicate_case_id
)

select 
	INDEX_CASE_ID,
	DUPLICATE_CASE_ID,
	CLOSED_FLAG,
	CLOSED_INDEX
from final