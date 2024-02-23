with 
dm_table_data_client as (
      select * from  {{ source('dm_table_data', 'CASE_CLIENT') }}
), 

--closed case
    cc as (

    select case_id, closed as closed_case from dm_table_data_client where closed = 1

    ), 
--all potential_duplicate_index_case_ids flattened
    dupes as
    (
        select distinct case_id, a.value::string as duplicate_index_case_id, potential_duplicate_index_case_ids, closed as match_closed_flag
        from dm_table_data_client, lateral flatten(input=> split(potential_duplicate_index_case_ids, ' ')) a order by case_id, a.value::string desc
    ),
-- mark dupes as deleted
    ddupes as
    (
        select dupes.case_id as index_case_id, dupes.duplicate_index_case_id, case when dupes.duplicate_index_case_id = cc.case_id then 1 else null end as 				closed_flag, match_closed_flag   
        from dupes left join cc on dupes.duplicate_index_case_id = cc.case_id
    
    ),
   

-- only get index_case_ids that have a closed case_id in their current potential_duplicate_case_ids    
    flag as 
    
    (
        select distinct index_case_id from ddupes where closed_flag = 1
    
    ),    

final as (
select  ddupes.index_case_id as match_case_id, 
        ddupes.duplicate_index_case_id as potential_duplicate_index_case_ids, 
        ddupes.closed_flag,
        ddupes.match_closed_flag
        
from ddupes 
    inner join flag on ddupes.index_case_id = flag.index_case_id 
    order by ddupes.index_case_id, duplicate_index_case_id
)

select
	MATCH_CASE_ID,
	POTENTIAL_DUPLICATE_INDEX_CASE_IDS,
	CLOSED_FLAG,
	MATCH_CLOSED_FLAG
from final