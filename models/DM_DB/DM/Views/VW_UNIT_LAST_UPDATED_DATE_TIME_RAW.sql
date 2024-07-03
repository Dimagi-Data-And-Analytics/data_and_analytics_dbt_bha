with 
dl_table_data_forms as (
      select * from  {{ source('dl_table_data', 'FORMS_RAW') }}
),  
dm_table_data_unit as (
      select * from  {{ source('dm_table_data', 'CASE_UNIT') }}
)
,final as (
    select 
        json,
        json:id::string as form_id,
        json:form."@name"::string as form_name, -- FORM
        flat_j.path as json_path, 
        json:server_modified_on::datetime as submission_date, 
        json:archived::string as archived,
        case
        when flat_j.this:update_unit_case:case is not null then
             flat_j.this:update_unit_case:case:"@case_id"::string
        when flat_j.this:update_unit_timestamp:case is not null then
             flat_j.this:update_unit_timestamp:case:"@case_id"::string
        when flat_j.this:save_to_case:case is not null then
             flat_j.this:save_to_case:case:"@case_id"::string
        end as case_id,
       
        case
        when flat_j.this:update_unit_case:case is not null then
             flat_j.this:update_unit_case:case.update.last_updated_date_time_raw::string
        when flat_j.this:update_unit_timestamp:case is not null then
             flat_j.this:update_unit_timestamp:case.update.last_updated_date_time_raw::string
        when flat_j.this:save_to_case:case is not null then
             flat_j.this:save_to_case:case.update.last_updated_date_time_raw::string
        end as last_updated_date_time_raw
    from dl_table_data_forms FR,
         lateral flatten(parse_json(FR.json:form), recursive => True) flat_j
    where 
     (flat_j.key = 'update_unit_case'
      or flat_j.key = 'update_unit_timestamp'  
      or flat_j.key = 'save_to_case' 
     )           
    and last_updated_date_time_raw is not null
    and archived != 'true'
    and case_id not in (select case_id from dm_table_data_unit where closed=true)
)

select 
-- json, 
form_id, 
-- form_name,
-- json_path,
case_id,
submission_date,  
last_updated_date_time_raw 
from final 
order by submission_date desc