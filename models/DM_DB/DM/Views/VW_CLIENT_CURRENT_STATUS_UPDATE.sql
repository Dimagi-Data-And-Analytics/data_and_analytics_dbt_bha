with 
dl_table_data_forms as (
      select * from  {{ source('dl_table_data', 'FORMS_RAW') }}
),  
dm_table_data_client as (
      select * from  {{ source('dm_table_data', 'CASE_CLIENT') }}
),
cte_all_form_update as (
    select
        json,
        json:id::string as form_id,
        json:form."@name"::string as form_name,
        flat_j.path as json_path, 
        json:server_modified_on::datetime as date_modified,
        case 
        when flat_j.this:create_client_profile:case:"@case_id"::string is not null then
            flat_j.this:create_client_profile:case:"@case_id"::string
        when flat_j.this:client_save_to_case:case:"@case_id"::string is not null then
            flat_j.this:client_save_to_case:case:"@case_id"::string
        when flat_j.this:update_client:case:"@case_id"::string is not null then
            flat_j.this:update_client:case:"@case_id"::string
        when flat_j.this:update_client_case:case:"@case_id"::string is not null then
            flat_j.this:update_client_case:case:"@case_id"::string
        when flat_j.this:close_client_case:case:"@case_id"::string is not null then
            flat_j.this:close_client_case:case:"@case_id"::string            
        else null end as case_id,   
        
       case 
        when flat_j.this:create_client_profile:case:update:current_status::string is not null then
            flat_j.this:create_client_profile:case:update:current_status::string
        when flat_j.this:client_save_to_case:case:update:current_status::string is not null then
            flat_j.this:client_save_to_case:case:update:current_status::string
        when flat_j.this:update_client:case:update:current_status::string is not null then
            flat_j.this:update_client:case:update:current_status::string
        when flat_j.this:update_client_case:case:update:current_status::string is not null then
            flat_j.this:update_client_case:case:update:current_status::string
        when flat_j.this:close_client_case:case:update:current_status::string is not null then
            flat_j.this:close_client_case:case:update:current_status::string            
        else null end as current_status, 

        case
            when form_name in ('Outgoing Referral Details', 'Add Additional Referrals') then 'no'
            else 
                case 
                when json:form.default_calculations is not null then
                    json:form.default_calculations.central_registry::string  
                when json:form.client_profile_group.default_calculations is not null then
                    json:form.client_profile_group.default_calculations.central_registry::string   
                else null end 
        end as central_registry
      from  dl_table_data_forms FR, 
            LATERAL FLATTEN(parse_json(FR.json:form), recursive => True) flat_j
      where 
       (flat_j.key = 'create_client_profile'
        or flat_j.key = 'client_save_to_case'  
        or flat_j.key = 'update_client' 
        or flat_j.key = 'update_client_case' 
        or flat_j.key = 'close_client_case' 
       )       
      and form_name in ('Escalated Referral Details', 'Create Client Profile and Escalate',
                        'Add Additional Referrals', 'Outgoing Referral Details','Create Profile and Refer')
       and central_registry = 'no'
       and case_id not in  (select case_id  from dm_table_data_client where closed = true)
       and current_status is not null -- If there were no updates to current status for client. removal, or blanking, should be captured as empty string ('') and not null.
    order by case_id, date_modified desc
), 
cte_system_form_update as ( 
select 
    json,
    json:id::string as form_id,
    json:form."#type"::string as form_name, -- FORM
    null as json_path,   
    json:server_modified_on::datetime as date_modified,
    json:form.case."@case_id"::string as case_id,
    json:form.case.update.current_status::string as current_status,
    'no' as central_registry
from dl_table_data_forms
where form_name ='system' 
and case_id in (select case_id from dm_table_data_client where closed=false and central_registry='no')
), 
cte_unified_updates as (
    select * from cte_all_form_update 
    union
    select * from cte_system_form_update
),
final as (
    select 
        json,
        form_id, 
        form_name,
        json_path,
        case_id,
        date_modified,
        current_status, 
        lag(current_status) over (partition by case_id order by date_modified) as prev_current_status
        from cte_unified_updates
        order by case_id
) 
select 
    -- json,
    form_id, 
    -- form_name,
    -- json_path,
    case_id,
    date_modified,
    current_status  
from final 
where prev_current_status is null or current_status != prev_current_status
order by case_id, date_modified desc