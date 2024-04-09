with 
dl_table_data_forms as (
      select * from  {{ source('dl_table_data', 'FORMS_RAW') }}
),  
dm_table_data_referral as (
      select * from  {{ source('dm_table_data', 'CASE_REFERRAL') }}
),
cte_all_form_update as (
    select 
        json,
        json:id::string as form_id,
        json:form."@name"::string as form_name, -- FORM
        flat_j.path as json_path, 
        json:server_modified_on::datetime as date_modified, 
        case
        when flat_j.this:edit_referral:case is not null then
             flat_j.this:edit_referral:case:"@case_id"::string
        when flat_j.this:create_referral:case is not null then
             flat_j.this:create_referral:case:"@case_id"::string
        when flat_j.this:referral_save_to_case:case is not null then
             flat_j.this:referral_save_to_case:case:"@case_id"::string
        when flat_j.this:new_referral:case is not null then
             flat_j.this:new_referral:case:"@case_id"::string
        when flat_j.this:update_referral:case is not null then
             flat_j.this:update_referral:case:"@case_id"::string
        when flat_j.this:update_escalation_referral:case is not null then
             flat_j.this:update_escalation_referral:case:"@case_id"::string
        when flat_j.this:create_care_coordination_team_referral:case is not null then
             flat_j.this:create_care_coordination_team_referral:case:"@case_id"::string
        end as case_id,
        
        case
        when flat_j.this:edit_referral:case is not null then
             flat_j.this:edit_referral:case.update.current_status::string
        when flat_j.this:create_referral:case is not null then
             flat_j.this:create_referral:case.update.current_status::string
        when flat_j.this:referral_save_to_case:case is not null then
             flat_j.this:referral_save_to_case:case.update.current_status::string
        when flat_j.this:new_referral:case is not null then
             flat_j.this:new_referral:case.update.current_status::string
        when flat_j.this:update_referral:case is not null then
             flat_j.this:update_referral:case.update.current_status::string
        when flat_j.this:update_escalation_referral:case is not null then
             flat_j.this:update_escalation_referral:case.update.current_status::string
        when flat_j.this:create_care_coordination_team_referral:case is not null then
             flat_j.this:create_care_coordination_team_referral:case.update.current_status::string
        end as current_status
    from dl_table_data_forms FR,
         lateral flatten(parse_json(FR.json:form), recursive => True) flat_j
    where 
     (flat_j.key = 'edit_referral'
      or flat_j.key = 'create_referral'  
      or flat_j.key = 'referral_save_to_case' 
      or flat_j.key = 'new_referral' 
      or flat_j.key = 'update_referral' 
      or flat_j.key = 'update_escalation_referral'
      or flat_j.key = 'create_care_coordination_team_referral'
     )
    and form_name in ('Outgoing Referral Details', 'Add Additional Referrals',
                'Create Profile and Refer', 'Escalated Referral Details', 'Respond to Referral Request',
                'Create Client Profile and Escalate')             
    and current_status is not null
),
cte_system_form_update as (
select 
    json,
    json:id::string as form_id,
    json:form."#type"::string as form_name, -- FORM
    null as json_path,     
    json:server_modified_on::datetime as date_modified,
    json:form.case."@case_id"::string as case_id,
    json:form.case.update.current_status::string as current_status
from dl_table_data_forms
where form_name = 'system' and current_status is not null
and case_id in (select case_id from dm_table_data_referral where closed=false)
),
final as (
  select * from cte_all_form_update
  union
  select * from cte_system_form_update
)
select 
-- json, 
form_id, 
-- form_name,
-- json_path,
case_id,
date_modified,  
current_status 
from final where case_id not in (select case_id from dm_table_data_referral where closed=true)
--group by form_id, case_id, date_modified, current_status, json_path, form_name
order by date_modified desc