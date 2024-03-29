with 
dl_table_data_forms as (
      select * from  {{ source('dl_table_data', 'FORMS_RAW') }}
),  
dm_table_data_referral as (
      select * from  {{ source('dm_table_data', 'CASE_REFERRAL') }}
),
cte_outgoing_referral_detail_update as (
    select 
        json,
        json:id::string as form_id,
        json:form."@name"::string as form_name, -- FORM
        json:server_modified_on::datetime as date_modified, 
        item.value:update_referral_case.edit_referral.case."@case_id"::string as case_id,
        item.value:update_referral_case.edit_referral.case.update.current_status::string as current_status
    from 
        DM_CO_CARE_COORD_DEV.DL.FORMS_RAW,
       lateral flatten(input => json:form.referrals_group.referral_repeat.item) item
    where form_name in ('Outgoing Referral Details') 
    and current_status is not null
), 
cte_add_additional_referrals_update as (
     select 
        json,
        json:id::string as form_id,
        json:form."@name"::string as form_name, -- FORM
        json:server_modified_on::datetime as date_modified, 
        item.value:case."@case_id"::STRING AS case_id,
        item.value:case.update.current_status::STRING AS current_status,
    from 
        DM_CO_CARE_COORD_DEV.DL.FORMS_RAW,
       lateral flatten(input => json:form.clinic_repeat.item) item
    where form_name in ('Add Additional Referrals' ) and current_status is not null
)
,
cte_create_profile_and_refer_update as (
    select 
        json,
        json:id::string as form_id,
        json:form."@name"::string as form_name, -- FORM
        json:server_modified_on::datetime as date_modified,
        case 
            when item.value:create_referral_because_not_removed.create_referral.case is not null then
                item.value:create_referral_because_not_removed.create_referral.case."@case_id"::string
            when item.value:create_referral.case is not null then 
                item.value:create_referral.case."@case_id"::string 
        end as case_id,
        case 
            when item.value:create_referral_because_not_removed.create_referral.case is not null then
                item.value:create_referral_because_not_removed.create_referral.case.update.current_status::string
            when item.value:create_referral.case is not null then 
                item.value:create_referral.case.update.current_status::string 
        end as current_status
    from 
        DM_CO_CARE_COORD_DEV.DL.FORMS_RAW,
       lateral flatten(input => json:form.clinic_repeat.item) item
    where form_name in ('Create Profile and Refer' ) and current_status is not null
), 
cte_single_update  as (
select 
    json,
    json:id::string as form_id,
    json:form."@name"::string as form_name, -- FORM
    json:server_modified_on::datetime as date_modified, 
        case 
        when form_name = 'Outgoing Referral Details' then 
            json:form.client_placed_group.other_facility_placed.new_referral.case."@case_id"::string
        when form_name = 'Create Client Profile and Escalate' then 
            json:form.create_referral.create_referral.case."@case_id"::string
        when form_name = 'Respond to Referral Request' then 
            json:form.referral_info.save_to_case_group.update_referral.case."@case_id"::string
        when form_name = 'Escalated Referral Details' then
            json:form.resolved_referral.update_escalation_referral.case."@case_id"::string
        when form_name = 'Create Profile and Refer' then
            json:form.care_coordination.create_referral.case."@case_id"::string
    end as case_id,
    case 
        when form_name = 'Outgoing Referral Details' then 
            json:form.client_placed_group.other_facility_placed.new_referral.case.update.current_status::string
        when form_name = 'Create Client Profile and Escalate' then 
            json:form.create_referral.create_referral.case.update.current_status::string
        when form_name = 'Respond to Referral Request' then 
            json:form.referral_info.save_to_case_group.update_referral.case.update.current_status::string
        when form_name = 'Escalated Referral Details' then
            json:form.resolved_referral.update_escalation_referral.case.update.current_status::string
        when form_name = 'Create Profile and Refer' then
            json:form.care_coordination.create_referral.case.update.current_status::string
    end as current_status
from 
    DM_CO_CARE_COORD_DEV.DL.FORMS_RAW
where form_name in ('Outgoing Referral Details','Create Profile and Refer' ,'Escalated Referral Details', 'Respond to Referral Request', 'Outgoing Referral Details',
'Create Client Profile and Escalate') and case_id is not null
), 

cte_outgoing_referral_second_solo_update as (
select 
    json,
    json:id::string as form_id,
    json:form."@name"::string as form_name, -- FORM
    json:server_modified_on::datetime as date_modified, 
    json:form.contact_support_group.create_care_coordination_team_referral.case."@case_id"::string as case_id,
    json:form.contact_support_group.create_care_coordination_team_referral.case.update.current_status::string as current_status
from 
    DM_CO_CARE_COORD_DEV.DL.FORMS_RAW
where form_name in ('Outgoing Referral Details')
and json:form.contact_support_group.create_care_coordination_team_referral.case is not null
),
cte_data_cleaning_update as (
select 
    json,
    json:id::string as form_id,
    json:form."#type"::string as form_name, -- FORM
    json:server_modified_on::datetime as server_modified_on,
    json:form.case."@case_id"::string as case_id,
    json:form.case.update.current_status::string as current_status
from
    DM_CO_CARE_COORD_DEV.DL.FORMS_RAW
where form_name in (
    'system' 
    ) and current_status is not null
and 
    case_id in (select case_id from DM_CO_CARE_COORD_DEV.DM.CASE_REFERRAL where closed=false)
),
final as (
select * from cte_outgoing_referral_detail_update
union
select * from cte_add_additional_referrals_update
union
select * from cte_create_profile_and_refer_update
union
select * from cte_single_update
union
select * from cte_outgoing_referral_second_solo_update
union
select * from cte_data_cleaning_update
)
select 
-- json, 
form_id, 
-- form_name,
case_id,
date_modified,  
current_status 
from final where case_id not in (select case_id from  DM_CO_CARE_COORD_DEV.DM.CASE_REFERRAL where closed=true)
group by form_id, case_id, date_modified, current_status
order by date_modified desc