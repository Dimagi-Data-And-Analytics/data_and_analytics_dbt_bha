with 
dl_table_data_forms as (
      select * from  {{ source('dl_table_data', 'FORMS_RAW') }}
),  
dm_table_data_client as (
      select * from  {{ source('dm_table_data', 'CASE_CLIENT') }}
),
cte_updates_from_user_forms as (
    select
        json,
        json:id::string as form_id,
        json:form."@name"::string as form_name,
        case
            when form_name = 'Escalated Referral Details' then
                json:form.resolved_referral.resolved_now.update_client.case."@case_id"::string
            when form_name = 'Create Client Profile and Escalate' then
                json:form.client_profile_group.client_profile_save_to_case.create_client_profile.case."@case_id"::string 
            when form_name = 'Add Additional Referrals' then
                json:form.client_going_from_withdrawn_to_open.client_save_to_case.case."@case_id"::string
            when form_name = 'Outgoing Referral Details' then
                case 
                    when json:form.client_placed_group.update_client_case is not null then
                        json:form.client_placed_group.update_client_case.case."@case_id"::string
                    when json:form.contact_support_group.update_client_case is not null then
                        json:form.contact_support_group.update_client_case.case."@case_id"::string
                    when json:form.referrals_group.update_client_current_status_withdrawn is not null then
                        json:form.referrals_group.update_client_current_status_withdrawn.update_client_case.case."@case_id"::string
                    when json:form.close_client_profile.close_client_case is not null then
                        json:form.close_client_profile.close_client_case.case."@case_id"::string
                end 
             when form_name = 'Create Profile and Refer' then
                json:form.client_profile_group.client_profile_save_to_case.create_client_profile.case."@case_id"::string
        else null end as case_id,
        json:server_modified_on::datetime as server_modified_on,
        case
            when form_name = 'Escalated Referral Details' then
                json:form.resolved_referral.resolved_now.update_client.case.update.current_status::string
            when form_name = 'Create Client Profile and Escalate' then
                json:form.client_profile_group.client_profile_save_to_case.create_client_profile.case.update.current_status::string 
            when form_name = 'Add Additional Referrals' then
                json:form.client_going_from_withdrawn_to_open.client_save_to_case.case.update.current_status::string
            when form_name = 'Outgoing Referral Details' then
                case 
                    when json:form.client_placed_group.update_client_case is not null then
                        json:form.client_placed_group.update_client_case.case.update.current_status::string
                    when json:form.contact_support_group.update_client_case is not null then
                        json:form.contact_support_group.update_client_case.case.update.current_status::string
                    when json:form.referrals_group.update_client_current_status_withdrawn is not null then
                        json:form.referrals_group.update_client_current_status_withdrawn.update_client_case.case.update.current_status::string
                    when json:form.close_client_profile is not null then
                        json:form.close_client_profile.close_client_case.case.update.current_status::string
                end 
            when form_name = 'Create Profile and Refer' then
                json:form.client_profile_group.client_profile_save_to_case.create_client_profile.case.update.current_status::string
            end as current_status,
        case
            when form_name = 'Escalated Referral Details' then
                json:form.default_calculations.central_registry::string   
            when form_name = 'Create Client Profile and Escalate' then
                json:form.client_profile_group.default_calculations.central_registry::string
            when form_name = 'Create Profile and Refer' then
                json:form.client_profile_group.default_calculations.central_registry::string
            when form_name in ('Outgoing Referral Details', 'Add Additional Referrals') then
                'no'
        else null end as central_registry
      from  dl_table_data_forms
      where form_name in (
        'Escalated Referral Details',
        'Create Client Profile and Escalate',
        'Add Additional Referrals',
        'Outgoing Referral Details',
        'Create Profile and Refer'
        )
       and central_registry = 'no'
       and case_id not in  (select case_id  from dm_table_data_client where closed = true)
       and current_status is not null -- If there were no updates to current status for client. removal, or blanking, should be captured as empty string ('') and not null.
    order by case_id, server_modified_on desc
    
), 
cte_updates_from_data_cleaning as ( 
select 
    json,
    json:id::string as form_id,
    json:form."#type"::string as form_name, -- FORM
    json:form.case."@case_id"::string as case_id,
    json:server_modified_on::datetime as server_modified_on,
    json:form.case.update.current_status::string as current_status,
    'no' as central_registry
from
    dl_table_data_forms
where form_name in (
    'system' 
    )
and 
    case_id in (select case_id from dm_table_data_client where closed=false and central_registry='no')
), 
cte_unified_updates as (
    select * from cte_updates_from_user_forms 
    union
    select * from cte_updates_from_data_cleaning
),
cte_lag_current_status as (
    select 
        json,
        form_id, 
        form_name,
        case_id,
        server_modified_on,
        current_status, 
        lag(current_status) over (partition by case_id order by server_modified_on) as prev_current_status
        from cte_unified_updates
        order by case_id
) 
select 
    -- json,
    form_id, 
    -- form_name,
    case_id,
    server_modified_on,
    current_status  
from cte_lag_current_status 
where prev_current_status is null or current_status != prev_current_status
order by case_id, server_modified_on desc
