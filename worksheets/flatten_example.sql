with val  as (;
select
        json,
        json:id::string as form_id,
        json:form."@name"::string as form_name,
        flat_jx.path as json_path, 
        flat_jx.key as key,
        flat_jx.value as value,
        flat_jx.index,
        flat_jx.seq,
        regexp_substr(flat_jx.path, '.*item\[(\d*)\].*')
       /*
        case 
        when flat_jx.this:create_client_profile:case:"@case_id"::string is not null then
            flat_jx.this:create_client_profile:case:"@case_id"::string
        when flat_jx.this:client_save_to_case:case:"@case_id"::string is not null then
            flat_jx.this:client_save_to_case:case:"@case_id"::string
        when flat_jx.this:update_client:case:"@case_id"::string is not null then
            flat_jx.this:update_client:case:"@case_id"::string
        when flat_jx.this:update_client_case:case:"@case_id"::string is not null then
            flat_jx.this:update_client_case:case:"@case_id"::string
        when flat_jx.this:close_client_case:case:"@case_id"::string is not null then
            flat_jx.this:close_client_case:case:"@case_id"::string            
        else null end as case_id,      
       case 
        when flat_jx.this:create_client_profile:case:update:current_status::string is not null then
            flat_jx.this:create_client_profile:case:update:current_status::string
        when flat_jx.this:client_save_to_case:case:update:current_status::string is not null then
            flat_jx.this:client_save_to_case:case:update:current_status::string
        when flat_jx.this:update_client:case:update:current_status::string is not null then
            flat_jx.this:update_client:case:update:current_status::string
        when flat_jx.this:update_client_case:case:update:current_status::string is not null then
            flat_jx.this:update_client_case:case:update:current_status::string
        when flat_jx.this:close_client_case:case:update:current_status::string is not null then
            flat_jx.this:close_client_case:case:update:current_status::string            
        else null end as current_status, 
        
        case
            --when form_name not in ('Outgoing Referral Details', 'Add Additional Referrals') then
            --    flat_j3.value::text 
            --when form_name = 'Escalated Referral Details' then
            --    json:form.default_calculations.central_registry::string   
            --when form_name = 'Create Client Profile and Escalate' then
            --    json:form.client_profile_group.default_calculations.central_registry::string
            --when form_name = 'Create Profile and Refer' then
            --    json:form.client_profile_group.default_calculations.central_registry::string
            when form_name in ('Outgoing Referral Details', 'Add Additional Referrals') then
                'no'
            else 
                case 
                when json:form.default_calculations is not null then
                    json:form.default_calculations.central_registry::string  
                when json:form.client_profile_group.default_calculations is not null then
                    json:form.client_profile_group.default_calculations.central_registry::string   
                else null end 
        end as central_registry*/
      from  DM_CO_CARE_COORD_DEV.DL.FORMS_RAW FR, 
        LATERAL FLATTEN(parse_json(FR.json:form), recursive => True) flat_jx,       
      where 
        (
        /*flat_jx.key = 'create_client_profile'
        or flat_jx.key = 'client_save_to_case'  
        or flat_jx.key = 'update_client' 
        or flat_jx.key = 'update_client_case' 
        or flat_jx.key = 'close_client_case' 
        or*/ flat_jx.key = '@case_id'
        or flat_jx.key = 'central_registry'
        or flat_jx.key = 'current_status'
        or flat_jx.index is not null
        )
      and 
      form_name in (
        'Escalated Referral Details',
        'Create Client Profile and Escalate',
        'Add Additional Referrals',
        'Outgoing Referral Details',
        'Create Profile and Refer'
        );
        --and typeof(f.value) <> 'OBJECT'
        --and f.path not like 'xform_ids%'
/*       and central_registry = 'no'
       and case_id not in  (select case_id  from DM_CO_CARE_COORD_DEV.DM.CASE_CLIENT where closed = true)
       and current_status is not null -- If there were no updates to current status for client. removal, or blanking, should be captured as empty string ('') and not null.
    --group by form_id, form_name, case_id, server_modified_on, current_status, central_registry, json 
    --order by case_id, server_modified_on desc;
    order by form_id, server_modified_on desc*/
)
--, form_case as (
    select distinct form_id, value::string case_id
    from val where key = '@case_id'
--)
;






//// use LATERAL FLATTEN(parse_json(FR.json:form), recursive => True) flat_j with when case ////
  select
        json,
        json:id::string as form_id,
        json:form."@name"::string as form_name,
        flat_jx.path as json_path, 
        --f.this:"@name"::string as form_name2,
        --f.this:resolved_referral.resolved_now.update_client.case."@case_id"::string as cc,
        --case 
        --when flat_j.value::text is not null then
        --    flat_j.value::text
        --else null end as case_id,
       
        case 
        when flat_jx.this:create_client_profile:case:"@case_id"::string is not null then
            flat_jx.this:create_client_profile:case:"@case_id"::string
        when flat_jx.this:client_save_to_case:case:"@case_id"::string is not null then
            flat_jx.this:client_save_to_case:case:"@case_id"::string
        when flat_jx.this:update_client:case:"@case_id"::string is not null then
            flat_jx.this:update_client:case:"@case_id"::string
        when flat_jx.this:update_client_case:case:"@case_id"::string is not null then
            flat_jx.this:update_client_case:case:"@case_id"::string
        when flat_jx.this:close_client_case:case:"@case_id"::string is not null then
            flat_jx.this:close_client_case:case:"@case_id"::string            
        else null end as case_id,      
        /*
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
        */
        json:server_modified_on::datetime as server_modified_on,
        /*
        case 
        when flat_j2.value::text is not null then
            flat_j2.value::text
        else null end as current_status,
        */
       case 
        when flat_jx.this:create_client_profile:case:update:current_status::string is not null then
            flat_jx.this:create_client_profile:case:update:current_status::string
        when flat_jx.this:client_save_to_case:case:update:current_status::string is not null then
            flat_jx.this:client_save_to_case:case:update:current_status::string
        when flat_jx.this:update_client:case:update:current_status::string is not null then
            flat_jx.this:update_client:case:update:current_status::string
        when flat_jx.this:update_client_case:case:update:current_status::string is not null then
            flat_jx.this:update_client_case:case:update:current_status::string
        when flat_jx.this:close_client_case:case:update:current_status::string is not null then
            flat_jx.this:close_client_case:case:update:current_status::string            
        else null end as current_status, 
        
        /*
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
        */
        case
            --when form_name not in ('Outgoing Referral Details', 'Add Additional Referrals') then
            --    flat_j3.value::text 
            --when form_name = 'Escalated Referral Details' then
            --    json:form.default_calculations.central_registry::string   
            --when form_name = 'Create Client Profile and Escalate' then
            --    json:form.client_profile_group.default_calculations.central_registry::string
            --when form_name = 'Create Profile and Refer' then
            --    json:form.client_profile_group.default_calculations.central_registry::string
            when form_name in ('Outgoing Referral Details', 'Add Additional Referrals') then
                'no'
            else 
                case 
                when json:form.default_calculations is not null then
                    json:form.default_calculations.central_registry::string  
                when json:form.client_profile_group.default_calculations is not null then
                    json:form.client_profile_group.default_calculations.central_registry::string   
                else null end 
        end as central_registry
      from  DM_CO_CARE_COORD_DEV.DL.FORMS_RAW FR, 
        --lateral flatten(input => FR.json:form) f,
        --LATERAL FLATTEN(parse_json(FR.json:form), recursive => True) f
        --lateral flatten(JSON:form, recursive=>true) f
        LATERAL FLATTEN(parse_json(FR.json:form), recursive => True) flat_jx,
        --LATERAL FLATTEN(parse_json(FR.json:form), recursive => True) flat_jy,
        --LATERAL FLATTEN(parse_json(FR.json:form), recursive => True) flat_jz,
        --LATERAL FLATTEN(parse_json(FR.json:form), recursive => True) flat_jw,
        --LATERAL FLATTEN(parse_json(FR.json:form), recursive => True) flat_ju,
        --LATERAL FLATTEN(parse_json(FR.json:form), recursive => True) flat_j,
        --LATERAL FLATTEN(parse_json(FR.json:form), recursive => True) flat_j2,
        --LATERAL FLATTEN(parse_json(FR.json:form), recursive => True) flat_j3        
      where 
            (flat_jx.key = 'create_client_profile'
        or flat_jx.key = 'client_save_to_case'  
        or flat_jx.key = 'update_client' 
        or flat_jx.key = 'update_client_case' 
        or flat_jx.key = 'close_client_case' 
        )
        --and flat_jz.key = 'update_client' --and flat_jz.this:update_client:case:"@case_id" is not null
        --and flat_jw.key = 'update_client_case' --and flat_jw.this:update_client_case:case:"@case_id" is not null
        --and flat_ju.key = 'close_client_case' --and flat_ju.this:close_client_case:case:"@case_id" is not null
        --and flat_j.key = '@case_id' and flat_j.this:create is not null
        --and flat_j2.key='current_status'
        --and flat_j3.key='central_registry'
      and form_name in (
        'Escalated Referral Details',
        'Create Client Profile and Escalate',
        'Add Additional Referrals',
        'Outgoing Referral Details',
        'Create Profile and Refer'
        )
        --and typeof(f.value) <> 'OBJECT'
        --and f.path not like 'xform_ids%'
       and central_registry = 'no'
       and case_id not in  (select case_id  from DM_CO_CARE_COORD_DEV.DM.CASE_CLIENT where closed = true)
       and current_status is not null -- If there were no updates to current status for client. removal, or blanking, should be captured as empty string ('') and not null.
    --group by form_id, form_name, case_id, server_modified_on, current_status, central_registry, json 
    --order by case_id, server_modified_on desc;
    order by form_id, server_modified_on desc;


    select 
        json,
        json:id::string as form_id,
        json:form."@name"::string as form_name, -- FORM
        json:server_modified_on::datetime as date_modified,
       flat_jx.path as json_path, 
       case 
        when flat_jx.this:create_referral:case is not null then
            flat_jx.this:create_referral:case:"@case_id"::string
        end as case_id,
        case
        when flat_jx.this:create_referral:case is not null then
            flat_jx.this:create_referral:case:update:current_status::string
        end as current_status,
        /*
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
        */
    from 
        DM_CO_CARE_COORD_DEV.DL.FORMS_RAW FR,
        LATERAL FLATTEN(parse_json(FR.json:form), recursive => True) flat_jx,
        --lateral flatten(input => json:form.clinic_repeat.item) item
    where 
    flat_jx.key = 'create_referral'
    and form_name in ('Create Profile and Refer' ) and current_status is not null
    order by form_id;
//256
    
//// use lateral flatten(input => json:form) f ////
select json, f.this:"@name"::string as form_name2,
    f.this:resolved_referral.resolved_now.update_client.case."@case_id"::string as cc,
    f.this:client_profile_group.client_profile_save_to_case.create_client_profile.case."@case_id"::string as cc2
from DM_CO_CARE_COORD_DEV.DL.FORMS_RAW, 
     lateral flatten(input => json:form) f 
     where form_name2 in ('Escalated Referral Details','Create Client Profile and Escalate');

//// use LATERAL FLATTEN(parse_json(FR.json:form), recursive => True) flat_j, ////
select fr.json, fr.json:form."@name"::string as form_name,
       flat_j.value::text as case_id
from DM_CO_CARE_COORD_DEV.DL.FORMS_RAW FR, 
     LATERAL FLATTEN(parse_json(FR.json:form), recursive => True) flat_j,
     LATERAL FLATTEN(parse_json(FR.json:form), recursive => True) flat_j2,
     LATERAL FLATTEN(parse_json(FR.json:form), recursive => True) flat_j3
     where 
     flat_j.key = '@case_id'
     and flat_j2.key='current_status'
     and flat_j3.key='central_registry'
     and form_name in ('Escalated Referral Details','Create Client Profile and Escalate'); 
     