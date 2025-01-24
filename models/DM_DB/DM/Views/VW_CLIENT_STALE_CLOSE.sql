with 
dl_table_data_forms as (
      select * from  {{ source('dl_table_data', 'FORMS_RAW') }}
),
dm_table_data_referral as (
      select * from  {{ source('dm_table_data', 'CASE_REFERRAL') }}
), 
final as (
    select 
        json,
        json:id::string as form_id,
        json:form."@name"::string as form_name, -- FORM
        flat_j.path as json_path, 
        json:server_modified_on::datetime as submission_date, 
        json:archived::string as archived,
        case
        when flat_j.this:case is not null then
             flat_j.this:case:"@case_id"::string
        end as case_id,
        case
        when flat_j.this:case is not null then
             flat_j.this:case:update.cc_closed_because_stale::string
        end as cc_closed_because_stale,
        case
        when flat_j.this:case is not null then
             flat_j.this:case:update.current_status::string
        end as current_status
         from dl_table_data_forms FR,
         lateral flatten(parse_json(FR.json:form), recursive => True) flat_j
        where (flat_j.key = 'case')
       and cc_closed_because_stale is not NULL and cc_closed_because_stale != '' and current_status is not NULL and current_status != ''
       and archived != 'true'
    and case_id not in (select case_id from dm_table_data_referral where closed=true)
)
select case_id, submission_date, cc_closed_because_stale, current_status from final

            


