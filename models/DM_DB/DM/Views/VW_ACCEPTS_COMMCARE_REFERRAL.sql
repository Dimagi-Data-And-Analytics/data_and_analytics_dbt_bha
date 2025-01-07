with 
dl_table_data_forms as (
      select * from  {{ source('dl_table_data', 'FORMS_RAW') }}
),
dm_table_data_clinic as (
      select * from  {{ source('dm_table_data', 'CASE_CLINIC') }}
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
        when flat_j.this:update_clinic:case is not null then
             flat_j.this:update_clinic:case:"@case_id"::string
        when flat_j.this:save_to_case:case is not null then
             flat_j.this:flat_j.this:save_to_case:case."@case_id"::string
         when flat_j.this:create_clinic:case is not null then
             flat_j.this:create_clinic:case:"@case_id"::string
        end as case_id,
       
        case
        when flat_j.this:update_clinic:case.update.accepts_commcare_referrals is not null then
             flat_j.this:update_clinic:case.update.accepts_commcare_referrals::string
        when flat_j.this:save_to_case:case.update.accepts_commcare_referrals is not null then
             flat_j.this:save_to_case:case.update.accepts_commcare_referrals::string
        when flat_j.this:create_clinic:case.update.accepts_commcare_referrals is not null then
             flat_j.this:create_clinic:case.update.accepts_commcare_referrals::string
        end as accepts_commcare_referrals
    from dl_table_data_forms FR,
         lateral flatten(parse_json(FR.json:form), recursive => True) flat_j
    where 
     (flat_j.key = 'save_to_case'
      or flat_j.key = 'update_clinic'
      or flat_j.key = 'create_clinic'
     )           
    and accepts_commcare_referrals is not null and accepts_commcare_referrals != ''
    and archived != 'true'
    and case_id not in (select case_id from dm_table_data_clinic where closed=true)
)
select case_id, submission_date as "accepts_commcare_referrals_date_time", accepts_commcare_referrals from final



