with 
dl_table_data_forms as (
      select * from  {{ source('dl_table_data', 'FORMS_RAW') }}
),
final as (
    select 
        json,
        json:id::string as form_id,
        json:form."@name"::string as form_name, -- FORM
        flat_j.path as json_path,  
        json:archived::string as archived,
        case
        when flat_j.this:save_to_case:case.update.clinic_availability_last_updated_date_time_raw::string is not null then
          'yes'
          else 'no' end as made_changes,
    from dl_table_data_forms FR,
         lateral flatten(parse_json(FR.json:form), recursive => True) flat_j
    where 
     flat_j.key = 'save_to_case'
              
    and archived != 'true'
    and (form_name = 'Update Bed Availability' or form_name = 'Update Bed Availability - Mobile')
)
,
 changed_forms as (
 select form_id from final where made_changes = 'yes'
 group by 1
)
select  
    F.time_start_form,
    F.time_end_form,
    F.received_on,
    F.form_id
    from {{ ref('VW_FORM_METADATA') }} as F
join changed_forms
on changed_forms.form_id = F.form_id
where (F.form_name = 'Update Bed Availability' or F.form_name = 'Update Bed Availability - Mobile')
and date(F.received_on) > '2024-06-16'  