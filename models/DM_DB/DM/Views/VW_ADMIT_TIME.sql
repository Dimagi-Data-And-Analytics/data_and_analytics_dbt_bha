with 
dl_table_data_forms as (
      select * from  {{ source('dl_table_data', 'FORMS_RAW') }}
),
admitted_service_forms as (
    select 
        json,
        json:id::string as form_id,
        flat_j.key,
        json:form."@name"::string as form_name, -- FORM
        json:form."@xmlns"::string as xmlns,
        flat_j.path as json_path,  
        json:archived::string as archived,
        case
        when flat_j.this:create_service_case.create_service_case.case.update.current_status::string is not null then
          'yes'
          else 'no' end as made_changes,
    from dl_table_data_forms FR,
         lateral flatten(parse_json(FR.json:form), recursive => True) flat_j
    where 
        flat_j.key = 'create_service_case'
        -- there are two forms named "Admit Client" forms, but we do want both
        -- 1. http://openrosa.org/formdesigner/56C13D64-E285-476A-A6D5-6210CB5FDDA6
        -- 2. http://openrosa.org/formdesigner/8BA2FC08-83B2-44D2-97F9-6F70217E04D7
        and form_name = 'Admit Client'
        and made_changes = 'yes'
        and archived = false
)
select  
    F.time_start_form,
    F.time_end_form,
    F.received_on
from {{ ref('VW_FORM_METADATA') }}  as F 
join admitted_service_forms
    on admitted_service_forms.form_id = F.form_id
    and date(F.received_on) > '2024-06-16'
