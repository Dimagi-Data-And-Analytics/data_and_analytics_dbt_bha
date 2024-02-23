with 
dl_table_data_forms as (
      select * from  {{ source('dl_table_data', 'FORMS_RAW') }}
), 
final as (
    select flat_j.value::text as case_id, fr.json:archived::boolean as archived, flat_j.this:create:case_type::text as case_type
        from dl_table_data_forms FR,
            LATERAL FLATTEN(parse_json(FR.json:form), recursive => True) flat_j
        where 
            flat_j.key = '@case_id'
            and flat_j.this:create is not null
            and FR.json:archived::boolean = true
        group by case_id, archived, case_type
   )

select 
   	CASE_ID,
	ARCHIVED,
	CASE_TYPE
from final