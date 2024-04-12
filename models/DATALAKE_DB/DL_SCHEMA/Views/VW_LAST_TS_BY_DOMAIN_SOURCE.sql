with 
dl_schema_table_data_fixtures as (
      select * from  {{ source('dl_schema_table_data', 'FIXTURES_RAW') }}
), 

dl_schema_table_data_cases as (
      select * from  {{ source('dl_schema_table_data', 'CASES_RAW') }}
), 

dl_schema_table_data_action_times as (
      select * from  {{ source('dl_schema_table_data', 'ACTION_TIMES_RAW') }}
), 

dl_schema_table_data_forms as (
      select * from  {{ source('dl_schema_table_data', 'FORMS_RAW') }}
), 

dl_schema_table_data_locations as (
      select * from  {{ source('dl_schema_table_data', 'LOCATIONS_RAW') }}
), 

dl_schema_table_data_web_users as (
      select * from  {{ source('dl_schema_table_data', 'WEB_USERS_RAW') }}
), 

d as (select domain from dl_schema_table_data_fixtures union select domain from dl_schema_table_data_cases union select domain from dl_schema_table_data_forms union select domain from dl_schema_table_data_locations)
, f as (select distinct domain from d)
, src as (select $1 source, $2 meta_calc, $3 json_calc, $4 id_calc, $5 ts_calc, $6 flatten_calc
        from (values 
            ('case', $$s.$1:meta$$, $$f.value$$, $$f.value:id::string$$, $$replace(split_part(metadata_filename, '_', -1), '.json')$$, $$s.$1:objects$$), 
            ('form', $$s.$1:meta$$, $$f.value$$, $$f.value:id::string$$, $$replace(split_part(metadata_filename, '_', -1), '.json')$$, $$s.$1:objects$$), 
            ('location', $$s.$1:meta$$, $$f.value$$, $$f.value:location_id::string$$, $$replace(split_part(metadata_filename, '_', -2), '.json')$$, $$s.$1:objects$$), 
            ('fixture', $$s.$1:meta$$, $$f.value$$, $$f.value:id::string$$, $$replace(split_part(metadata_filename, '_', -2), '.json')$$, $$s.$1:objects$$), 
            ('web-user', $$s.$1:meta$$, $$f.value$$, $$f.value:id::string$$, $$replace(split_part(metadata_filename, '_', -2), '.json')$$, $$s.$1:objects$$), 
            ('action_times', $$s.$1:meta$$, $$f.value$$, $$coalesce(f.value:user_id::string, f.value:user::string) || '_' || f.value:UTC_start_time::string$$, 
                $$replace(split_part(metadata_filename, '_', -2), '.json')$$, $$s.$1:objects$$)))
, ts as (select f.domain source_domain, 'case' source, max(c.system_query_ts) last_ts 
            ,max('/' || split_part(METADATA_FILENAME, '/', -5) || '/' || split_part(METADATA_FILENAME, '/', -4) || '/' || 
                split_part(METADATA_FILENAME, '/', -3) || '/' || split_part(METADATA_FILENAME, '/', -2) || '/') path
            ,$$f.value:domain::string$$ domain_calc
        from f left join dl_schema_table_data_cases c on c.domain = f.domain group by f.domain
        union
        select f.domain source_domain, 'form' source, max(r.system_query_ts) last_ts 
            ,max('/' || split_part(METADATA_FILENAME, '/', -5) || '/' || split_part(METADATA_FILENAME, '/', -4) || '/' || 
                split_part(METADATA_FILENAME, '/', -3) || '/' || split_part(METADATA_FILENAME, '/', -2) || '/') path
            ,$$f.value:domain::string$$ domain_calc
        from f left join dl_schema_table_data_forms r on r.domain = f.domain group by f.domain
        union
        select f.domain source_domain, 'location' source, max(r.system_query_ts) last_ts 
            ,max('/' || split_part(METADATA_FILENAME, '/', -5) || '/' || split_part(METADATA_FILENAME, '/', -4) || '/' || 
                split_part(METADATA_FILENAME, '/', -3) || '/' || split_part(METADATA_FILENAME, '/', -2) || '/') path
            ,$$'$$ || f.domain || $$'$$ domain_calc
        from f left join dl_schema_table_data_locations r on r.domain = f.domain group by f.domain
        union
        select f.domain source_domain, 'web-user' source, max(r.system_query_ts) last_ts 
            ,max('/' || split_part(METADATA_FILENAME, '/', -5) || '/' || split_part(METADATA_FILENAME, '/', -4) || '/' || 
                split_part(METADATA_FILENAME, '/', -3) || '/' || split_part(METADATA_FILENAME, '/', -2) || '/') path
            ,$$'$$ || f.domain || $$'$$ domain_calc
        from f left join dl_schema_table_data_web_users r on r.domain = f.domain group by f.domain
        union
        select f.domain source_domain, 'action_times' source, max(r.system_query_ts) last_ts 
            ,max('/' || split_part(METADATA_FILENAME, '/', -5) || '/' || split_part(METADATA_FILENAME, '/', -4) || '/' || 
                split_part(METADATA_FILENAME, '/', -3) || '/' || split_part(METADATA_FILENAME, '/', -2) || '/') path
            ,$$'$$ || f.domain || $$'$$ domain_calc
        from f left join dl_schema_table_data_action_times r on r.domain = f.domain group by f.domain
        union
        select f.domain source_domain, 'fixture' source, max(d.system_query_ts) last_ts 
            ,max('/' || split_part(METADATA_FILENAME, '/', -5) || '/' || split_part(METADATA_FILENAME, '/', -4) || '/' || 
                split_part(METADATA_FILENAME, '/', -3) || '/' || split_part(METADATA_FILENAME, '/', -2) || '/') path
            ,$$coalesce(f.value:fields.domain::string, '$$ || f.domain || $$')$$ domain_calc
        from f left join dl_schema_table_data_fixtures d on d.domain = f.domain group by f.domain
        ),

final as (
select ts.source_domain source_domain, src.source, ts.last_ts, ts.path, ts.domain_calc, src.meta_calc, src.json_calc, src.id_calc, src.ts_calc, src.flatten_calc
from src
left join ts on ts.source = src.source
)

select
	SOURCE_DOMAIN,
	SOURCE,
	LAST_TS,
	PATH,
	DOMAIN_CALC,
	META_CALC,
	JSON_CALC,
	ID_CALC,
	TS_CALC,
	FLATTEN_CALC
from final