with final as (
select 'case' source, case_type source_type, vw_type, sql_text from generate_case_views
union
select 'fixture' source, fixture_type source_type, vw_type, sql_text from generate_fixture_views
union
select 'location' source, null source_type, vw_type, sql_text from generate_location_views
union
select 'web-user' source, null source_type, vw_type, sql_text from generate_web_user_views
union
select 'action_times' source, null source_type, vw_type, sql_text from generate_web_user_views
)

select 
	SOURCE,
	SOURCE_TYPE,
	VW_TYPE,
	SQL_TEXT
from final