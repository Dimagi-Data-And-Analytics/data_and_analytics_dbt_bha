with integration_table_data_config_case as (
      select * from  {{ source('integration_table_data', 'CONFIG_CASE_FIELDS') }}
), 
final as (
select case_type, vw_type,
    'DELETE FROM <<dm_db>>.DM.CASE_' || replace(upper(case_type),'-','_') || ' c ' || 
    'USING <<dm_db>>.INTEGRATION.SRC_UPDATE_CASE_DELETIONS' || max(vt.src_type) || ' f ' ||
    'WHERE c.ID = f.caseid and f.CASETYPE = \'' || case_type || '\';' sql_text
from integration_table_data_config_case
full join (select $1 vw_type, $2 src_type from (values ('STG', '_STAGE'), ('ALL', '_ALL'))) vt
where include_in_dim
group by case_type, vw_type
)

select 
	CASE_TYPE,
	VW_TYPE,
	SQL_TEXT
from final