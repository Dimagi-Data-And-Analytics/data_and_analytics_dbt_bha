with integration_table_data_config_fixture as (
      select * from  {{ source('integration_table_data', 'CONFIG_FIXTURE_FIELDS') }}
), 
final as (
select fixture_type, 
    'create or replace table <<dm_db>>.DM.FIXTURE_' || replace(upper(fixture_type),'-','_') 
    || ' COPY GRANTS (\n   DOMAIN\n   ,ID\n   ,LAST_UPDATED\n   ,' ||
    listagg(ifnull(FIELD_NAME_OVERRIDE, FIELD_ALIAS), '\n   ,') || '\n) as \n' ||
    'select\n   DOMAIN\n   ,ID\n   ,LAST_UPDATED\n   ,' 
    || listagg(ifnull(FIELD_NAME_OVERRIDE, FIELD_ALIAS), '\n   ,') || 
    '\nfrom <<dm_db>>.INTEGRATION.VW_FIXTURE_' || replace(upper(fixture_type),'-','_') || '_ALL;' sql_text
from integration_table_data_config_fixture
where include_in_dim
group by fixture_type
)

select
	FIXTURE_TYPE,
	SQL_TEXT
from final