////// setup project in datalake //////

use role sysadmin;
use database DATALAKE_DEV;

create schema IF NOT EXISTS CO_CARE_COORDINATION_COMMCARE_PERF;

use schema CO_CARE_COORDINATION_COMMCARE_PERF;

use role securityadmin;
grant select on future tables in schema datalake_dev.CO_CARE_COORDINATION_COMMCARE_PERF to role DATALAKE_DEV_R;
grant select on future views in schema datalake_dev.CO_CARE_COORDINATION_COMMCARE_PERF to role DATALAKE_DEV_R;

-- if testing project --
use role securityadmin;
grant select on future tables in schema datalake_dev.CO_CARE_COORDINATION_COMMCARE_PERF to role DATALAKE_RW;
grant select on future views in schema datalake_dev.CO_CARE_COORDINATION_COMMCARE_PERF to role DATALAKE_RW;
-- for role datalake_test_rw
grant usage on schema datalake_dev.CO_CARE_COORDINATION_COMMCARE_PERF to role datalake_test_rw;
grant create function on schema datalake_dev.CO_CARE_COORDINATION_COMMCARE_PERF to role datalake_test_rw;
grant create stage on schema datalake_dev.CO_CARE_COORDINATION_COMMCARE_PERF to role datalake_test_rw;
grant create task on schema datalake_dev.CO_CARE_COORDINATION_COMMCARE_PERF to role datalake_test_rw;
grant monitor on future tasks in schema datalake_dev.CO_CARE_COORDINATION_COMMCARE_PERF  to role datalake_test_rw;
grant create file format on schema datalake_dev.CO_CARE_COORDINATION_COMMCARE_PERF to role datalake_test_rw;
grant create table on schema datalake_dev.CO_CARE_COORDINATION_COMMCARE_PERF to role datalake_test_rw;
grant create view on schema datalake_dev.CO_CARE_COORDINATION_COMMCARE_PERF to role datalake_test_rw;
grant delete on future tables in schema datalake_dev.CO_CARE_COORDINATION_COMMCARE_PERF to role datalake_test_rw;
grant delete on future views in schema datalake_dev.CO_CARE_COORDINATION_COMMCARE_PERF to role datalake_test_rw;
grant insert on future tables in schema datalake_dev.CO_CARE_COORDINATION_COMMCARE_PERF to role datalake_test_rw;
grant insert on future views in schema datalake_dev.CO_CARE_COORDINATION_COMMCARE_PERF to role datalake_test_rw;
grant select on future tables in schema datalake_dev.CO_CARE_COORDINATION_COMMCARE_PERF to role datalake_test_rw;
grant select on future views in schema datalake_dev.CO_CARE_COORDINATION_COMMCARE_PERF to role datalake_test_rw;
grant truncate on future tables in schema datalake_dev.CO_CARE_COORDINATION_COMMCARE_PERF to role datalake_test_rw;
grant truncate on future views in schema datalake_dev.CO_CARE_COORDINATION_COMMCARE_PERF to role datalake_test_rw;
grant update on future tables in schema datalake_dev.CO_CARE_COORDINATION_COMMCARE_PERF to role datalake_test_rw;
grant update on future views in schema datalake_dev.CO_CARE_COORDINATION_COMMCARE_PERF to role datalake_test_rw;

use role user_dbt_test;
-- end if testing project --

use schema CO_CARE_COORDINATION_COMMCARE_PERF;

create or replace TABLE CASES_RAW (
	DOMAIN VARCHAR(16777216),
    METADATA VARCHAR(16777216), 
    METADATA_FILENAME VARCHAR(16777216), 
	JSON VARIANT,
	ID VARCHAR(16777216) NOT NULL DEFAULT '0',
	SYSTEM_QUERY_TS VARCHAR(16777216),
	SYSTEM_CREATE_TS VARCHAR(16777216) DEFAULT CASES_RAW.SYSTEM_QUERY_TS,
    TASK_ID NUMBER(38,0),
    EXECUTION_ID NUMBER(38,0),
    DELETED_FLAG BOOLEAN DEFAULT FALSE,
    DELETED_DATE VARCHAR(16777216),
    DELETED_FORMID VARCHAR(16777216),
    primary key (ID)
);

create or replace TABLE CASES_RAW_STAGE ( 
    DOMAIN VARCHAR(16777216), 
    METADATA VARCHAR(16777216), 
    METADATA_FILENAME VARCHAR(16777216), 
    JSON VARIANT, 
    ID VARCHAR(16777216) NOT NULL DEFAULT '0', 
	SYSTEM_QUERY_TS VARCHAR(16777216),
    TASK_ID NUMBER(38,0),
    EXECUTION_ID NUMBER(38,0),
    primary key (ID) );
    
create or replace TABLE FIXTURES_RAW ( 
    DOMAIN VARCHAR(16777216), 
    METADATA VARCHAR(16777216), 
    METADATA_FILENAME VARCHAR(16777216), 
    JSON VARIANT, 
    ID VARCHAR(16777216) NOT NULL DEFAULT '0', 
    SYSTEM_QUERY_TS VARCHAR(16777216), 
    SYSTEM_CREATE_TS VARCHAR(16777216) DEFAULT FIXTURES_RAW.SYSTEM_QUERY_TS,
    TASK_ID NUMBER(38,0),
    EXECUTION_ID NUMBER(38,0),
    primary key (ID) );

create or replace TABLE FIXTURES_RAW_STAGE ( 
    DOMAIN VARCHAR(16777216), 
    METADATA VARCHAR(16777216), 
    METADATA_FILENAME VARCHAR(16777216), 
    JSON VARIANT, 
    ID VARCHAR(16777216) NOT NULL DEFAULT '0', 
	SYSTEM_QUERY_TS VARCHAR(16777216),
    TASK_ID NUMBER(38,0),
    EXECUTION_ID NUMBER(38,0),
    primary key (ID) );

create or replace TABLE FORMS_RAW ( 
    DOMAIN VARCHAR(16777216), 
    METADATA VARCHAR(16777216), 
    METADATA_FILENAME VARCHAR(16777216), 
    JSON VARIANT,
    ID VARCHAR(16777216) NOT NULL DEFAULT '0', 
    SYSTEM_QUERY_TS VARCHAR(16777216), 
    SYSTEM_CREATE_TS VARCHAR(16777216) DEFAULT FORMS_RAW.SYSTEM_QUERY_TS, 
    TASK_ID NUMBER(38,0),
    EXECUTION_ID NUMBER(38,0),
    primary key (ID) );
    
create or replace TABLE FORMS_RAW_STAGE ( 
    DOMAIN VARCHAR(16777216), 
    METADATA VARCHAR(16777216), 
    METADATA_FILENAME VARCHAR(16777216), 
    JSON VARIANT, 
    ID VARCHAR(16777216) NOT NULL DEFAULT '0', 
	SYSTEM_QUERY_TS VARCHAR(16777216),
    TASK_ID NUMBER(38,0),
    EXECUTION_ID NUMBER(38,0),
    primary key (ID) );

create or replace TABLE LOCATIONS_RAW ( 
    DOMAIN VARCHAR(16777216), 
    METADATA VARCHAR(16777216), 
    METADATA_FILENAME VARCHAR(16777216), 
    JSON VARIANT, 
    ID VARCHAR(16777216) NOT NULL DEFAULT '0', 
    SYSTEM_QUERY_TS VARCHAR(16777216), 
    SYSTEM_CREATE_TS VARCHAR(16777216) DEFAULT LOCATIONS_RAW.SYSTEM_QUERY_TS, 
    TASK_ID NUMBER(38,0),
    EXECUTION_ID NUMBER(38,0),
    primary key (ID) );

create or replace TABLE LOCATIONS_RAW_STAGE (
	DOMAIN VARCHAR(16777216),
    METADATA VARCHAR(16777216), 
    METADATA_FILENAME VARCHAR(16777216), 
    JSON VARIANT, 
    ID VARCHAR(16777216) NOT NULL DEFAULT '0', 
	SYSTEM_QUERY_TS VARCHAR(16777216),
    TASK_ID NUMBER(38,0),
    EXECUTION_ID NUMBER(38,0),
	primary key (ID)
);

create or replace TABLE WEB_USERS_RAW (
	DOMAIN VARCHAR(16777216),
    METADATA VARCHAR(16777216), 
    METADATA_FILENAME VARCHAR(16777216), 
	JSON VARIANT,
	ID VARCHAR(16777216) NOT NULL DEFAULT '0',
	SYSTEM_QUERY_TS VARCHAR(16777216),
	SYSTEM_CREATE_TS VARCHAR(16777216) DEFAULT WEB_USERS_RAW.SYSTEM_QUERY_TS,
    TASK_ID NUMBER(38,0),
    EXECUTION_ID NUMBER(38,0),
	primary key (ID)
);

create or replace TABLE WEB_USERS_RAW_STAGE (
	DOMAIN VARCHAR(16777216),
    METADATA VARCHAR(16777216), 
    METADATA_FILENAME VARCHAR(16777216), 
    JSON VARIANT, 
    ID VARCHAR(16777216) NOT NULL DEFAULT '0', 
	SYSTEM_QUERY_TS VARCHAR(16777216),
    TASK_ID NUMBER(38,0),
    EXECUTION_ID NUMBER(38,0),
	primary key (ID)
);

create or replace TABLE ACTION_TIMES_RAW (
	DOMAIN VARCHAR(16777216),
    METADATA VARCHAR(16777216), 
    METADATA_FILENAME VARCHAR(16777216), 
	JSON VARIANT,
	ID VARCHAR(16777216) NOT NULL DEFAULT '0',
	SYSTEM_QUERY_TS VARCHAR(16777216),
	SYSTEM_CREATE_TS VARCHAR(16777216) DEFAULT ACTION_TIMES_RAW.SYSTEM_QUERY_TS,
    TASK_ID NUMBER(38,0),
    EXECUTION_ID NUMBER(38,0),
	primary key (ID)
);

create or replace TABLE ACTION_TIMES_RAW_STAGE (
	DOMAIN VARCHAR(16777216),
    METADATA VARCHAR(16777216), 
    METADATA_FILENAME VARCHAR(16777216), 
    JSON VARIANT, 
    ID VARCHAR(16777216) NOT NULL DEFAULT '0', 
	SYSTEM_QUERY_TS VARCHAR(16777216),
    TASK_ID NUMBER(38,0),
    EXECUTION_ID NUMBER(38,0),
	primary key (ID)
);

use role sysadmin;

CREATE OR REPLACE file format json_file_format
	type=JSON TIME_FORMAT=AUTO;

CREATE OR REPLACE STAGE s3_json_stage
    url='s3://commcare-snowflake-data-sync/co-carecoordination-perf/snowflake-copy/'
    Storage_integration = s3_int_obj
    file_format = json_file_format;

/* -- in dbt --
create or replace view VW_LAST_TS_BY_DOMAIN_SOURCE as
with d as (select domain from FIXTURES_RAW union select domain from CASES_RAW union select domain from FORMS_RAW union select domain from LOCATIONS_RAW)
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
        from f left join cases_raw c on c.domain = f.domain group by f.domain
        union
        select f.domain source_domain, 'form' source, max(r.system_query_ts) last_ts 
            ,max('/' || split_part(METADATA_FILENAME, '/', -5) || '/' || split_part(METADATA_FILENAME, '/', -4) || '/' || 
                split_part(METADATA_FILENAME, '/', -3) || '/' || split_part(METADATA_FILENAME, '/', -2) || '/') path
            ,$$f.value:domain::string$$ domain_calc
        from f left join forms_raw r on r.domain = f.domain group by f.domain
        union
        select f.domain source_domain, 'location' source, max(r.system_query_ts) last_ts 
            ,max('/' || split_part(METADATA_FILENAME, '/', -5) || '/' || split_part(METADATA_FILENAME, '/', -4) || '/' || 
                split_part(METADATA_FILENAME, '/', -3) || '/' || split_part(METADATA_FILENAME, '/', -2) || '/') path
            ,$$'$$ || f.domain || $$'$$ domain_calc
        from f left join locations_raw r on r.domain = f.domain group by f.domain
        union
        select f.domain source_domain, 'web-user' source, max(r.system_query_ts) last_ts 
            ,max('/' || split_part(METADATA_FILENAME, '/', -5) || '/' || split_part(METADATA_FILENAME, '/', -4) || '/' || 
                split_part(METADATA_FILENAME, '/', -3) || '/' || split_part(METADATA_FILENAME, '/', -2) || '/') path
            ,$$'$$ || f.domain || $$'$$ domain_calc
        from f left join web_users_raw r on r.domain = f.domain group by f.domain
        union
        select f.domain source_domain, 'action_times' source, max(r.system_query_ts) last_ts 
            ,max('/' || split_part(METADATA_FILENAME, '/', -5) || '/' || split_part(METADATA_FILENAME, '/', -4) || '/' || 
                split_part(METADATA_FILENAME, '/', -3) || '/' || split_part(METADATA_FILENAME, '/', -2) || '/') path
            ,$$'$$ || f.domain || $$'$$ domain_calc
        from f left join action_times_raw r on r.domain = f.domain group by f.domain
        union
        select f.domain source_domain, 'fixture' source, max(d.system_query_ts) last_ts 
            ,max('/' || split_part(METADATA_FILENAME, '/', -5) || '/' || split_part(METADATA_FILENAME, '/', -4) || '/' || 
                split_part(METADATA_FILENAME, '/', -3) || '/' || split_part(METADATA_FILENAME, '/', -2) || '/') path
            ,$$coalesce(f.value:fields.domain::string, '$$ || f.domain || $$')$$ domain_calc
        from f left join fixtures_raw d on d.domain = f.domain group by f.domain
        )
select ts.source_domain source_domain, src.source, ts.last_ts, ts.path, ts.domain_calc, src.meta_calc, src.json_calc, src.id_calc, src.ts_calc, src.flatten_calc
from src
left join ts on ts.source = src.source
;
*/

////// setup project in datamart //////
use role sysadmin;

create database IF NOT EXISTS DM_CO_CARE_COORD_PERF;
use database DM_CO_CARE_COORD_PERF;

grant usage on database DM_CO_CARE_COORD_PERF to role DM_CO_CARE_COORD_DEV_USAGE;
grant usage on database DM_CO_CARE_COORD_PERF to role DW_UTIL_RW;
grant usage on database DM_CO_CARE_COORD_PERF to role user_etl;

-- if testing project --
grant usage on database DM_CO_CARE_COORD_PERF to role DM_CO_CARE_COORD_TEST_RW;
grant usage on database DM_CO_CARE_COORD_PERF to role USER_DIMAGI_ANALYST;
-- end if testing project --

use role sysadmin;
create schema IF NOT EXISTS DL;

use schema DL;
grant usage on schema DM_CO_CARE_COORD_PERF.DL to role DM_CO_CARE_COORD_DEV_DL_R;
grant usage on schema DM_CO_CARE_COORD_PERF.DL to role user_etl;
use role accountadmin;
grant select on future tables in schema DM_CO_CARE_COORD_PERF.DL to role DM_CO_CARE_COORD_DEV_DL_R;
grant select on future views in schema DM_CO_CARE_COORD_PERF.DL to role DM_CO_CARE_COORD_DEV_DL_R;
grant select on future tables in schema DM_CO_CARE_COORD_PERF.DL to role user_etl;
grant select on future views in schema DM_CO_CARE_COORD_PERF.DL to role user_etl;

-- if testing project --
use role sysadmin;
grant usage on schema DM_CO_CARE_COORD_PERF.DL to role DM_CO_CARE_COORD_TEST_RW;
use role accountadmin;
grant create function on schema DM_CO_CARE_COORD_PERF.DL to role DM_CO_CARE_COORD_TEST_RW;
grant create stage on schema DM_CO_CARE_COORD_PERF.DL to role DM_CO_CARE_COORD_TEST_RW;
grant create task on schema DM_CO_CARE_COORD_PERF.DL to role DM_CO_CARE_COORD_TEST_RW;
grant create file format on schema DM_CO_CARE_COORD_PERF.DL to role DM_CO_CARE_COORD_TEST_RW;
grant create table on schema DM_CO_CARE_COORD_PERF.DL to role DM_CO_CARE_COORD_TEST_RW;
grant create view on schema DM_CO_CARE_COORD_PERF.DL to role DM_CO_CARE_COORD_TEST_RW;
grant DELETE, INSERT, SELECT, TRUNCATE, UPDATE on future tables in schema DM_CO_CARE_COORD_PERF.DL to role DM_CO_CARE_COORD_TEST_RW;
grant DELETE, INSERT, SELECT, TRUNCATE, UPDATE on future views in schema DM_CO_CARE_COORD_PERF.DL to role DM_CO_CARE_COORD_TEST_RW;
-- end if testing project --

use role sysadmin;
create schema IF NOT EXISTS DM;

use schema DM;
grant usage on schema DM_CO_CARE_COORD_PERF.DM to role DM_CO_CARE_COORD_DEV_DM_R;
grant usage on schema DM_CO_CARE_COORD_PERF.DM to role user_etl;
use role accountadmin;
grant usage on future functions in schema DM_CO_CARE_COORD_PERF.DM to role DM_CO_CARE_COORD_DEV_DM_R;
grant select on future tables in schema DM_CO_CARE_COORD_PERF.DM to role DM_CO_CARE_COORD_DEV_DM_R;
grant select on future views in schema DM_CO_CARE_COORD_PERF.DM to role DM_CO_CARE_COORD_DEV_DM_R;
grant select on future tables in schema DM_CO_CARE_COORD_PERF.DM to role user_etl;
grant select on future views in schema DM_CO_CARE_COORD_PERF.DM to role user_etl;

-- if testing project --
use role sysadmin;
grant usage on schema DM_CO_CARE_COORD_PERF.DM to role DM_CO_CARE_COORD_TEST_RW;
use role accountadmin;
grant create function on schema DM_CO_CARE_COORD_PERF.DM to role DM_CO_CARE_COORD_TEST_RW;
grant create stage on schema DM_CO_CARE_COORD_PERF.DM to role DM_CO_CARE_COORD_TEST_RW;
grant create task on schema DM_CO_CARE_COORD_PERF.DM to role DM_CO_CARE_COORD_TEST_RW;
grant create file format on schema DM_CO_CARE_COORD_PERF.DM to role DM_CO_CARE_COORD_TEST_RW;
grant create table on schema DM_CO_CARE_COORD_PERF.DM to role DM_CO_CARE_COORD_TEST_RW;
grant create view on schema DM_CO_CARE_COORD_PERF.DM to role DM_CO_CARE_COORD_TEST_RW;
grant DELETE, INSERT, SELECT, TRUNCATE, UPDATE on future tables in schema DM_CO_CARE_COORD_PERF.DM to role DM_CO_CARE_COORD_TEST_RW;
grant DELETE, INSERT, SELECT, TRUNCATE, UPDATE on future views in schema DM_CO_CARE_COORD_PERF.DM to role DM_CO_CARE_COORD_TEST_RW;
-- end if testing project --

use role sysadmin;
create schema IF NOT EXISTS INTEGRATION;

use schema INTEGRATION;
grant usage on schema DM_CO_CARE_COORD_PERF.INTEGRATION to role DM_CO_CARE_COORD_DEV_INTEGRATION_R;
grant usage on schema DM_CO_CARE_COORD_PERF.INTEGRATION to role DW_UTIL_RW;
grant usage on schema DM_CO_CARE_COORD_PERF.INTEGRATION to role user_etl;
use role accountadmin;
grant select on future tables in schema DM_CO_CARE_COORD_PERF.INTEGRATION to role DM_CO_CARE_COORD_DEV_INTEGRATION_R;
grant select on future views in schema DM_CO_CARE_COORD_PERF.INTEGRATION to role DM_CO_CARE_COORD_DEV_INTEGRATION_R;
grant select on future tables in schema DM_CO_CARE_COORD_PERF.INTEGRATION to role user_etl;
grant select on future views in schema DM_CO_CARE_COORD_PERF.INTEGRATION to role user_etl;
grant DELETE, INSERT, SELECT, TRUNCATE, UPDATE on future tables in schema DM_CO_CARE_COORD_PERF.INTEGRATION to role DW_UTIL_RW;
grant DELETE, INSERT, SELECT, TRUNCATE, UPDATE on future views in schema DM_CO_CARE_COORD_PERF.INTEGRATION to role DW_UTIL_RW;

-- if testing project --
use role sysadmin;
grant usage on schema DM_CO_CARE_COORD_PERF.INTEGRATION to role DM_CO_CARE_COORD_TEST_RW;
use role accountadmin;
grant create function on schema DM_CO_CARE_COORD_PERF.INTEGRATION to role DM_CO_CARE_COORD_TEST_RW;
grant create stage on schema DM_CO_CARE_COORD_PERF.INTEGRATION to role DM_CO_CARE_COORD_TEST_RW;
grant create task on schema DM_CO_CARE_COORD_PERF.INTEGRATION to role DM_CO_CARE_COORD_TEST_RW;
grant create file format on schema DM_CO_CARE_COORD_PERF.INTEGRATION to role DM_CO_CARE_COORD_TEST_RW;
grant create table on schema DM_CO_CARE_COORD_PERF.INTEGRATION to role DM_CO_CARE_COORD_TEST_RW;
grant create view on schema DM_CO_CARE_COORD_PERF.INTEGRATION to role DM_CO_CARE_COORD_TEST_RW;
grant DELETE, INSERT, SELECT, TRUNCATE, UPDATE on future tables in schema DM_CO_CARE_COORD_PERF.INTEGRATION to role DM_CO_CARE_COORD_TEST_RW;
grant DELETE, INSERT, SELECT, TRUNCATE, UPDATE on future views in schema DM_CO_CARE_COORD_PERF.INTEGRATION to role DM_CO_CARE_COORD_TEST_RW;
-- end if testing project --

use role sysadmin;
create schema IF NOT EXISTS UTIL;

use schema UTIL;
grant usage on schema DM_CO_CARE_COORD_PERF.UTIL to role DM_CO_CARE_COORD_DEV_UTIL_R;
grant usage on schema DM_CO_CARE_COORD_PERF.UTIL to role DW_UTIL_RW;
grant usage on schema DM_CO_CARE_COORD_PERF.UTIL to role user_etl;
use role accountadmin;
grant select on future tables in schema DM_CO_CARE_COORD_PERF.UTIL to role DM_CO_CARE_COORD_DEV_UTIL_R;
grant select on future views in schema DM_CO_CARE_COORD_PERF.UTIL to role DM_CO_CARE_COORD_DEV_UTIL_R;
grant select on future tables in schema DM_CO_CARE_COORD_PERF.UTIL to role user_etl;
grant select on future views in schema DM_CO_CARE_COORD_PERF.UTIL to role user_etl;
grant DELETE, INSERT, SELECT, TRUNCATE, UPDATE on future tables in schema DM_CO_CARE_COORD_PERF.UTIL to role DW_UTIL_RW;
grant DELETE, INSERT, SELECT, TRUNCATE, UPDATE on future views in schema DM_CO_CARE_COORD_PERF.UTIL to role DW_UTIL_RW;

-- if testing project --
use role sysadmin;
grant usage on schema DM_CO_CARE_COORD_PERF.UTIL to role DM_CO_CARE_COORD_TEST_RW;
use role accountadmin;
grant create function on schema DM_CO_CARE_COORD_PERF.UTIL to role DM_CO_CARE_COORD_TEST_RW;
grant create stage on schema DM_CO_CARE_COORD_PERF.UTIL to role DM_CO_CARE_COORD_TEST_RW;
grant create task on schema DM_CO_CARE_COORD_PERF.UTIL to role DM_CO_CARE_COORD_TEST_RW;
grant create file format on schema DM_CO_CARE_COORD_PERF.UTIL to role DM_CO_CARE_COORD_TEST_RW;
grant create table on schema DM_CO_CARE_COORD_PERF.UTIL to role DM_CO_CARE_COORD_TEST_RW;
grant create view on schema DM_CO_CARE_COORD_PERF.UTIL to role DM_CO_CARE_COORD_TEST_RW;
grant DELETE, INSERT, SELECT, TRUNCATE, UPDATE on future tables in schema DM_CO_CARE_COORD_PERF.UTIL to role DM_CO_CARE_COORD_TEST_RW;
grant DELETE, INSERT, SELECT, TRUNCATE, UPDATE on future views in schema DM_CO_CARE_COORD_PERF.UTIL to role DM_CO_CARE_COORD_TEST_RW;
-- end if testing project --


/* -- in dbt --
//// setup DL schema ////

use role sysadmin;
use schema DL;

create or replace view SRC_CASES_RAW(
	DOMAIN,
    METADATA,
    METADATA_FILENAME,
	JSON,
	ID,
	SYSTEM_QUERY_TS,
    SYSTEM_CREATE_TS,
    TASK_ID,
    EXECUTION_ID,
    DELETED_FLAG,
    DELETED_DATE,
    DELETED_FORMID
) as 
SELECT 
     DOMAIN
     ,METADATA
     ,METADATA_FILENAME
     ,JSON
     ,ID
     ,SYSTEM_QUERY_TS
     ,SYSTEM_CREATE_TS
     ,TASK_ID
     ,EXECUTION_ID
     ,DELETED_FLAG
     ,DELETED_DATE
     ,DELETED_FORMID
FROM 
    "DATALAKE_DEV"."ETL_REPLACE_TEST_BHA_DEV"."CASES_RAW"
;

create or replace view SRC_CASES_RAW_STAGE(
	DOMAIN,
    METADATA,
    METADATA_FILENAME,
	JSON,
	ID,
	SYSTEM_QUERY_TS,
    TASK_ID,
    EXECUTION_ID
) as 
SELECT 
	DOMAIN,
    METADATA,
    METADATA_FILENAME,
	JSON,
	ID,
	SYSTEM_QUERY_TS,
    TASK_ID,
    EXECUTION_ID
FROM 
    "DATALAKE_DEV"."ETL_REPLACE_TEST_BHA_DEV"."CASES_RAW_STAGE"
;

create or replace view SRC_FORMS_RAW(
	DOMAIN,
    METADATA,
    METADATA_FILENAME,
	JSON,
	ID,
	SYSTEM_QUERY_TS,
    SYSTEM_CREATE_TS,
    TASK_ID,
    EXECUTION_ID
) as 
SELECT 
     DOMAIN
     ,METADATA
     ,METADATA_FILENAME
     ,JSON
     ,ID
     ,SYSTEM_QUERY_TS
     ,SYSTEM_CREATE_TS
     ,TASK_ID
     ,EXECUTION_ID
FROM 
    "DATALAKE_DEV"."ETL_REPLACE_TEST_BHA_DEV"."FORMS_RAW"
;

create or replace view SRC_FORMS_RAW_STAGE(
	DOMAIN,
    METADATA,
    METADATA_FILENAME,
	JSON,
	ID,
	SYSTEM_QUERY_TS,
    TASK_ID,
    EXECUTION_ID
) as 
SELECT 
	DOMAIN,
    METADATA,
    METADATA_FILENAME,
	JSON,
	ID,
	SYSTEM_QUERY_TS,
    TASK_ID,
    EXECUTION_ID
FROM 
    "DATALAKE_DEV"."ETL_REPLACE_TEST_BHA_DEV"."FORMS_RAW_STAGE"
;

create or replace view SRC_FIXTURES_RAW(
	DOMAIN,
    METADATA,
    METADATA_FILENAME,
	JSON,
	ID,
	SYSTEM_QUERY_TS,
    SYSTEM_CREATE_TS,
    TASK_ID,
    EXECUTION_ID
) as 
SELECT 
     DOMAIN
     ,METADATA
     ,METADATA_FILENAME
     ,JSON
     ,ID
     ,SYSTEM_QUERY_TS
     ,SYSTEM_CREATE_TS
     ,TASK_ID
     ,EXECUTION_ID
FROM 
    "DATALAKE_DEV"."ETL_REPLACE_TEST_BHA_DEV"."FIXTURES_RAW"
;

create or replace view SRC_FIXTURES_RAW_STAGE(
	DOMAIN,
    METADATA,
    METADATA_FILENAME,
	JSON,
	ID,
	SYSTEM_QUERY_TS,
    TASK_ID,
    EXECUTION_ID
) as 
SELECT 
	DOMAIN,
    METADATA,
    METADATA_FILENAME,
	JSON,
	ID,
	SYSTEM_QUERY_TS,
    TASK_ID,
    EXECUTION_ID
FROM 
    "DATALAKE_DEV"."ETL_REPLACE_TEST_BHA_DEV"."FIXTURES_RAW_STAGE"
;

create or replace view SRC_LOCATIONS_RAW(
	DOMAIN,
    METADATA,
    METADATA_FILENAME,
	JSON,
	ID,
	SYSTEM_QUERY_TS,
    SYSTEM_CREATE_TS,
    TASK_ID,
    EXECUTION_ID
) as 
SELECT 
     DOMAIN
     ,METADATA
     ,METADATA_FILENAME
     ,JSON
     ,ID
     ,SYSTEM_QUERY_TS
     ,SYSTEM_CREATE_TS
     ,TASK_ID
     ,EXECUTION_ID
FROM 
    "DATALAKE_DEV"."ETL_REPLACE_TEST_BHA_DEV"."LOCATIONS_RAW"
;

create or replace view SRC_LOCATIONS_RAW_STAGE(
	DOMAIN,
    METADATA,
    METADATA_FILENAME,
	JSON,
	ID,
	SYSTEM_QUERY_TS,
    TASK_ID,
    EXECUTION_ID
) as 
SELECT 
	DOMAIN,
    METADATA,
    METADATA_FILENAME,
	JSON,
	ID,
	SYSTEM_QUERY_TS,
    TASK_ID,
    EXECUTION_ID
FROM 
    "DATALAKE_DEV"."ETL_REPLACE_TEST_BHA_DEV"."LOCATIONS_RAW_STAGE"
;

create or replace view SRC_WEB_USERS_RAW(
	DOMAIN,
    METADATA,
    METADATA_FILENAME,
	JSON,
	ID,
	SYSTEM_QUERY_TS,
    SYSTEM_CREATE_TS,
    TASK_ID,
    EXECUTION_ID
) as 
SELECT 
     DOMAIN
     ,METADATA
     ,METADATA_FILENAME
     ,JSON
     ,ID
     ,SYSTEM_QUERY_TS
     ,SYSTEM_CREATE_TS
     ,TASK_ID
     ,EXECUTION_ID
FROM 
    "DATALAKE_DEV"."ETL_REPLACE_TEST_BHA_DEV"."WEB_USERS_RAW"
;

create or replace view SRC_WEB_USERS_RAW_STAGE(
	DOMAIN,
    METADATA,
    METADATA_FILENAME,
	JSON,
	ID,
	SYSTEM_QUERY_TS,
    TASK_ID,
    EXECUTION_ID
) as 
SELECT 
	DOMAIN,
    METADATA,
    METADATA_FILENAME,
	JSON,
	ID,
	SYSTEM_QUERY_TS,
    TASK_ID,
    EXECUTION_ID
FROM 
    "DATALAKE_DEV"."ETL_REPLACE_TEST_BHA_DEV"."WEB_USERS_RAW_STAGE"
;

create or replace view SRC_ACTION_TIMES_RAW(
	DOMAIN,
    METADATA,
    METADATA_FILENAME,
	JSON,
	ID,
	SYSTEM_QUERY_TS,
    SYSTEM_CREATE_TS,
    TASK_ID,
    EXECUTION_ID
) as 
SELECT 
     DOMAIN
     ,METADATA
     ,METADATA_FILENAME
     ,JSON
     ,ID
     ,SYSTEM_QUERY_TS
     ,SYSTEM_CREATE_TS
     ,TASK_ID
     ,EXECUTION_ID
FROM 
    "DATALAKE_DEV"."ETL_REPLACE_TEST_BHA_DEV"."ACTION_TIMES_RAW"
;

create or replace view SRC_ACTION_TIMES_RAW_STAGE(
	DOMAIN,
    METADATA,
    METADATA_FILENAME,
	JSON,
	ID,
	SYSTEM_QUERY_TS,
    TASK_ID,
    EXECUTION_ID
) as 
SELECT 
	DOMAIN,
    METADATA,
    METADATA_FILENAME,
	JSON,
	ID,
	SYSTEM_QUERY_TS,
    TASK_ID,
    EXECUTION_ID
FROM 
    "DATALAKE_DEV"."ETL_REPLACE_TEST_BHA_DEV"."ACTION_TIMES_RAW_STAGE"
;    

//// setup DM schema ////

use schema DM;

CREATE OR REPLACE SECURE FUNCTION DECIMAL_TO_TS("DEC_TS" VARCHAR(16777216))
RETURNS TIMESTAMP_NTZ(9)
LANGUAGE SQL
AS '
   convert_timezone(''UTC'',''America/Denver'', to_timestamp_ntz(dec_ts::decimal(38,5) * 86400))
';
;
*/

//// setup INTEGRATION schema ////

use schema INTEGRATION;

/* -- in dbt --
create or replace view SRC_CASES_RAW(
	DOMAIN,
    METADATA,
    METADATA_FILENAME,
	JSON,
	ID,
	SYSTEM_QUERY_TS,
    SYSTEM_CREATE_TS,
    TASK_ID,
    EXECUTION_ID,
    DELETED_FLAG,
    DELETED_DATE,
    DELETED_FORMID
) as 
SELECT 
     DOMAIN
     ,METADATA
     ,METADATA_FILENAME
     ,JSON
     ,ID
     ,SYSTEM_QUERY_TS
     ,SYSTEM_CREATE_TS
     ,TASK_ID
     ,EXECUTION_ID
     ,DELETED_FLAG
     ,DELETED_DATE
     ,DELETED_FORMID
FROM 
    "DATALAKE_DEV"."ETL_REPLACE_TEST_BHA_DEV"."CASES_RAW"
;

create or replace view SRC_CASES_RAW_STAGE(
	DOMAIN,
    METADATA,
    METADATA_FILENAME,
	JSON,
	ID,
	SYSTEM_QUERY_TS,
    TASK_ID,
    EXECUTION_ID
) as 
SELECT 
	DOMAIN,
    METADATA,
    METADATA_FILENAME,
	JSON,
	ID,
	SYSTEM_QUERY_TS,
    TASK_ID,
    EXECUTION_ID
FROM 
    "DATALAKE_DEV"."ETL_REPLACE_TEST_BHA_DEV"."CASES_RAW_STAGE"
;

create or replace view SRC_FORMS_RAW(
	DOMAIN,
    METADATA,
    METADATA_FILENAME,
	JSON,
	ID,
	SYSTEM_QUERY_TS,
    SYSTEM_CREATE_TS,
    TASK_ID,
    EXECUTION_ID
) as 
SELECT 
     DOMAIN
     ,METADATA
     ,METADATA_FILENAME
     ,JSON
     ,ID
     ,SYSTEM_QUERY_TS
     ,SYSTEM_CREATE_TS
     ,TASK_ID
     ,EXECUTION_ID
FROM 
    "DATALAKE_DEV"."ETL_REPLACE_TEST_BHA_DEV"."FORMS_RAW"
;

create or replace view SRC_FORMS_RAW_STAGE(
	DOMAIN,
    METADATA,
    METADATA_FILENAME,
	JSON,
	ID,
	SYSTEM_QUERY_TS,
    TASK_ID,
    EXECUTION_ID
) as 
SELECT 
	DOMAIN,
    METADATA,
    METADATA_FILENAME,
	JSON,
	ID,
	SYSTEM_QUERY_TS,
    TASK_ID,
    EXECUTION_ID
FROM 
    "DATALAKE_DEV"."ETL_REPLACE_TEST_BHA_DEV"."FORMS_RAW_STAGE"
;

create or replace view SRC_FIXTURES_RAW(
	DOMAIN,
    METADATA,
    METADATA_FILENAME,
	JSON,
	ID,
	SYSTEM_QUERY_TS,
    SYSTEM_CREATE_TS,
    TASK_ID,
    EXECUTION_ID
) as 
SELECT 
     DOMAIN
     ,METADATA
     ,METADATA_FILENAME
     ,JSON
     ,ID
     ,SYSTEM_QUERY_TS
     ,SYSTEM_CREATE_TS
     ,TASK_ID
     ,EXECUTION_ID
FROM 
    "DATALAKE_DEV"."ETL_REPLACE_TEST_BHA_DEV"."FIXTURES_RAW"
;

create or replace view SRC_FIXTURES_RAW_STAGE(
	DOMAIN,
    METADATA,
    METADATA_FILENAME,
	JSON,
	ID,
	SYSTEM_QUERY_TS,
    TASK_ID,
    EXECUTION_ID
) as 
SELECT 
	DOMAIN,
    METADATA,
    METADATA_FILENAME,
	JSON,
	ID,
	SYSTEM_QUERY_TS,
    TASK_ID,
    EXECUTION_ID
FROM 
    "DATALAKE_DEV"."ETL_REPLACE_TEST_BHA_DEV"."FIXTURES_RAW_STAGE"
;

create or replace view SRC_LOCATIONS_RAW(
	DOMAIN,
    METADATA,
    METADATA_FILENAME,
	JSON,
	ID,
	SYSTEM_QUERY_TS,
    SYSTEM_CREATE_TS,
    TASK_ID,
    EXECUTION_ID
) as 
SELECT 
     DOMAIN
     ,METADATA
     ,METADATA_FILENAME
     ,JSON
     ,ID
     ,SYSTEM_QUERY_TS
     ,SYSTEM_CREATE_TS
     ,TASK_ID
     ,EXECUTION_ID
FROM 
    "DATALAKE_DEV"."ETL_REPLACE_TEST_BHA_DEV"."LOCATIONS_RAW"
;

create or replace view SRC_LOCATIONS_RAW_STAGE(
	DOMAIN,
    METADATA,
    METADATA_FILENAME,
	JSON,
	ID,
	SYSTEM_QUERY_TS,
    TASK_ID,
    EXECUTION_ID
) as 
SELECT 
	DOMAIN,
    METADATA,
    METADATA_FILENAME,
	JSON,
	ID,
	SYSTEM_QUERY_TS,
    TASK_ID,
    EXECUTION_ID
FROM 
    "DATALAKE_DEV"."ETL_REPLACE_TEST_BHA_DEV"."LOCATIONS_RAW_STAGE"
;

create or replace view SRC_WEB_USERS_RAW(
	DOMAIN,
    METADATA,
    METADATA_FILENAME,
	JSON,
	ID,
	SYSTEM_QUERY_TS,
    SYSTEM_CREATE_TS,
    TASK_ID,
    EXECUTION_ID
) as 
SELECT 
     DOMAIN
     ,METADATA
     ,METADATA_FILENAME
     ,JSON
     ,ID
     ,SYSTEM_QUERY_TS
     ,SYSTEM_CREATE_TS
     ,TASK_ID
     ,EXECUTION_ID
FROM 
    "DATALAKE_DEV"."ETL_REPLACE_TEST_BHA_DEV"."WEB_USERS_RAW"
;

create or replace view SRC_WEB_USERS_RAW_STAGE(
	DOMAIN,
    METADATA,
    METADATA_FILENAME,
	JSON,
	ID,
	SYSTEM_QUERY_TS,
    TASK_ID,
    EXECUTION_ID
) as 
SELECT 
	DOMAIN,
    METADATA,
    METADATA_FILENAME,
	JSON,
	ID,
	SYSTEM_QUERY_TS,
    TASK_ID,
    EXECUTION_ID
FROM 
    "DATALAKE_DEV"."ETL_REPLACE_TEST_BHA_DEV"."WEB_USERS_RAW_STAGE"
;

create or replace view SRC_ACTION_TIMES_RAW(
	DOMAIN,
    METADATA,
    METADATA_FILENAME,
	JSON,
	ID,
	SYSTEM_QUERY_TS,
    SYSTEM_CREATE_TS,
    TASK_ID,
    EXECUTION_ID
) as 
SELECT 
     DOMAIN
     ,METADATA
     ,METADATA_FILENAME
     ,JSON
     ,ID
     ,SYSTEM_QUERY_TS
     ,SYSTEM_CREATE_TS
     ,TASK_ID
     ,EXECUTION_ID
FROM 
    "DATALAKE_DEV"."ETL_REPLACE_TEST_BHA_DEV"."ACTION_TIMES_RAW"
;

create or replace view SRC_ACTION_TIMES_RAW_STAGE(
	DOMAIN,
    METADATA,
    METADATA_FILENAME,
	JSON,
	ID,
	SYSTEM_QUERY_TS,
    TASK_ID,
    EXECUTION_ID
) as 
SELECT 
	DOMAIN,
    METADATA,
    METADATA_FILENAME,
	JSON,
	ID,
	SYSTEM_QUERY_TS,
    TASK_ID,
    EXECUTION_ID
FROM 
    "DATALAKE_DEV"."ETL_REPLACE_TEST_BHA_DEV"."ACTION_TIMES_RAW_STAGE"
;

create or replace view SRC_UPDATE_CASE_DELETIONS_ALL(
	CASEID,
    CASETYPE,
	FORMID,
	FORMDATE
) as 
select distinct case when typeof(caselist.this) = 'ARRAY' then caselist.value:"@case_id"::string else caselist.this:"@case_id"::string end CASEID, 
case when typeof(caselist.this) = 'ARRAY' then caselist.value:create.case_type::string else caselist.this:create.case_type::string end CASETYPE, 
JSON:id::string FORMID, JSON:server_modified_on::string FORMDATE
    from SRC_FORMS_RAW,
    lateral flatten(input => JSON:form.case) caselist
    where JSON:archived = 'true'and CASETYPE is not null
;

create or replace view SRC_UPDATE_CASE_DELETIONS_STAGE(
	CASEID,
    CASETYPE,
	FORMID,
	FORMDATE
) as 
select distinct case when typeof(caselist.this) = 'ARRAY' then caselist.value:"@case_id"::string else caselist.this:"@case_id"::string end CASEID, 
case when typeof(caselist.this) = 'ARRAY' then caselist.value:create.case_type::string else caselist.this:create.case_type::string end CASETYPE, 
JSON:id::string FORMID, JSON:server_modified_on::string FORMDATE
    from SRC_FORMS_RAW_STAGE,
    lateral flatten(input => JSON:form.case) caselist
    where JSON:archived = 'true'and CASETYPE is not null
;
*/
-- if testing project
use role user_dbt_test;
-- if not testing project
use role sysadmin;

create or replace TABLE CONFIG_CASE_FIELDS (
	CASE_TYPE VARCHAR(16777216),
	FIELD_NAME VARCHAR(16777216),
	FIELD_ALIAS VARCHAR(16777216),
	FIELD_DATA_TYPE VARCHAR(8),
	DATA_TYPE_OVERRIDE VARCHAR(16777216),
	FIELD_NAME_OVERRIDE VARCHAR(16777216),
	INCLUDE_IN_DIM BOOLEAN,
	TAGS VARCHAR(16777216),
	INSERT_DATE TIMESTAMP_LTZ(9),
	INSERT_BY VARCHAR(16777216),
	UPDATE_DATE TIMESTAMP_NTZ(9),
	UPDATE_BY VARCHAR(16777216)
);

create or replace TABLE CONFIG_FORM_FIELDS (
	PROJECT VARCHAR(16777216),
	FIELD_NAME VARCHAR(16777216),
	FIELD_ALIAS VARCHAR(16777216),
	FIELD_PATTERN1 VARCHAR(16777216),
	FIELD_PATTERN2 VARCHAR(16777216),
	FIELD_PATTERN3 VARCHAR(16777216),
	INCLUDE_IN_FORM_VALUES BOOLEAN,
	INSERT_DATE TIMESTAMP_LTZ(9),
	INSERT_BY VARCHAR(16777216),
	UPDATE_DATE TIMESTAMP_LTZ(9),
	UPDATE_BY VARCHAR(16777216)
);

create or replace TABLE CONFIG_FIXTURE_FIELDS (
	FIXTURE_TYPE VARCHAR(16777216),
	FIELD_NAME VARCHAR(16777216),
	FIELD_ALIAS VARCHAR(16777216),
	FIELD_DATA_TYPE VARCHAR(8),
	DATA_TYPE_OVERRIDE VARCHAR(16777216),
	FIELD_NAME_OVERRIDE VARCHAR(16777216),
	INCLUDE_IN_DIM BOOLEAN,
	TAGS VARCHAR(16777216),
	INSERT_DATE TIMESTAMP_LTZ(9),
	INSERT_BY VARCHAR(16777216),
	UPDATE_DATE TIMESTAMP_NTZ(9),
	UPDATE_BY VARCHAR(16777216)
);

create or replace TABLE CONFIG_LOCATION_FIELDS (
	FIELD_NAME VARCHAR(16777216),
	FIELD_ALIAS VARCHAR(16777216),
	FIELD_DATA_TYPE VARCHAR(8),
	DATA_TYPE_OVERRIDE VARCHAR(16777216),
	FIELD_NAME_OVERRIDE VARCHAR(16777216),
	INCLUDE_IN_DIM BOOLEAN,
	TAGS VARCHAR(16777216),
	INSERT_DATE TIMESTAMP_LTZ(9),
	INSERT_BY VARCHAR(16777216),
	UPDATE_DATE TIMESTAMP_NTZ(9),
	UPDATE_BY VARCHAR(16777216)
);

create or replace TABLE CONFIG_WEB_USER_FIELDS (
	FIELD_NAME VARCHAR(16777216),
	FIELD_ALIAS VARCHAR(16777216),
	FIELD_DATA_TYPE VARCHAR(8),
	DATA_TYPE_OVERRIDE VARCHAR(16777216),
	FIELD_NAME_OVERRIDE VARCHAR(16777216),
	INCLUDE_IN_DIM BOOLEAN,
	TAGS VARCHAR(16777216),
	INSERT_DATE TIMESTAMP_LTZ(9),
	INSERT_BY VARCHAR(16777216),
	UPDATE_DATE TIMESTAMP_NTZ(9),
	UPDATE_BY VARCHAR(16777216)
);

create or replace TABLE CONFIG_ACTION_TIME_FIELDS (
	FIELD_NAME VARCHAR(16777216),
	FIELD_ALIAS VARCHAR(16777216),
	FIELD_DATA_TYPE VARCHAR(8),
	DATA_TYPE_OVERRIDE VARCHAR(16777216),
	FIELD_NAME_OVERRIDE VARCHAR(16777216),
	INCLUDE_IN_DIM BOOLEAN,
	TAGS VARCHAR(16777216),
	INSERT_DATE TIMESTAMP_LTZ(9),
	INSERT_BY VARCHAR(16777216),
	UPDATE_DATE TIMESTAMP_NTZ(9),
	UPDATE_BY VARCHAR(16777216)
);

/* -- in dbt --
create or replace view CASE_FIELD_VALUES_ALL(
	CASE_ID,
	CLOSED,
	CLOSED_BY,
	DATE_CLOSED,
	DATE_MODIFIED,
	DOMAIN,
	INDEXED_ON,
	OPENED_BY,
	CASE_TYPE,
	FIELD_NAME,
	FIELD_VALUE,
	BOOLEAN_VALUE,
	DATETIME_VALUE,
	DATE_VALUE,
	NUMBER_VALUE,
	STRING_VALUE
) as
    SELECT 
      JSON:id::string                           as Case_Id
      ,JSON:closed:: boolean                    as Closed
      ,JSON:closed_by::varchar                  as Closed_By
      ,JSON:date_closed::datetime               as Date_Closed
      ,JSON:date_modified::datetime             as Date_Modified
      ,JSON:domain::varchar                     as Domain
      ,JSON:indexed_on::datetime                as Indexed_On
      ,JSON:opened_by::varchar                  as Opened_By
      ,JSON:properties.case_type::string        as Case_Type
      ,f.path                                   as Field_Name
      ,f.value                                  as Field_Value
      ,try_cast(f.value::string as boolean)     as Boolean_Value
      ,try_cast(f.value::string as timestamp)   as Datetime_Value
      ,try_cast(f.value::string as date)        as Date_Value
      ,try_cast(f.value::string as number)      as Number_Value
      ,f.value::string                          as String_Value
    FROM 
        SRC_CASES_RAW, 
        lateral flatten(JSON, recursive=>true) f
    WHERE 
        typeof(f.value) <> 'OBJECT'
        and f.path not like 'xform_ids%'
;

create or replace view CASE_FIELD_VALUES_STG(
	CASE_ID,
	CLOSED,
	CLOSED_BY,
	DATE_CLOSED,
	DATE_MODIFIED,
	DOMAIN,
	INDEXED_ON,
	OPENED_BY,
	CASE_TYPE,
	FIELD_NAME,
	FIELD_VALUE,
	BOOLEAN_VALUE,
	DATETIME_VALUE,
	DATE_VALUE,
	NUMBER_VALUE,
	STRING_VALUE
) as
    SELECT 
      JSON:id::string                           as Case_Id
      ,JSON:closed:: boolean                    as Closed
      ,JSON:closed_by::varchar                  as Closed_By
      ,JSON:date_closed::datetime               as Date_Closed
      ,JSON:date_modified::datetime             as Date_Modified
      ,JSON:domain::varchar                     as Domain
      ,JSON:indexed_on::datetime                as Indexed_On
      ,JSON:opened_by::varchar                  as Opened_By
      ,JSON:properties.case_type::string        as Case_Type
      ,f.path                                   as Field_Name
      ,f.value                                  as Field_Value
      ,try_cast(f.value::string as boolean)     as Boolean_Value
      ,try_cast(f.value::string as timestamp)   as Datetime_Value
      ,try_cast(f.value::string as date)        as Date_Value
      ,try_cast(f.value::string as number)      as Number_Value
      ,f.value::string                          as String_Value
    FROM 
        SRC_CASES_RAW_STAGE, 
        lateral flatten(JSON, recursive=>true) f
    WHERE 
        typeof(f.value) <> 'OBJECT'
        and f.path not like 'xform_ids%'
;

create or replace view FIXTURE_FIELD_VALUES_ALL(
	FIXTURE_ID,
	FIXTURE_TYPE,
	FIELD_NAME,
	FIELD_VALUE,
	BOOLEAN_VALUE,
	DATETIME_VALUE,
	DATE_VALUE,
	NUMBER_VALUE,
	STRING_VALUE
) as
    SELECT 
      JSON:id::string                           as Fixture_Id
      ,JSON:fixture_type::string                as Fixture_Type
      ,f.path                                   as Field_Name
      ,f.value                                  as Field_Value
      ,try_cast(f.value::string as boolean)     as Boolean_Value
      ,try_cast(f.value::string as timestamp)   as Datetime_Value
      ,try_cast(f.value::string as date)        as Date_Value
      ,try_cast(f.value::string as number)      as Number_Value
      ,f.value::string                          as String_Value
    FROM 
        SRC_FIXTURES_RAW, 
        lateral flatten(JSON, recursive=>true) f
    WHERE 
        typeof(f.value) <> 'OBJECT'
        and f.path not like 'xform_ids%'
;

create or replace view FIXTURE_FIELD_VALUES_STG(
	FIXTURE_ID,
	FIXTURE_TYPE,
	FIELD_NAME,
	FIELD_VALUE,
	BOOLEAN_VALUE,
	DATETIME_VALUE,
	DATE_VALUE,
	NUMBER_VALUE,
	STRING_VALUE
) as
    SELECT 
      JSON:id::string                           as Fixture_Id
      ,JSON:fixture_type::string                as Fixture_Type
      ,f.path                                   as Field_Name
      ,f.value                                  as Field_Value
      ,try_cast(f.value::string as boolean)     as Boolean_Value
      ,try_cast(f.value::string as timestamp)   as Datetime_Value
      ,try_cast(f.value::string as date)        as Date_Value
      ,try_cast(f.value::string as number)      as Number_Value
      ,f.value::string                          as String_Value
    FROM 
        SRC_FIXTURES_RAW_STAGE, 
        lateral flatten(JSON, recursive=>true) f
    WHERE 
        typeof(f.value) <> 'OBJECT'
        and f.path not like 'xform_ids%'
;

create or replace view LOCATION_FIELD_VALUES_ALL(
	LOCATION_ID,
	FIELD_NAME,
	FIELD_VALUE,
	BOOLEAN_VALUE,
	DATETIME_VALUE,
	DATE_VALUE,
	NUMBER_VALUE,
	STRING_VALUE
) as
    SELECT 
      JSON:id::string                           as Location_Id
      ,f.path                                   as Field_Name
      ,f.value                                  as Field_Value
      ,try_cast(f.value::string as boolean)     as Boolean_Value
      ,try_cast(f.value::string as timestamp)   as Datetime_Value
      ,try_cast(f.value::string as date)        as Date_Value
      ,try_cast(f.value::string as number)      as Number_Value
      ,f.value::string                          as String_Value
    FROM 
        SRC_LOCATIONS_RAW, 
        lateral flatten(JSON, recursive=>true) f
    WHERE 
        typeof(f.value) <> 'OBJECT'
        and f.path not like 'xform_ids%'
;

create or replace view LOCATION_FIELD_VALUES_STG(
	LOCATION_ID,
	FIELD_NAME,
	FIELD_VALUE,
	BOOLEAN_VALUE,
	DATETIME_VALUE,
	DATE_VALUE,
	NUMBER_VALUE,
	STRING_VALUE
) as
    SELECT 
      JSON:id::string                           as Location_Id
      ,f.path                                   as Field_Name
      ,f.value                                  as Field_Value
      ,try_cast(f.value::string as boolean)     as Boolean_Value
      ,try_cast(f.value::string as timestamp)   as Datetime_Value
      ,try_cast(f.value::string as date)        as Date_Value
      ,try_cast(f.value::string as number)      as Number_Value
      ,f.value::string                          as String_Value
    FROM 
        SRC_LOCATIONS_RAW_STAGE, 
        lateral flatten(JSON, recursive=>true) f
    WHERE 
        typeof(f.value) <> 'OBJECT'
        and f.path not like 'xform_ids%'
;

create or replace view WEB_USER_FIELD_VALUES_ALL(
	WEB_USER_ID,
	FIELD_NAME,
	FIELD_VALUE,
	BOOLEAN_VALUE,
	DATETIME_VALUE,
	DATE_VALUE,
	NUMBER_VALUE,
	STRING_VALUE
) as
    SELECT 
      JSON:id::string                           as WEB_USER_Id
      ,f.path                                   as Field_Name
      ,f.value                                  as Field_Value
      ,try_cast(f.value::string as boolean)     as Boolean_Value
      ,try_cast(f.value::string as timestamp)   as Datetime_Value
      ,try_cast(f.value::string as date)        as Date_Value
      ,try_cast(f.value::string as number)      as Number_Value
      ,f.value::string                          as String_Value
    FROM 
        SRC_WEB_USERS_RAW, 
        lateral flatten(JSON, recursive=>true) f
    WHERE 
        typeof(f.value) <> 'OBJECT'
        and f.path not like 'xform_ids%'
;

create or replace view WEB_USER_FIELD_VALUES_STG(
	WEB_USER_ID,
	FIELD_NAME,
	FIELD_VALUE,
	BOOLEAN_VALUE,
	DATETIME_VALUE,
	DATE_VALUE,
	NUMBER_VALUE,
	STRING_VALUE
) as
    SELECT 
      JSON:id::string                           as WEB_USER_Id
      ,f.path                                   as Field_Name
      ,f.value                                  as Field_Value
      ,try_cast(f.value::string as boolean)     as Boolean_Value
      ,try_cast(f.value::string as timestamp)   as Datetime_Value
      ,try_cast(f.value::string as date)        as Date_Value
      ,try_cast(f.value::string as number)      as Number_Value
      ,f.value::string                          as String_Value
    FROM 
        SRC_WEB_USERS_RAW_STAGE, 
        lateral flatten(JSON, recursive=>true) f
    WHERE 
        typeof(f.value) <> 'OBJECT'
        and f.path not like 'xform_ids%'
;

create or replace view ACTION_TIME_FIELD_VALUES_ALL(
	ACTION_TIME_ID,
	FIELD_NAME,
	FIELD_VALUE,
	BOOLEAN_VALUE,
	DATETIME_VALUE,
	DATE_VALUE,
	NUMBER_VALUE,
	STRING_VALUE
) as
    SELECT 
      JSON:id::string                           as ACTION_TIME_Id
      ,f.path                                   as Field_Name
      ,f.value                                  as Field_Value
      ,try_cast(f.value::string as boolean)     as Boolean_Value
      ,try_cast(f.value::string as timestamp)   as Datetime_Value
      ,try_cast(f.value::string as date)        as Date_Value
      ,try_cast(f.value::string as number)      as Number_Value
      ,f.value::string                          as String_Value
    FROM 
        SRC_ACTION_TIMES_RAW, 
        lateral flatten(JSON, recursive=>true) f
    WHERE 
        typeof(f.value) <> 'OBJECT'
        and f.path not like 'xform_ids%'
;

create or replace view ACTION_TIME_FIELD_VALUES_STG(
	ACTION_TIME_ID,
	FIELD_NAME,
	FIELD_VALUE,
	BOOLEAN_VALUE,
	DATETIME_VALUE,
	DATE_VALUE,
	NUMBER_VALUE,
	STRING_VALUE
) as
    SELECT 
      JSON:id::string                           as ACTION_TIME_ID
      ,f.path                                   as Field_Name
      ,f.value                                  as Field_Value
      ,try_cast(f.value::string as boolean)     as Boolean_Value
      ,try_cast(f.value::string as timestamp)   as Datetime_Value
      ,try_cast(f.value::string as date)        as Date_Value
      ,try_cast(f.value::string as number)      as Number_Value
      ,f.value::string                          as String_Value
    FROM 
        SRC_ACTION_TIMES_RAW_STAGE, 
        lateral flatten(JSON, recursive=>true) f
    WHERE 
        typeof(f.value) <> 'OBJECT'
        and f.path not like 'xform_ids%'
;

create or replace view CASE_FIELD_VALUE_DATA_TYPES_ALL(
	CASE_TYPE,
	FIELD_NAME,
	VALUE_DATA_TYPE,
	DATA_TYPE_COUNT,
	FIELD_COUNT,
	DATA_TYPE_PERC,
	IS_DUPE
) as
  WITH Field_Types as (
    SELECT
      CASE_TYPE
      ,FIELD_NAME
      ,FIELD_VALUE
      ,CASE WHEN Boolean_Value  IS NOT NULL THEN 'Boolean'
            WHEN Datetime_Value IS NOT NULL and (hour(Datetime_Value)<>0 or minute(Datetime_Value)<>0 or second(Datetime_Value)<>0) THEN 'Datetime'
            WHEN Date_Value     IS NOT NULL THEN 'Date'
            WHEN Number_Value   IS NOT NULL THEN 'Number'
            ELSE 'Varchar' END as Value_Data_Type
    FROM
        Case_Field_Values_All
  )
  ,Field_Type_Count as (
    SELECT DISTINCT
      CASE_TYPE
      ,FIELD_NAME
      ,VALUE_DATA_TYPE
      ,count(*) OVER (PARTITION BY CASE_TYPE,FIELD_NAME,VALUE_DATA_TYPE) as Data_Type_Count
      ,count(*) OVER (PARTITION BY CASE_TYPE,FIELD_NAME)                 as Field_Count
      ,(Data_Type_Count/Field_Count*100)::decimal(5,2)                   as Data_Type_Perc
    FROM
        Field_Types
    WHERE
        NULLIF(FIELD_VALUE,'') is not null
  )
  ,Field_Distinct as (
    SELECT DISTINCT
      CASE_TYPE
      ,FIELD_NAME
    FROM
        Field_Types
  )
  SELECT 
      FD.CASE_TYPE, FD.FIELD_NAME, IFNULL(TC.VALUE_DATA_TYPE, 'Varchar') VALUE_DATA_TYPE,
      IFNULL(TC.Data_Type_Count, 0) Data_Type_Count, IFNULL(TC.Field_Count, 0) Field_Count, IFNULL(TC.Data_Type_Perc, 100.00) Data_Type_Perc
      ,CASE WHEN Dupes.CASE_TYPE IS NOT NULL THEN True ELSE False END::Boolean as Is_Dupe
  FROM Field_Distinct FD
      LEFT JOIN Field_Type_Count TC on TC.CASE_TYPE = FD.CASE_TYPE and TC.FIELD_NAME = FD.FIELD_NAME
      LEFT JOIN (
            SELECT
              CASE_TYPE
              ,FIELD_NAME
            FROM
                Field_Type_Count
            GROUP BY
                CASE_TYPE
                ,FIELD_NAME
            HAVING count(distinct VALUE_DATA_TYPE)>1
            ) as Dupes
          on TC.CASE_TYPE = Dupes.CASE_TYPE and TC.FIELD_NAME = Dupes.FIELD_NAME
  ORDER BY
      CASE_TYPE
      ,FIELD_NAME
      ,Data_Type_Count desc
;

create or replace view CASE_FIELD_VALUE_DATA_TYPES_STG(
	CASE_TYPE,
	FIELD_NAME,
	VALUE_DATA_TYPE,
	DATA_TYPE_COUNT,
	FIELD_COUNT,
	DATA_TYPE_PERC,
	IS_DUPE
) as
  WITH Field_Types as (
    SELECT
      CASE_TYPE
      ,FIELD_NAME
      ,FIELD_VALUE
      ,CASE WHEN Boolean_Value  IS NOT NULL THEN 'Boolean'
            WHEN Datetime_Value IS NOT NULL and (hour(Datetime_Value)<>0 or minute(Datetime_Value)<>0 or second(Datetime_Value)<>0) THEN 'Datetime'
            WHEN Date_Value     IS NOT NULL THEN 'Date'
            WHEN Number_Value   IS NOT NULL THEN 'Number'
            ELSE 'Varchar' END as Value_Data_Type
    FROM
        Case_Field_Values_Stg
  )
  ,Field_Type_Count as (
    SELECT DISTINCT
      CASE_TYPE
      ,FIELD_NAME
      ,VALUE_DATA_TYPE
      ,count(*) OVER (PARTITION BY CASE_TYPE,FIELD_NAME,VALUE_DATA_TYPE) as Data_Type_Count
      ,count(*) OVER (PARTITION BY CASE_TYPE,FIELD_NAME)                 as Field_Count
      ,(Data_Type_Count/Field_Count*100)::decimal(5,2)                   as Data_Type_Perc
    FROM
        Field_Types
    WHERE
        NULLIF(FIELD_VALUE,'') is not null
  )
  ,Field_Distinct as (
    SELECT DISTINCT
      CASE_TYPE
      ,FIELD_NAME
    FROM
        Field_Types
  )
  SELECT 
      FD.CASE_TYPE, FD.FIELD_NAME, IFNULL(TC.VALUE_DATA_TYPE, 'Varchar') VALUE_DATA_TYPE,
      IFNULL(TC.Data_Type_Count, 0) Data_Type_Count, IFNULL(TC.Field_Count, 0) Field_Count, IFNULL(TC.Data_Type_Perc, 100.00) Data_Type_Perc
      ,CASE WHEN Dupes.CASE_TYPE IS NOT NULL THEN True ELSE False END::Boolean as Is_Dupe
  FROM Field_Distinct FD
      LEFT JOIN Field_Type_Count TC on TC.CASE_TYPE = FD.CASE_TYPE and TC.FIELD_NAME = FD.FIELD_NAME
      LEFT JOIN (
            SELECT
              CASE_TYPE
              ,FIELD_NAME
            FROM
                Field_Type_Count
            GROUP BY
                CASE_TYPE
                ,FIELD_NAME
            HAVING count(distinct VALUE_DATA_TYPE)>1
            ) as Dupes
          on TC.CASE_TYPE = Dupes.CASE_TYPE and TC.FIELD_NAME = Dupes.FIELD_NAME
  ORDER BY
      CASE_TYPE
      ,FIELD_NAME
      ,Data_Type_Count desc
;

create or replace view FIXTURE_FIELD_VALUE_DATA_TYPES_ALL(
	FIXTURE_TYPE,
	FIELD_NAME,
	VALUE_DATA_TYPE,
	DATA_TYPE_COUNT,
	FIELD_COUNT,
	DATA_TYPE_PERC,
	IS_DUPE
) as
  WITH Field_Types as (
    SELECT
      FIXTURE_TYPE
      ,FIELD_NAME
      ,FIELD_VALUE
      ,CASE WHEN Boolean_Value  IS NOT NULL THEN 'Boolean'
            WHEN Datetime_Value IS NOT NULL and (hour(Datetime_Value)<>0 or minute(Datetime_Value)<>0 or second(Datetime_Value)<>0) THEN 'Datetime'
            WHEN Date_Value     IS NOT NULL THEN 'Date'
            WHEN Number_Value   IS NOT NULL THEN 'Number'
            ELSE 'Varchar' END as Value_Data_Type
    FROM
        Fixture_Field_Values_All
  )
  ,Field_Type_Count as (
    SELECT DISTINCT
      FIXTURE_TYPE
      ,FIELD_NAME
      ,VALUE_DATA_TYPE
      ,count(*) OVER (PARTITION BY FIXTURE_TYPE,FIELD_NAME,VALUE_DATA_TYPE) as Data_Type_Count
      ,count(*) OVER (PARTITION BY FIXTURE_TYPE,FIELD_NAME)                 as Field_Count
      ,(Data_Type_Count/Field_Count*100)::decimal(5,2)                   as Data_Type_Perc
    FROM
        Field_Types
    WHERE
        NULLIF(FIELD_VALUE,'') is not null
  )
  ,Field_Distinct as (
    SELECT DISTINCT
      FIXTURE_TYPE
      ,FIELD_NAME
    FROM
        Field_Types
  )
  SELECT 
      FD.FIXTURE_TYPE, FD.FIELD_NAME, IFNULL(TC.VALUE_DATA_TYPE, 'Varchar') VALUE_DATA_TYPE,
      IFNULL(TC.Data_Type_Count, 0) Data_Type_Count, IFNULL(TC.Field_Count, 0) Field_Count, IFNULL(TC.Data_Type_Perc, 100.00) Data_Type_Perc
      ,CASE WHEN Dupes.FIXTURE_TYPE IS NOT NULL THEN True ELSE False END::Boolean as Is_Dupe
  FROM Field_Distinct FD
      LEFT JOIN Field_Type_Count TC on TC.FIXTURE_TYPE = FD.FIXTURE_TYPE and TC.FIELD_NAME = FD.FIELD_NAME
      LEFT JOIN (
            SELECT
              FIXTURE_TYPE
              ,FIELD_NAME
            FROM
                Field_Type_Count
            GROUP BY
                FIXTURE_TYPE
                ,FIELD_NAME
            HAVING count(distinct VALUE_DATA_TYPE)>1
            ) as Dupes
          on TC.FIXTURE_TYPE = Dupes.FIXTURE_TYPE and TC.FIELD_NAME = Dupes.FIELD_NAME
  ORDER BY
      FIXTURE_TYPE
      ,FIELD_NAME
      ,Data_Type_Count desc
;

create or replace view FIXTURE_FIELD_VALUE_DATA_TYPES_STG(
	FIXTURE_TYPE,
	FIELD_NAME,
	VALUE_DATA_TYPE,
	DATA_TYPE_COUNT,
	FIELD_COUNT,
	DATA_TYPE_PERC,
	IS_DUPE
) as
  WITH Field_Types as (
    SELECT
      FIXTURE_TYPE
      ,FIELD_NAME
      ,FIELD_VALUE
      ,CASE WHEN Boolean_Value  IS NOT NULL THEN 'Boolean'
            WHEN Datetime_Value IS NOT NULL and (hour(Datetime_Value)<>0 or minute(Datetime_Value)<>0 or second(Datetime_Value)<>0) THEN 'Datetime'
            WHEN Date_Value     IS NOT NULL THEN 'Date'
            WHEN Number_Value   IS NOT NULL THEN 'Number'
            ELSE 'Varchar' END as Value_Data_Type
    FROM
        Fixture_Field_Values_Stg
  )
  ,Field_Type_Count as (
    SELECT DISTINCT
      FIXTURE_TYPE
      ,FIELD_NAME
      ,VALUE_DATA_TYPE
      ,count(*) OVER (PARTITION BY FIXTURE_TYPE,FIELD_NAME,VALUE_DATA_TYPE) as Data_Type_Count
      ,count(*) OVER (PARTITION BY FIXTURE_TYPE,FIELD_NAME)                 as Field_Count
      ,(Data_Type_Count/Field_Count*100)::decimal(5,2)                   as Data_Type_Perc
    FROM
        Field_Types
    WHERE
        NULLIF(FIELD_VALUE,'') is not null
  )
  ,Field_Distinct as (
    SELECT DISTINCT
      FIXTURE_TYPE
      ,FIELD_NAME
    FROM
        Field_Types
  )
  SELECT 
      FD.FIXTURE_TYPE, FD.FIELD_NAME, IFNULL(TC.VALUE_DATA_TYPE, 'Varchar') VALUE_DATA_TYPE,
      IFNULL(TC.Data_Type_Count, 0) Data_Type_Count, IFNULL(TC.Field_Count, 0) Field_Count, IFNULL(TC.Data_Type_Perc, 100.00) Data_Type_Perc
      ,CASE WHEN Dupes.FIXTURE_TYPE IS NOT NULL THEN True ELSE False END::Boolean as Is_Dupe
  FROM Field_Distinct FD
      LEFT JOIN Field_Type_Count TC on TC.FIXTURE_TYPE = FD.FIXTURE_TYPE and TC.FIELD_NAME = FD.FIELD_NAME
      LEFT JOIN (
            SELECT
              FIXTURE_TYPE
              ,FIELD_NAME
            FROM
                Field_Type_Count
            GROUP BY
                FIXTURE_TYPE
                ,FIELD_NAME
            HAVING count(distinct VALUE_DATA_TYPE)>1
            ) as Dupes
          on TC.FIXTURE_TYPE = Dupes.FIXTURE_TYPE and TC.FIELD_NAME = Dupes.FIELD_NAME
  ORDER BY
      FIXTURE_TYPE
      ,FIELD_NAME
      ,Data_Type_Count desc
;

create or replace view LOCATION_FIELD_VALUE_DATA_TYPES_ALL(
	FIELD_NAME,
	VALUE_DATA_TYPE,
	DATA_TYPE_COUNT,
	FIELD_COUNT,
	DATA_TYPE_PERC,
	IS_DUPE
) as
  WITH Field_Types as (
    SELECT
      FIELD_NAME
      ,FIELD_VALUE
      ,CASE WHEN Boolean_Value  IS NOT NULL THEN 'Boolean'
            WHEN Datetime_Value IS NOT NULL and (hour(Datetime_Value)<>0 or minute(Datetime_Value)<>0 or second(Datetime_Value)<>0) THEN 'Datetime'
            WHEN Date_Value     IS NOT NULL THEN 'Date'
            WHEN Number_Value   IS NOT NULL THEN 'Number'
            ELSE 'Varchar' END as Value_Data_Type
    FROM
        Location_Field_Values_All
  )
  ,Field_Type_Count as (
    SELECT DISTINCT
      FIELD_NAME
      ,VALUE_DATA_TYPE
      ,count(*) OVER (PARTITION BY FIELD_NAME,VALUE_DATA_TYPE) as Data_Type_Count
      ,count(*) OVER (PARTITION BY FIELD_NAME)                 as Field_Count
      ,(Data_Type_Count/Field_Count*100)::decimal(5,2)                   as Data_Type_Perc
    FROM
        Field_Types
    WHERE
        NULLIF(FIELD_VALUE,'') is not null
  )
  ,Field_Distinct as (
    SELECT DISTINCT
      FIELD_NAME
    FROM
        Field_Types
  )
  SELECT 
      FD.FIELD_NAME, IFNULL(TC.VALUE_DATA_TYPE, 'Varchar') VALUE_DATA_TYPE,
      IFNULL(TC.Data_Type_Count, 0) Data_Type_Count, IFNULL(TC.Field_Count, 0) Field_Count, IFNULL(TC.Data_Type_Perc, 100.00) Data_Type_Perc
      ,CASE WHEN Dupes.FIELD_NAME IS NOT NULL THEN True ELSE False END::Boolean as Is_Dupe
  FROM Field_Distinct FD
      LEFT JOIN Field_Type_Count TC on TC.FIELD_NAME = FD.FIELD_NAME
      LEFT JOIN (
            SELECT
              FIELD_NAME
            FROM
                Field_Type_Count
            GROUP BY
                FIELD_NAME
            HAVING count(distinct VALUE_DATA_TYPE)>1
            ) as Dupes
          on TC.FIELD_NAME = Dupes.FIELD_NAME
  ORDER BY
      FIELD_NAME
      ,Data_Type_Count desc
;

create or replace view LOCATION_FIELD_VALUE_DATA_TYPES_STG(
	FIELD_NAME,
	VALUE_DATA_TYPE,
	DATA_TYPE_COUNT,
	FIELD_COUNT,
	DATA_TYPE_PERC,
	IS_DUPE
) as
  WITH Field_Types as (
    SELECT
      FIELD_NAME
      ,FIELD_VALUE
      ,CASE WHEN Boolean_Value  IS NOT NULL THEN 'Boolean'
            WHEN Datetime_Value IS NOT NULL and (hour(Datetime_Value)<>0 or minute(Datetime_Value)<>0 or second(Datetime_Value)<>0) THEN 'Datetime'
            WHEN Date_Value     IS NOT NULL THEN 'Date'
            WHEN Number_Value   IS NOT NULL THEN 'Number'
            ELSE 'Varchar' END as Value_Data_Type
    FROM
        Location_Field_Values_Stg
  )
  ,Field_Type_Count as (
    SELECT DISTINCT
      FIELD_NAME
      ,VALUE_DATA_TYPE
      ,count(*) OVER (PARTITION BY FIELD_NAME,VALUE_DATA_TYPE) as Data_Type_Count
      ,count(*) OVER (PARTITION BY FIELD_NAME)                 as Field_Count
      ,(Data_Type_Count/Field_Count*100)::decimal(5,2)                   as Data_Type_Perc
    FROM
        Field_Types
    WHERE
        NULLIF(FIELD_VALUE,'') is not null
  )
  ,Field_Distinct as (
    SELECT DISTINCT
      FIELD_NAME
    FROM
        Field_Types
  )
  SELECT 
      FD.FIELD_NAME, IFNULL(TC.VALUE_DATA_TYPE, 'Varchar') VALUE_DATA_TYPE,
      IFNULL(TC.Data_Type_Count, 0) Data_Type_Count, IFNULL(TC.Field_Count, 0) Field_Count, IFNULL(TC.Data_Type_Perc, 100.00) Data_Type_Perc
      ,CASE WHEN Dupes.FIELD_NAME IS NOT NULL THEN True ELSE False END::Boolean as Is_Dupe
  FROM Field_Distinct FD
      LEFT JOIN Field_Type_Count TC on TC.FIELD_NAME = FD.FIELD_NAME
      LEFT JOIN (
            SELECT
              FIELD_NAME
            FROM
                Field_Type_Count
            GROUP BY
                FIELD_NAME
            HAVING count(distinct VALUE_DATA_TYPE)>1
            ) as Dupes
          on TC.FIELD_NAME = Dupes.FIELD_NAME
  ORDER BY
      FIELD_NAME
      ,Data_Type_Count desc
;

create or replace view WEB_USER_FIELD_VALUE_DATA_TYPES_ALL(
	FIELD_NAME,
	VALUE_DATA_TYPE,
	DATA_TYPE_COUNT,
	FIELD_COUNT,
	DATA_TYPE_PERC,
	IS_DUPE
) as
  WITH Field_Types as (
    SELECT
      FIELD_NAME
      ,FIELD_VALUE
      ,CASE WHEN Boolean_Value  IS NOT NULL THEN 'Boolean'
            WHEN Datetime_Value IS NOT NULL and (hour(Datetime_Value)<>0 or minute(Datetime_Value)<>0 or second(Datetime_Value)<>0) THEN 'Datetime'
            WHEN Date_Value     IS NOT NULL THEN 'Date'
            WHEN Number_Value   IS NOT NULL THEN 'Number'
            ELSE 'Varchar' END as Value_Data_Type
    FROM
        Web_User_Field_Values_All
  )
  ,Field_Type_Count as (
    SELECT DISTINCT
      FIELD_NAME
      ,VALUE_DATA_TYPE
      ,count(*) OVER (PARTITION BY FIELD_NAME,VALUE_DATA_TYPE) as Data_Type_Count
      ,count(*) OVER (PARTITION BY FIELD_NAME)                 as Field_Count
      ,(Data_Type_Count/Field_Count*100)::decimal(5,2)                   as Data_Type_Perc
    FROM
        Field_Types
    WHERE
        NULLIF(FIELD_VALUE,'') is not null
  )
  ,Field_Distinct as (
    SELECT DISTINCT
      FIELD_NAME
    FROM
        Field_Types
  )
  SELECT 
      FD.FIELD_NAME, IFNULL(TC.VALUE_DATA_TYPE, 'Varchar') VALUE_DATA_TYPE,
      IFNULL(TC.Data_Type_Count, 0) Data_Type_Count, IFNULL(TC.Field_Count, 0) Field_Count, IFNULL(TC.Data_Type_Perc, 100.00) Data_Type_Perc
      ,CASE WHEN Dupes.FIELD_NAME IS NOT NULL THEN True ELSE False END::Boolean as Is_Dupe
  FROM Field_Distinct FD
      LEFT JOIN Field_Type_Count TC on TC.FIELD_NAME = FD.FIELD_NAME
      LEFT JOIN (
            SELECT
              FIELD_NAME
            FROM
                Field_Type_Count
            GROUP BY
                FIELD_NAME
            HAVING count(distinct VALUE_DATA_TYPE)>1
            ) as Dupes
          on TC.FIELD_NAME = Dupes.FIELD_NAME
  ORDER BY
      FIELD_NAME
      ,Data_Type_Count desc
;

create or replace view WEB_USER_FIELD_VALUE_DATA_TYPES_STG(
	FIELD_NAME,
	VALUE_DATA_TYPE,
	DATA_TYPE_COUNT,
	FIELD_COUNT,
	DATA_TYPE_PERC,
	IS_DUPE
) as
  WITH Field_Types as (
    SELECT
      FIELD_NAME
      ,FIELD_VALUE
      ,CASE WHEN Boolean_Value  IS NOT NULL THEN 'Boolean'
            WHEN Datetime_Value IS NOT NULL and (hour(Datetime_Value)<>0 or minute(Datetime_Value)<>0 or second(Datetime_Value)<>0) THEN 'Datetime'
            WHEN Date_Value     IS NOT NULL THEN 'Date'
            WHEN Number_Value   IS NOT NULL THEN 'Number'
            ELSE 'Varchar' END as Value_Data_Type
    FROM
        Web_User_Field_Values_Stg
  )
  ,Field_Type_Count as (
    SELECT DISTINCT
      FIELD_NAME
      ,VALUE_DATA_TYPE
      ,count(*) OVER (PARTITION BY FIELD_NAME,VALUE_DATA_TYPE) as Data_Type_Count
      ,count(*) OVER (PARTITION BY FIELD_NAME)                 as Field_Count
      ,(Data_Type_Count/Field_Count*100)::decimal(5,2)                   as Data_Type_Perc
    FROM
        Field_Types
    WHERE
        NULLIF(FIELD_VALUE,'') is not null
  )
  ,Field_Distinct as (
    SELECT DISTINCT
      FIELD_NAME
    FROM
        Field_Types
  )
  SELECT 
      FD.FIELD_NAME, IFNULL(TC.VALUE_DATA_TYPE, 'Varchar') VALUE_DATA_TYPE,
      IFNULL(TC.Data_Type_Count, 0) Data_Type_Count, IFNULL(TC.Field_Count, 0) Field_Count, IFNULL(TC.Data_Type_Perc, 100.00) Data_Type_Perc
      ,CASE WHEN Dupes.FIELD_NAME IS NOT NULL THEN True ELSE False END::Boolean as Is_Dupe
  FROM Field_Distinct FD
      LEFT JOIN Field_Type_Count TC on TC.FIELD_NAME = FD.FIELD_NAME
      LEFT JOIN (
            SELECT
              FIELD_NAME
            FROM
                Field_Type_Count
            GROUP BY
                FIELD_NAME
            HAVING count(distinct VALUE_DATA_TYPE)>1
            ) as Dupes
          on TC.FIELD_NAME = Dupes.FIELD_NAME
  ORDER BY
      FIELD_NAME
      ,Data_Type_Count desc
;

create or replace view ACTION_TIME_FIELD_VALUE_DATA_TYPES_ALL(
	FIELD_NAME,
	VALUE_DATA_TYPE,
	DATA_TYPE_COUNT,
	FIELD_COUNT,
	DATA_TYPE_PERC,
	IS_DUPE
) as
  WITH Field_Types as (
    SELECT
      FIELD_NAME
      ,FIELD_VALUE
      ,CASE WHEN Boolean_Value  IS NOT NULL THEN 'Boolean'
            WHEN Datetime_Value IS NOT NULL and (hour(Datetime_Value)<>0 or minute(Datetime_Value)<>0 or second(Datetime_Value)<>0) THEN 'Datetime'
            WHEN Date_Value     IS NOT NULL THEN 'Date'
            WHEN Number_Value   IS NOT NULL THEN 'Number'
            ELSE 'Varchar' END as Value_Data_Type
    FROM
        Action_Time_Field_Values_All
  )
  ,Field_Type_Count as (
    SELECT DISTINCT
      FIELD_NAME
      ,VALUE_DATA_TYPE
      ,count(*) OVER (PARTITION BY FIELD_NAME,VALUE_DATA_TYPE) as Data_Type_Count
      ,count(*) OVER (PARTITION BY FIELD_NAME)                 as Field_Count
      ,(Data_Type_Count/Field_Count*100)::decimal(5,2)                   as Data_Type_Perc
    FROM
        Field_Types
    WHERE
        NULLIF(FIELD_VALUE,'') is not null
  )
  ,Field_Distinct as (
    SELECT DISTINCT
      FIELD_NAME
    FROM
        Field_Types
  )
  SELECT 
      FD.FIELD_NAME, IFNULL(TC.VALUE_DATA_TYPE, 'Varchar') VALUE_DATA_TYPE,
      IFNULL(TC.Data_Type_Count, 0) Data_Type_Count, IFNULL(TC.Field_Count, 0) Field_Count, IFNULL(TC.Data_Type_Perc, 100.00) Data_Type_Perc
      ,CASE WHEN Dupes.FIELD_NAME IS NOT NULL THEN True ELSE False END::Boolean as Is_Dupe
  FROM Field_Distinct FD
      LEFT JOIN Field_Type_Count TC on TC.FIELD_NAME = FD.FIELD_NAME
      LEFT JOIN (
            SELECT
              FIELD_NAME
            FROM
                Field_Type_Count
            GROUP BY
                FIELD_NAME
            HAVING count(distinct VALUE_DATA_TYPE)>1
            ) as Dupes
          on TC.FIELD_NAME = Dupes.FIELD_NAME
  ORDER BY
      FIELD_NAME
      ,Data_Type_Count desc
;

create or replace view ACTION_TIME_FIELD_VALUE_DATA_TYPES_STG(
	FIELD_NAME,
	VALUE_DATA_TYPE,
	DATA_TYPE_COUNT,
	FIELD_COUNT,
	DATA_TYPE_PERC,
	IS_DUPE
) as
  WITH Field_Types as (
    SELECT
      FIELD_NAME
      ,FIELD_VALUE
      ,CASE WHEN Boolean_Value  IS NOT NULL THEN 'Boolean'
            WHEN Datetime_Value IS NOT NULL and (hour(Datetime_Value)<>0 or minute(Datetime_Value)<>0 or second(Datetime_Value)<>0) THEN 'Datetime'
            WHEN Date_Value     IS NOT NULL THEN 'Date'
            WHEN Number_Value   IS NOT NULL THEN 'Number'
            ELSE 'Varchar' END as Value_Data_Type
    FROM
        Action_Time_Field_Values_Stg
  )
  ,Field_Type_Count as (
    SELECT DISTINCT
      FIELD_NAME
      ,VALUE_DATA_TYPE
      ,count(*) OVER (PARTITION BY FIELD_NAME,VALUE_DATA_TYPE) as Data_Type_Count
      ,count(*) OVER (PARTITION BY FIELD_NAME)                 as Field_Count
      ,(Data_Type_Count/Field_Count*100)::decimal(5,2)                   as Data_Type_Perc
    FROM
        Field_Types
    WHERE
        NULLIF(FIELD_VALUE,'') is not null
  )
  ,Field_Distinct as (
    SELECT DISTINCT
      FIELD_NAME
    FROM
        Field_Types
  )
  SELECT 
      FD.FIELD_NAME, IFNULL(TC.VALUE_DATA_TYPE, 'Varchar') VALUE_DATA_TYPE,
      IFNULL(TC.Data_Type_Count, 0) Data_Type_Count, IFNULL(TC.Field_Count, 0) Field_Count, IFNULL(TC.Data_Type_Perc, 100.00) Data_Type_Perc
      ,CASE WHEN Dupes.FIELD_NAME IS NOT NULL THEN True ELSE False END::Boolean as Is_Dupe
  FROM Field_Distinct FD
      LEFT JOIN Field_Type_Count TC on TC.FIELD_NAME = FD.FIELD_NAME
      LEFT JOIN (
            SELECT
              FIELD_NAME
            FROM
                Field_Type_Count
            GROUP BY
                FIELD_NAME
            HAVING count(distinct VALUE_DATA_TYPE)>1
            ) as Dupes
          on TC.FIELD_NAME = Dupes.FIELD_NAME
  ORDER BY
      FIELD_NAME
      ,Data_Type_Count desc
;

create or replace view CASE_FIELD_NAMES_TYPES_ALL(
	CASE_TYPE,
	FIELD_NAME,
	FIELD_ALIAS,
    FIELD_COUNT,
	FIELD_DATA_TYPE
) as
SELECT 
  CASE_TYPE
  ,FIELD_NAME
  ,REGEXP_REPLACE(
  REGEXP_REPLACE(
    IFNULL(
      REGEXP_SUBSTR(FIELD_NAME, '(\\.|\\[\')([^\']*)(\'\\])?', 1, 1, 'c', 2), 
      FIELD_NAME),
    '-|\\.|\\s','_'),
  '[^a-zA-Z0-9 _ \\$]','') FIELD_ALIAS
  ,FIELD_COUNT
  ,CASE WHEN DATA_TYPE_PERC >= 99 THEN VALUE_DATA_TYPE
        ELSE 'Varchar' END                              as Field_Data_Type
FROM (
    SELECT
      CASE_TYPE
      ,FIELD_NAME
      ,FIELD_COUNT
      ,VALUE_DATA_TYPE
      ,case when VALUE_DATA_TYPE = 'Varchar' then 1 
            when VALUE_DATA_TYPE = 'Number' then 2
            when VALUE_DATA_TYPE = 'Date' then 3
            when VALUE_DATA_TYPE = 'Datetime' then 4
            when VALUE_DATA_TYPE = 'Boolean' then 5
        else 6 end TYPE_SORT
      ,DATA_TYPE_PERC
      ,RANK() OVER (PARTITION BY CASE_TYPE,FIELD_NAME ORDER BY DATA_TYPE_PERC, TYPE_SORT DESC) as VALUE_DATA_TYPE_RANK
    FROM CASE_FIELD_VALUE_DATA_TYPES_ALL
    )
WHERE
    VALUE_DATA_TYPE_RANK = 1
;

create or replace view CASE_FIELD_NAMES_TYPES_STG(
	CASE_TYPE,
	FIELD_NAME,
	FIELD_ALIAS,
    FIELD_COUNT,
	FIELD_DATA_TYPE
) as
SELECT 
  CASE_TYPE
  ,FIELD_NAME
  ,REGEXP_REPLACE(
  REGEXP_REPLACE(
    IFNULL(
      REGEXP_SUBSTR(FIELD_NAME, '(\\.|\\[\')([^\']*)(\'\\])?', 1, 1, 'c', 2), 
      FIELD_NAME),
    '-|\\.|\\s','_'),
  '[^a-zA-Z0-9 _ \\$]','') FIELD_ALIAS
  ,FIELD_COUNT
  ,CASE WHEN DATA_TYPE_PERC >= 99 THEN VALUE_DATA_TYPE
        ELSE 'Varchar' END                              as Field_Data_Type
FROM (
    SELECT
      CASE_TYPE
      ,FIELD_NAME
      ,FIELD_COUNT
      ,VALUE_DATA_TYPE
      ,case when VALUE_DATA_TYPE = 'Varchar' then 1 
            when VALUE_DATA_TYPE = 'Number' then 2
            when VALUE_DATA_TYPE = 'Date' then 3
            when VALUE_DATA_TYPE = 'Datetime' then 4
            when VALUE_DATA_TYPE = 'Boolean' then 5
        else 6 end TYPE_SORT
      ,DATA_TYPE_PERC
      ,RANK() OVER (PARTITION BY CASE_TYPE,FIELD_NAME ORDER BY DATA_TYPE_PERC, TYPE_SORT DESC) as VALUE_DATA_TYPE_RANK
    FROM CASE_FIELD_VALUE_DATA_TYPES_STG
    )
WHERE
    VALUE_DATA_TYPE_RANK = 1
;

create or replace view FIXTURE_FIELD_NAMES_TYPES_ALL(
	FIXTURE_TYPE,
	FIELD_NAME,
	FIELD_ALIAS,
    FIELD_COUNT,
	FIELD_DATA_TYPE
) as
SELECT 
  FIXTURE_TYPE
  ,FIELD_NAME
  ,REGEXP_REPLACE(
  REGEXP_REPLACE(
    IFNULL(
      REGEXP_SUBSTR(FIELD_NAME, '(\\.|\\[\')([^\']*)(\'\\])?', 1, 1, 'c', 2), 
      FIELD_NAME),
    '-|\\.|\\s','_'),
  '[^a-zA-Z0-9 _ \\$]','') FIELD_ALIAS
  ,FIELD_COUNT
  ,CASE WHEN DATA_TYPE_PERC >= 99 THEN VALUE_DATA_TYPE
        ELSE 'Varchar' END                              as Field_Data_Type
FROM (
    SELECT
      FIXTURE_TYPE
      ,FIELD_NAME
      ,FIELD_COUNT
      ,VALUE_DATA_TYPE
      ,case when VALUE_DATA_TYPE = 'Varchar' then 1 
            when VALUE_DATA_TYPE = 'Number' then 2
            when VALUE_DATA_TYPE = 'Date' then 3
            when VALUE_DATA_TYPE = 'Datetime' then 4
            when VALUE_DATA_TYPE = 'Boolean' then 5
        else 6 end TYPE_SORT
      ,DATA_TYPE_PERC
      ,RANK() OVER (PARTITION BY FIXTURE_TYPE,FIELD_NAME ORDER BY DATA_TYPE_PERC, TYPE_SORT DESC) as VALUE_DATA_TYPE_RANK
    FROM FIXTURE_FIELD_VALUE_DATA_TYPES_ALL
    )
WHERE
    VALUE_DATA_TYPE_RANK = 1
;

create or replace view FIXTURE_FIELD_NAMES_TYPES_STG(
	FIXTURE_TYPE,
	FIELD_NAME,
	FIELD_ALIAS,
    FIELD_COUNT,
	FIELD_DATA_TYPE
) as
SELECT 
  FIXTURE_TYPE
  ,FIELD_NAME
  ,REGEXP_REPLACE(
  REGEXP_REPLACE(
    IFNULL(
      REGEXP_SUBSTR(FIELD_NAME, '(\\.|\\[\')([^\']*)(\'\\])?', 1, 1, 'c', 2), 
      FIELD_NAME),
    '-|\\.|\\s','_'),
  '[^a-zA-Z0-9 _ \\$]','') FIELD_ALIAS
  ,FIELD_COUNT
  ,CASE WHEN DATA_TYPE_PERC >= 99 THEN VALUE_DATA_TYPE
        ELSE 'Varchar' END                              as Field_Data_Type
FROM (
    SELECT
      FIXTURE_TYPE
      ,FIELD_NAME
      ,FIELD_COUNT
      ,VALUE_DATA_TYPE
      ,case when VALUE_DATA_TYPE = 'Varchar' then 1 
            when VALUE_DATA_TYPE = 'Number' then 2
            when VALUE_DATA_TYPE = 'Date' then 3
            when VALUE_DATA_TYPE = 'Datetime' then 4
            when VALUE_DATA_TYPE = 'Boolean' then 5
        else 6 end TYPE_SORT
      ,DATA_TYPE_PERC
      ,RANK() OVER (PARTITION BY FIXTURE_TYPE,FIELD_NAME ORDER BY DATA_TYPE_PERC, TYPE_SORT DESC) as VALUE_DATA_TYPE_RANK
    FROM FIXTURE_FIELD_VALUE_DATA_TYPES_STG
    )
WHERE
    VALUE_DATA_TYPE_RANK = 1
;

create or replace view LOCATION_FIELD_NAMES_TYPES_ALL(
	FIELD_NAME,
	FIELD_ALIAS,
    FIELD_COUNT,
	FIELD_DATA_TYPE
) as
SELECT 
  FIELD_NAME
  ,REGEXP_REPLACE(
  REGEXP_REPLACE(
    IFNULL(
      REGEXP_SUBSTR(FIELD_NAME, '(\\.|\\[\')([^\']*)(\'\\])?', 1, 1, 'c', 2), 
      FIELD_NAME),
    '-|\\.|\\s','_'),
  '[^a-zA-Z0-9 _ \\$]','') FIELD_ALIAS
  ,FIELD_COUNT
  ,CASE WHEN DATA_TYPE_PERC >= 99 THEN VALUE_DATA_TYPE
        ELSE 'Varchar' END                              as Field_Data_Type
FROM (
    SELECT
      FIELD_NAME
      ,FIELD_COUNT
      ,VALUE_DATA_TYPE
      ,case when VALUE_DATA_TYPE = 'Varchar' then 1 
            when VALUE_DATA_TYPE = 'Number' then 2
            when VALUE_DATA_TYPE = 'Date' then 3
            when VALUE_DATA_TYPE = 'Datetime' then 4
            when VALUE_DATA_TYPE = 'Boolean' then 5
        else 6 end TYPE_SORT
      ,DATA_TYPE_PERC
      ,RANK() OVER (PARTITION BY FIELD_NAME ORDER BY DATA_TYPE_PERC, TYPE_SORT DESC) as VALUE_DATA_TYPE_RANK
    FROM LOCATION_FIELD_VALUE_DATA_TYPES_ALL
    )
WHERE
    VALUE_DATA_TYPE_RANK = 1
;

create or replace view LOCATION_FIELD_NAMES_TYPES_STG(
	FIELD_NAME,
	FIELD_ALIAS,
    FIELD_COUNT,
	FIELD_DATA_TYPE
) as
SELECT 
  FIELD_NAME
  ,REGEXP_REPLACE(
  REGEXP_REPLACE(
    IFNULL(
      REGEXP_SUBSTR(FIELD_NAME, '(\\.|\\[\')([^\']*)(\'\\])?', 1, 1, 'c', 2), 
      FIELD_NAME),
    '-|\\.|\\s','_'),
  '[^a-zA-Z0-9 _ \\$]','') FIELD_ALIAS
  ,FIELD_COUNT
  ,CASE WHEN DATA_TYPE_PERC >= 99 THEN VALUE_DATA_TYPE
        ELSE 'Varchar' END                              as Field_Data_Type
FROM (
    SELECT
      FIELD_NAME
      ,FIELD_COUNT
      ,VALUE_DATA_TYPE
      ,case when VALUE_DATA_TYPE = 'Varchar' then 1 
            when VALUE_DATA_TYPE = 'Number' then 2
            when VALUE_DATA_TYPE = 'Date' then 3
            when VALUE_DATA_TYPE = 'Datetime' then 4
            when VALUE_DATA_TYPE = 'Boolean' then 5
        else 6 end TYPE_SORT
      ,DATA_TYPE_PERC
      ,RANK() OVER (PARTITION BY FIELD_NAME ORDER BY DATA_TYPE_PERC, TYPE_SORT DESC) as VALUE_DATA_TYPE_RANK
    FROM LOCATION_FIELD_VALUE_DATA_TYPES_STG
    )
WHERE
    VALUE_DATA_TYPE_RANK = 1
;

create or replace view WEB_USER_FIELD_NAMES_TYPES_ALL(
	FIELD_NAME,
	FIELD_ALIAS,
    FIELD_COUNT,
	FIELD_DATA_TYPE
) as
SELECT 
  FIELD_NAME
  ,REGEXP_REPLACE(
  REGEXP_REPLACE(
    IFNULL(
      REGEXP_SUBSTR(FIELD_NAME, '(\\.|\\[\')([^\']*)(\'\\])?', 1, 1, 'c', 2), 
      FIELD_NAME),
    '-|\\.|\\s','_'),
  '[^a-zA-Z0-9 _ \\$]','') FIELD_ALIAS
  ,FIELD_COUNT
  ,CASE WHEN DATA_TYPE_PERC >= 99 THEN VALUE_DATA_TYPE
        ELSE 'Varchar' END                              as Field_Data_Type
FROM (
    SELECT
      FIELD_NAME
      ,FIELD_COUNT
      ,VALUE_DATA_TYPE
      ,case when VALUE_DATA_TYPE = 'Varchar' then 1 
            when VALUE_DATA_TYPE = 'Number' then 2
            when VALUE_DATA_TYPE = 'Date' then 3
            when VALUE_DATA_TYPE = 'Datetime' then 4
            when VALUE_DATA_TYPE = 'Boolean' then 5
        else 6 end TYPE_SORT
      ,DATA_TYPE_PERC
      ,RANK() OVER (PARTITION BY FIELD_NAME ORDER BY DATA_TYPE_PERC, TYPE_SORT DESC) as VALUE_DATA_TYPE_RANK
    FROM WEB_USER_FIELD_VALUE_DATA_TYPES_ALL
    )
WHERE
    VALUE_DATA_TYPE_RANK = 1
;

create or replace view WEB_USER_FIELD_NAMES_TYPES_STG(
	FIELD_NAME,
	FIELD_ALIAS,
    FIELD_COUNT,
	FIELD_DATA_TYPE
) as
SELECT 
  FIELD_NAME
  ,REGEXP_REPLACE(
  REGEXP_REPLACE(
    IFNULL(
      REGEXP_SUBSTR(FIELD_NAME, '(\\.|\\[\')([^\']*)(\'\\])?', 1, 1, 'c', 2), 
      FIELD_NAME),
    '-|\\.|\\s','_'),
  '[^a-zA-Z0-9 _ \\$]','') FIELD_ALIAS
  ,FIELD_COUNT
  ,CASE WHEN DATA_TYPE_PERC >= 99 THEN VALUE_DATA_TYPE
        ELSE 'Varchar' END                              as Field_Data_Type
FROM (
    SELECT
      FIELD_NAME
      ,FIELD_COUNT
      ,VALUE_DATA_TYPE
      ,case when VALUE_DATA_TYPE = 'Varchar' then 1 
            when VALUE_DATA_TYPE = 'Number' then 2
            when VALUE_DATA_TYPE = 'Date' then 3
            when VALUE_DATA_TYPE = 'Datetime' then 4
            when VALUE_DATA_TYPE = 'Boolean' then 5
        else 6 end TYPE_SORT
      ,DATA_TYPE_PERC
      ,RANK() OVER (PARTITION BY FIELD_NAME ORDER BY DATA_TYPE_PERC, TYPE_SORT DESC) as VALUE_DATA_TYPE_RANK
    FROM WEB_USER_FIELD_VALUE_DATA_TYPES_STG
    )
WHERE
    VALUE_DATA_TYPE_RANK = 1
;

create or replace view ACTION_TIME_FIELD_NAMES_TYPES_ALL(
	FIELD_NAME,
	FIELD_ALIAS,
    FIELD_COUNT,
	FIELD_DATA_TYPE
) as
SELECT 
  FIELD_NAME
  ,REGEXP_REPLACE(
  REGEXP_REPLACE(
    IFNULL(
      REGEXP_SUBSTR(FIELD_NAME, '(\\.|\\[\')([^\']*)(\'\\])?', 1, 1, 'c', 2), 
      FIELD_NAME),
    '-|\\.|\\s','_'),
  '[^a-zA-Z0-9 _ \\$]','') FIELD_ALIAS
  ,FIELD_COUNT
  ,CASE WHEN DATA_TYPE_PERC >= 99 THEN VALUE_DATA_TYPE
        ELSE 'Varchar' END                              as Field_Data_Type
FROM (
    SELECT
      FIELD_NAME
      ,FIELD_COUNT
      ,VALUE_DATA_TYPE
      ,case when VALUE_DATA_TYPE = 'Varchar' then 1 
            when VALUE_DATA_TYPE = 'Number' then 2
            when VALUE_DATA_TYPE = 'Date' then 3
            when VALUE_DATA_TYPE = 'Datetime' then 4
            when VALUE_DATA_TYPE = 'Boolean' then 5
        else 6 end TYPE_SORT
      ,DATA_TYPE_PERC
      ,RANK() OVER (PARTITION BY FIELD_NAME ORDER BY DATA_TYPE_PERC, TYPE_SORT DESC) as VALUE_DATA_TYPE_RANK
    FROM ACTION_TIME_FIELD_VALUE_DATA_TYPES_ALL
    )
WHERE
    VALUE_DATA_TYPE_RANK = 1
;

create or replace view ACTION_TIME_FIELD_NAMES_TYPES_STG(
	FIELD_NAME,
	FIELD_ALIAS,
    FIELD_COUNT,
	FIELD_DATA_TYPE
) as
SELECT 
  FIELD_NAME
  ,REGEXP_REPLACE(
  REGEXP_REPLACE(
    IFNULL(
      REGEXP_SUBSTR(FIELD_NAME, '(\\.|\\[\')([^\']*)(\'\\])?', 1, 1, 'c', 2), 
      FIELD_NAME),
    '-|\\.|\\s','_'),
  '[^a-zA-Z0-9 _ \\$]','') FIELD_ALIAS
  ,FIELD_COUNT
  ,CASE WHEN DATA_TYPE_PERC >= 99 THEN VALUE_DATA_TYPE
        ELSE 'Varchar' END                              as Field_Data_Type
FROM (
    SELECT
      FIELD_NAME
      ,FIELD_COUNT
      ,VALUE_DATA_TYPE
      ,case when VALUE_DATA_TYPE = 'Varchar' then 1 
            when VALUE_DATA_TYPE = 'Number' then 2
            when VALUE_DATA_TYPE = 'Date' then 3
            when VALUE_DATA_TYPE = 'Datetime' then 4
            when VALUE_DATA_TYPE = 'Boolean' then 5
        else 6 end TYPE_SORT
      ,DATA_TYPE_PERC
      ,RANK() OVER (PARTITION BY FIELD_NAME ORDER BY DATA_TYPE_PERC, TYPE_SORT DESC) as VALUE_DATA_TYPE_RANK
    FROM ACTION_TIME_FIELD_VALUE_DATA_TYPES_STG
    )
WHERE
    VALUE_DATA_TYPE_RANK = 1
;

create or replace view CASE_FIELDS_CONFIG_ALL(
	CASE_TYPE,
	FIELD_NAME,
	FIELD_ALIAS,
	FIELD_DATA_TYPE,
	DATA_TYPE_OVERRIDE,
	FIELD_NAME_OVERRIDE,
	INCLUDE_IN_DIM,
	TAGS,
	INSERT_DATE,
	INSERT_BY,
	UPDATE_DATE,
	UPDATE_BY
) as
SELECT 
  CFNT.CASE_TYPE
  ,CFNT.FIELD_NAME
  ,CFNT.FIELD_ALIAS
  ,CFNT.FIELD_DATA_TYPE
  ,NULL::varchar            as Data_Type_Override
  ,NULL::varchar            as Field_Name_Override
  ,False::Boolean           as Include_In_Dim
  ,NULL::varchar			AS Tags
  ,CURRENT_TIMESTAMP        as Insert_Date
  ,CURRENT_USER::varchar    as Insert_By
  ,NULL::Datetime           as Update_Date
  ,NULL::varchar            as Update_By
FROM
    CASE_FIELD_NAMES_TYPES_ALL CFNT
    LEFT JOIN CONFIG_CASE_FIELDS CCF on CFNT.CASE_TYPE = CCF.CASE_TYPE and  CFNT.FIELD_NAME = CCF.FIELD_NAME
WHERE 
    NULLIF(CFNT.CASE_TYPE,'') is not null
    and CCF.FIELD_NAME is NULL
;

create or replace view CASE_FIELDS_CONFIG_STG(
	CASE_TYPE,
	FIELD_NAME,
	FIELD_ALIAS,
	FIELD_DATA_TYPE,
	DATA_TYPE_OVERRIDE,
	FIELD_NAME_OVERRIDE,
	INCLUDE_IN_DIM,
	TAGS,
	INSERT_DATE,
	INSERT_BY,
	UPDATE_DATE,
	UPDATE_BY
) as
SELECT 
  CFNT.CASE_TYPE
  ,CFNT.FIELD_NAME
  ,CFNT.FIELD_ALIAS
  ,CFNT.FIELD_DATA_TYPE
  ,NULL::varchar            as Data_Type_Override
  ,NULL::varchar            as Field_Name_Override
  ,False::Boolean           as Include_In_Dim
  ,NULL::varchar			AS Tags
  ,CURRENT_TIMESTAMP        as Insert_Date
  ,CURRENT_USER::varchar    as Insert_By
  ,NULL::Datetime           as Update_Date
  ,NULL::varchar            as Update_By
FROM
    CASE_FIELD_NAMES_TYPES_STG CFNT
    LEFT JOIN CONFIG_CASE_FIELDS CCF on CFNT.CASE_TYPE = CCF.CASE_TYPE and  CFNT.FIELD_NAME = CCF.FIELD_NAME
WHERE 
    NULLIF(CFNT.CASE_TYPE,'') is not null
    and CCF.FIELD_NAME is NULL
;

create or replace view FIXTURE_FIELDS_CONFIG_ALL(
	FIXTURE_TYPE,
	FIELD_NAME,
	FIELD_ALIAS,
	FIELD_DATA_TYPE,
	DATA_TYPE_OVERRIDE,
	FIELD_NAME_OVERRIDE,
	INCLUDE_IN_DIM,
	TAGS,
	INSERT_DATE,
	INSERT_BY,
	UPDATE_DATE,
	UPDATE_BY
) as
SELECT 
  CFNT.FIXTURE_TYPE
  ,CFNT.FIELD_NAME
  ,CFNT.FIELD_ALIAS
  ,CFNT.FIELD_DATA_TYPE
  ,NULL::varchar            as Data_Type_Override
  ,NULL::varchar            as Field_Name_Override
  ,False::Boolean           as Include_In_Dim
  ,NULL::varchar			AS Tags
  ,CURRENT_TIMESTAMP        as Insert_Date
  ,CURRENT_USER::varchar    as Insert_By
  ,NULL::Datetime           as Update_Date
  ,NULL::varchar            as Update_By
FROM
    FIXTURE_FIELD_NAMES_TYPES_ALL CFNT
    LEFT JOIN CONFIG_FIXTURE_FIELDS CCF on CFNT.FIXTURE_TYPE = CCF.FIXTURE_TYPE and  CFNT.FIELD_NAME = CCF.FIELD_NAME
WHERE 
    NULLIF(CFNT.FIXTURE_TYPE,'') is not null
    and CCF.FIELD_NAME is NULL
;

create or replace view FIXTURE_FIELDS_CONFIG_STG(
	FIXTURE_TYPE,
	FIELD_NAME,
	FIELD_ALIAS,
	FIELD_DATA_TYPE,
	DATA_TYPE_OVERRIDE,
	FIELD_NAME_OVERRIDE,
	INCLUDE_IN_DIM,
	TAGS,
	INSERT_DATE,
	INSERT_BY,
	UPDATE_DATE,
	UPDATE_BY
) as
SELECT 
  CFNT.FIXTURE_TYPE
  ,CFNT.FIELD_NAME
  ,CFNT.FIELD_ALIAS
  ,CFNT.FIELD_DATA_TYPE
  ,NULL::varchar            as Data_Type_Override
  ,NULL::varchar            as Field_Name_Override
  ,False::Boolean           as Include_In_Dim
  ,NULL::varchar			AS Tags
  ,CURRENT_TIMESTAMP        as Insert_Date
  ,CURRENT_USER::varchar    as Insert_By
  ,NULL::Datetime           as Update_Date
  ,NULL::varchar            as Update_By
FROM
    FIXTURE_FIELD_NAMES_TYPES_STG CFNT
    LEFT JOIN CONFIG_FIXTURE_FIELDS CCF on CFNT.FIXTURE_TYPE = CCF.FIXTURE_TYPE and  CFNT.FIELD_NAME = CCF.FIELD_NAME
WHERE 
    NULLIF(CFNT.FIXTURE_TYPE,'') is not null
    and CCF.FIELD_NAME is NULL
;

create or replace view LOCATION_FIELDS_CONFIG_ALL(
	FIELD_NAME,
	FIELD_ALIAS,
	FIELD_DATA_TYPE,
	DATA_TYPE_OVERRIDE,
	FIELD_NAME_OVERRIDE,
	INCLUDE_IN_DIM,
	TAGS,
	INSERT_DATE,
	INSERT_BY,
	UPDATE_DATE,
	UPDATE_BY
) as
SELECT 
  CFNT.FIELD_NAME
  ,CFNT.FIELD_ALIAS
  ,CFNT.FIELD_DATA_TYPE
  ,NULL::varchar            as Data_Type_Override
  ,NULL::varchar            as Field_Name_Override
  ,False::Boolean           as Include_In_Dim
  ,NULL::varchar			AS Tags
  ,CURRENT_TIMESTAMP        as Insert_Date
  ,CURRENT_USER::varchar    as Insert_By
  ,NULL::Datetime           as Update_Date
  ,NULL::varchar            as Update_By
FROM
    LOCATION_FIELD_NAMES_TYPES_ALL CFNT
    LEFT JOIN CONFIG_LOCATION_FIELDS CCF on CFNT.FIELD_NAME = CCF.FIELD_NAME
WHERE 
    CCF.FIELD_NAME is NULL
;

create or replace view LOCATION_FIELDS_CONFIG_STG(
	FIELD_NAME,
	FIELD_ALIAS,
	FIELD_DATA_TYPE,
	DATA_TYPE_OVERRIDE,
	FIELD_NAME_OVERRIDE,
	INCLUDE_IN_DIM,
	TAGS,
	INSERT_DATE,
	INSERT_BY,
	UPDATE_DATE,
	UPDATE_BY
) as
SELECT 
  CFNT.FIELD_NAME
  ,CFNT.FIELD_ALIAS
  ,CFNT.FIELD_DATA_TYPE
  ,NULL::varchar            as Data_Type_Override
  ,NULL::varchar            as Field_Name_Override
  ,False::Boolean           as Include_In_Dim
  ,NULL::varchar			AS Tags
  ,CURRENT_TIMESTAMP        as Insert_Date
  ,CURRENT_USER::varchar    as Insert_By
  ,NULL::Datetime           as Update_Date
  ,NULL::varchar            as Update_By
FROM
    LOCATION_FIELD_NAMES_TYPES_STG CFNT
    LEFT JOIN CONFIG_LOCATION_FIELDS CCF on CFNT.FIELD_NAME = CCF.FIELD_NAME
WHERE 
    CCF.FIELD_NAME is NULL
;

create or replace view WEB_USER_FIELDS_CONFIG_ALL(
	FIELD_NAME,
	FIELD_ALIAS,
	FIELD_DATA_TYPE,
	DATA_TYPE_OVERRIDE,
	FIELD_NAME_OVERRIDE,
	INCLUDE_IN_DIM,
	TAGS,
	INSERT_DATE,
	INSERT_BY,
	UPDATE_DATE,
	UPDATE_BY
) as
SELECT 
  CFNT.FIELD_NAME
  ,CFNT.FIELD_ALIAS
  ,CFNT.FIELD_DATA_TYPE
  ,NULL::varchar            as Data_Type_Override
  ,NULL::varchar            as Field_Name_Override
  ,False::Boolean           as Include_In_Dim
  ,NULL::varchar			AS Tags
  ,CURRENT_TIMESTAMP        as Insert_Date
  ,CURRENT_USER::varchar    as Insert_By
  ,NULL::Datetime           as Update_Date
  ,NULL::varchar            as Update_By
FROM
    WEB_USER_FIELD_NAMES_TYPES_ALL CFNT
    LEFT JOIN CONFIG_WEB_USER_FIELDS CCF on CFNT.FIELD_NAME = CCF.FIELD_NAME
WHERE 
    CCF.FIELD_NAME is NULL
;

create or replace view WEB_USER_FIELDS_CONFIG_STG(
	FIELD_NAME,
	FIELD_ALIAS,
	FIELD_DATA_TYPE,
	DATA_TYPE_OVERRIDE,
	FIELD_NAME_OVERRIDE,
	INCLUDE_IN_DIM,
	TAGS,
	INSERT_DATE,
	INSERT_BY,
	UPDATE_DATE,
	UPDATE_BY
) as
SELECT 
  CFNT.FIELD_NAME
  ,CFNT.FIELD_ALIAS
  ,CFNT.FIELD_DATA_TYPE
  ,NULL::varchar            as Data_Type_Override
  ,NULL::varchar            as Field_Name_Override
  ,False::Boolean           as Include_In_Dim
  ,NULL::varchar			AS Tags
  ,CURRENT_TIMESTAMP        as Insert_Date
  ,CURRENT_USER::varchar    as Insert_By
  ,NULL::Datetime           as Update_Date
  ,NULL::varchar            as Update_By
FROM
    WEB_USER_FIELD_NAMES_TYPES_STG CFNT
    LEFT JOIN CONFIG_WEB_USER_FIELDS CCF on CFNT.FIELD_NAME = CCF.FIELD_NAME
WHERE 
    CCF.FIELD_NAME is NULL
;

create or replace view ACTION_TIME_FIELDS_CONFIG_ALL(
	FIELD_NAME,
	FIELD_ALIAS,
	FIELD_DATA_TYPE,
	DATA_TYPE_OVERRIDE,
	FIELD_NAME_OVERRIDE,
	INCLUDE_IN_DIM,
	TAGS,
	INSERT_DATE,
	INSERT_BY,
	UPDATE_DATE,
	UPDATE_BY
) as
SELECT 
  CFNT.FIELD_NAME
  ,CFNT.FIELD_ALIAS
  ,CFNT.FIELD_DATA_TYPE
  ,NULL::varchar            as Data_Type_Override
  ,NULL::varchar            as Field_Name_Override
  ,False::Boolean           as Include_In_Dim
  ,NULL::varchar			AS Tags
  ,CURRENT_TIMESTAMP        as Insert_Date
  ,CURRENT_USER::varchar    as Insert_By
  ,NULL::Datetime           as Update_Date
  ,NULL::varchar            as Update_By
FROM
    ACTION_TIME_FIELD_NAMES_TYPES_ALL CFNT
    LEFT JOIN CONFIG_ACTION_TIME_FIELDS CCF on CFNT.FIELD_NAME = CCF.FIELD_NAME
WHERE 
    CCF.FIELD_NAME is NULL
;

create or replace view ACTION_TIME_FIELDS_CONFIG_STG(
	FIELD_NAME,
	FIELD_ALIAS,
	FIELD_DATA_TYPE,
	DATA_TYPE_OVERRIDE,
	FIELD_NAME_OVERRIDE,
	INCLUDE_IN_DIM,
	TAGS,
	INSERT_DATE,
	INSERT_BY,
	UPDATE_DATE,
	UPDATE_BY
) as
SELECT 
  CFNT.FIELD_NAME
  ,CFNT.FIELD_ALIAS
  ,CFNT.FIELD_DATA_TYPE
  ,NULL::varchar            as Data_Type_Override
  ,NULL::varchar            as Field_Name_Override
  ,False::Boolean           as Include_In_Dim
  ,NULL::varchar			AS Tags
  ,CURRENT_TIMESTAMP        as Insert_Date
  ,CURRENT_USER::varchar    as Insert_By
  ,NULL::Datetime           as Update_Date
  ,NULL::varchar            as Update_By
FROM
    ACTION_TIME_FIELD_NAMES_TYPES_STG CFNT
    LEFT JOIN CONFIG_ACTION_TIME_FIELDS CCF on CFNT.FIELD_NAME = CCF.FIELD_NAME
WHERE 
    CCF.FIELD_NAME is NULL
;

create or replace view FORM_FIELD_VALUES_ALL(
	FORM_ID,
	DOMAIN,
	FORM_NAME,
	FORM_XMLNS,
	SERVER_MODIFIED_ON,
	FIELD_NAME,
	FIELD_ALIAS,
	FIELD_PATH,
	KEY_NAME,
	FIELD_VALUE,
	BOOLEAN_VALUE,
	DATETIME_VALUE,
	DATE_VALUE,
	NUMBER_VALUE,
	STRING_VALUE
) as
   SELECT 
      JSON:id::string                                   as Form_Id
      ,Domain                                           as Domain
      ,JSON:form."@name"::varchar                       as form_Name
      ,JSON:form."@xmlns"::varchar                      as form_XMLNS
      ,JSON:server_modified_on::datetime                as Server_Modified_On
      ,cfv.FIELD_NAME                                   as FIELD_NAME
      ,cfv.FIELD_ALIAS                                  as FIELD_ALIAS
      ,f.path                                           as Field_Path
      ,f.key::varchar                                   as Key_Name
      ,f.value                                          as Field_Value
      ,try_cast(f.value::string as boolean)             as Boolean_Value
      ,try_cast(f.value::string as timestamp)           as Datetime_Value
      ,try_cast(f.value::string as date)                as Date_Value
      ,try_cast(f.value::string as number)              as Number_Value
      ,f.value::string                                  as String_Value
    FROM 
        SRC_FORMS_RAW fr
        ,lateral flatten(JSON, recursive=>true) f
        JOIN CONFIG_FORM_FIELDS cfv on f.path like cfv.FIELD_PATTERN1
    WHERE 
        typeof(f.value) <> 'OBJECT'
        and f.path not like 'xform_ids%'
        and cfv.INCLUDE_IN_FORM_VALUES = TRUE        
;

create or replace view FORM_FIELD_VALUES_STG(
	FORM_ID,
	DOMAIN,
	FORM_NAME,
	FORM_XMLNS,
	SERVER_MODIFIED_ON,
	FIELD_NAME,
	FIELD_ALIAS,
	FIELD_PATH,
	KEY_NAME,
	FIELD_VALUE,
	BOOLEAN_VALUE,
	DATETIME_VALUE,
	DATE_VALUE,
	NUMBER_VALUE,
	STRING_VALUE
) as
   SELECT 
      JSON:id::string                                   as Form_Id
      ,Domain                                           as Domain
      ,JSON:form."@name"::varchar                       as form_Name
      ,JSON:form."@xmlns"::varchar                      as form_XMLNS
      ,JSON:server_modified_on::datetime                as Server_Modified_On
      ,cfv.FIELD_NAME                                   as FIELD_NAME
      ,cfv.FIELD_ALIAS                                  as FIELD_ALIAS
      ,f.path                                           as Field_Path
      ,f.key::varchar                                   as Key_Name
      ,f.value                                          as Field_Value
      ,try_cast(f.value::string as boolean)             as Boolean_Value
      ,try_cast(f.value::string as timestamp)           as Datetime_Value
      ,try_cast(f.value::string as date)                as Date_Value
      ,try_cast(f.value::string as number)              as Number_Value
      ,f.value::string                                  as String_Value
    FROM 
        SRC_FORMS_RAW_STAGE fr
        ,lateral flatten(JSON, recursive=>true) f
        JOIN CONFIG_FORM_FIELDS cfv on f.path like cfv.FIELD_PATTERN1
    WHERE 
        typeof(f.value) <> 'OBJECT'
        and f.path not like 'xform_ids%'
        and cfv.INCLUDE_IN_FORM_VALUES = TRUE
;
*/

//// setup UTIL schema ////

use role sysadmin;
use schema util;

create or replace TABLE SQL_JOB (
	JOB_ID integer autoincrement order not null constraint uniq_id unique enforced,
	JOB_NAME STRING,
	JOB_DESC STRING,
    constraint sql_job_pk primary key (job_id) enforced
);

create or replace TABLE SQL_JOB_STEP (
	JOB_STEP_ID integer autoincrement order not null constraint uniq_id unique enforced,
    JOB_ID INTEGER,
	STEP_ORDER INTEGER,
	STEP_SQL STRING,
    constraint sql_job_step_pk primary key (job_step_id) enforced,
    constraint sql_job_fk foreign key (job_id) references sql_job (job_id) not enforced
);

-- *********** NEW LOG TABLES
create or replace TABLE task_log (
	TASK_ID NUMBER(38,0) NOT NULL autoincrement order,
	UUID STRING,
	TYPE STRING,
	SUBTYPE STRING,
	DOMAIN STRING,
	STATUS STRING,
	TASK_START TIMESTAMP_NTZ,
	TASK_END TIMESTAMP_NTZ,
	constraint TASK_PK primary key (TASK_ID)
);

create or replace TABLE execution_log (
	EXECUTION_ID NUMBER(38,0) NOT NULL autoincrement order,
    TASK_ID NUMBER(38,0),
	UUID STRING,
	TYPE STRING,
	SUBTYPE STRING,
	DOMAIN STRING,
	STATUS STRING,
	EXECUTION_START TIMESTAMP_NTZ,
	EXECUTION_END TIMESTAMP_NTZ,
	constraint EXECUTION_PK primary key (EXECUTION_ID)
);

create or replace TABLE message_log (
	MESSAGE_ID NUMBER(38,0) NOT NULL autoincrement order,
    TASK_ID NUMBER(38,0),
    EXECUTION_ID NUMBER(38,0),
	TYPE STRING,
	SUBTYPE STRING,
	MESSAGE VARIANT,
	MESSAGE_TIME TIMESTAMP_NTZ DEFAULT SYSDATE(),
	constraint MESSAGE_PK primary key (MESSAGE_ID)
);
-- ************* END NEW TABLES

--changed ******** will need to decide what to do with existing logs
create or replace TABLE SQL_LOGS (
	SQL_LOG_ID NUMBER(38,0) NOT NULL autoincrement order,
	TASK_ID NUMBER(38,0), -- change
    EXECUTION_ID NUMBER(38,0), -- change
	QUERY_ID VARCHAR(16777216),
	QUERY_TEXT VARCHAR(16777216),
	DATABASE_NAME VARCHAR(16777216),
	SCHEMA_NAME VARCHAR(16777216),
	QUERY_TYPE VARCHAR(16777216),
	SESSION_ID NUMBER(38,0),
	USER_NAME VARCHAR(16777216),
	ROLE_NAME VARCHAR(16777216),
	WAREHOUSE_NAME VARCHAR(16777216),
	WAREHOUSE_SIZE VARCHAR(16777216),
	WAREHOUSE_TYPE VARCHAR(16777216),
	CLUSTER_NUMBER NUMBER(38,0),
	QUERY_TAG VARCHAR(16777216),
	EXECUTION_STATUS VARCHAR(16777216),
	ERROR_CODE NUMBER(38,0),
	ERROR_MESSAGE VARCHAR(16777216),
	START_TIME TIMESTAMP_LTZ(3),
	END_TIME TIMESTAMP_LTZ(3),
	TOTAL_ELAPSED_TIME NUMBER(38,0),
	BYTES_SCANNED NUMBER(38,0),
	ROWS_PRODUCED NUMBER(38,0),
	COMPILATION_TIME NUMBER(38,0),
	EXECUTION_TIME NUMBER(38,0),
	QUEUED_PROVISIONING_TIME NUMBER(38,0),
	QUEUED_REPAIR_TIME NUMBER(38,0),
	QUEUED_OVERLOAD_TIME NUMBER(38,0),
	TRANSACTION_BLOCKED_TIME NUMBER(38,0),
	OUTBOUND_DATA_TRANSFER_CLOUD VARCHAR(16777216),
	OUTBOUND_DATA_TRANSFER_REGION VARCHAR(16777216),
	OUTBOUND_DATA_TRANSFER_BYTES NUMBER(38,0),
	INBOUND_DATA_TRANSFER_CLOUD VARCHAR(16777216),
	INBOUND_DATA_TRANSFER_REGION VARCHAR(16777216),
	INBOUND_DATA_TRANSFER_BYTES NUMBER(38,0),
	CREDITS_USED_CLOUD_SERVICES NUMBER(38,9),
	LIST_EXTERNAL_FILE_TIME NUMBER(38,0),
	RELEASE_VERSION VARCHAR(16777216),
	EXTERNAL_FUNCTION_TOTAL_INVOCATIONS NUMBER(38,0),
	EXTERNAL_FUNCTION_TOTAL_SENT_ROWS NUMBER(38,0),
	EXTERNAL_FUNCTION_TOTAL_RECEIVED_ROWS NUMBER(38,0),
	EXTERNAL_FUNCTION_TOTAL_SENT_BYTES NUMBER(38,0),
	EXTERNAL_FUNCTION_TOTAL_RECEIVED_BYTES NUMBER(38,0),
	IS_CLIENT_GENERATED_STATEMENT BOOLEAN,
	QUERY_HASH VARCHAR(16777216),
	QUERY_HASH_VERSION NUMBER(38,0),
	QUERY_PARAMETERIZED_HASH VARCHAR(16777216),
	QUERY_PARAMETERIZED_HASH_VERSION NUMBER(38,0),
	constraint SQL_LOG_PK primary key (SQL_LOG_ID)
	//constraint TALEND_RUN_FK foreign key (RUN_ID) references TALEND_RUNS(RUN_ID)
);

////// grant usage with sysadmin //////

use role sysadmin;

--grant usage on database DM_ETL_REPLACE_TEST_BHA_DEV to role user_dimagi;
//grant usage on database DM_ETL_REPLACE_TEST_BHA_DEV to role DW_UTIL_RW;
//grant usage on database DM_ETL_REPLACE_TEST_BHA_DEV to role user_etl;
--grant usage on schema DM_ETL_REPLACE_TEST_BHA_DEV.DL to role user_dimagi;
--grant usage on schema DM_ETL_REPLACE_TEST_BHA_DEV.DM to role user_dimagi;
--grant usage on schema DM_ETL_REPLACE_TEST_BHA_DEV.INTEGRATION to role user_dimagi;
--grant usage on schema DM_ETL_REPLACE_TEST_BHA_DEV.UTIL to role user_dimagi;
//grant usage on schema DM_ETL_REPLACE_TEST_BHA_DEV.DL to role user_etl;
//grant usage on schema DM_ETL_REPLACE_TEST_BHA_DEV.DM to role user_etl;
//grant usage on schema DM_ETL_REPLACE_TEST_BHA_DEV.INTEGRATION to role user_etl;
//grant usage on schema DM_ETL_REPLACE_TEST_BHA_DEV.UTIL to role user_etl;
//grant usage on schema DM_ETL_REPLACE_TEST_BHA_DEV.UTIL to role DW_UTIL_RW;
//grant usage on schema DM_ETL_REPLACE_TEST_BHA_DEV.INTEGRATION to role DW_UTIL_RW;

--grant select on all tables in schema DM_ETL_REPLACE_TEST_BHA_DEV.DL to role user_dimagi;
--grant select on all tables in schema DM_ETL_REPLACE_TEST_BHA_DEV.DM to role user_dimagi;
--grant select on all tables in schema DM_ETL_REPLACE_TEST_BHA_DEV.INTEGRATION to role user_dimagi;
--grant select on all tables in schema DM_ETL_REPLACE_TEST_BHA_DEV.UTIL to role user_dimagi;
//grant select on all tables in schema DM_ETL_REPLACE_TEST_BHA_DEV.DL to role user_etl;
//grant select on all tables in schema DM_ETL_REPLACE_TEST_BHA_DEV.DM to role user_etl;
//grant select on all tables in schema DM_ETL_REPLACE_TEST_BHA_DEV.INTEGRATION to role user_etl;
//grant select on all tables in schema DM_ETL_REPLACE_TEST_BHA_DEV.UTIL to role user_etl;
//grant DELETE, INSERT, SELECT, TRUNCATE, UPDATE on all tables in schema DM_ETL_REPLACE_TEST_BHA_DEV.UTIL to role DW_UTIL_RW;
//grant DELETE, INSERT, SELECT, TRUNCATE, UPDATE on all tables in schema DM_ETL_REPLACE_TEST_BHA_DEV.INTEGRATION to role DW_UTIL_RW;

--grant select on all views in schema DM_ETL_REPLACE_TEST_BHA_DEV.DL to role user_dimagi;
--grant select on all views in schema DM_ETL_REPLACE_TEST_BHA_DEV.DM to role user_dimagi;
--grant select on all views in schema DM_ETL_REPLACE_TEST_BHA_DEV.INTEGRATION to role user_dimagi;
--grant select on all views in schema DM_ETL_REPLACE_TEST_BHA_DEV.UTIL to role user_dimagi;
//grant select on all views in schema DM_ETL_REPLACE_TEST_BHA_DEV.DL to role user_etl;
//grant select on all views in schema DM_ETL_REPLACE_TEST_BHA_DEV.DM to role user_etl;
//grant select on all views in schema DM_ETL_REPLACE_TEST_BHA_DEV.INTEGRATION to role user_etl;
//grant select on all views in schema DM_ETL_REPLACE_TEST_BHA_DEV.UTIL to role user_etl;
//grant DELETE, INSERT, SELECT, TRUNCATE, UPDATE on all views in schema DM_ETL_REPLACE_TEST_BHA_DEV.UTIL to role DW_UTIL_RW;
//grant DELETE, INSERT, SELECT, TRUNCATE, UPDATE on all views in schema DM_ETL_REPLACE_TEST_BHA_DEV.INTEGRATION to role DW_UTIL_RW;

//grant usage on all procedures in schema DM_ETL_REPLACE_TEST_BHA_DEV.UTIL to role user_etl;

////// grant usage with accountadmin role //////

use role accountadmin;

--grant select on future tables in schema DM_ETL_REPLACE_TEST_BHA_DEV.DL to role user_dimagi;
--grant select on future tables in schema DM_ETL_REPLACE_TEST_BHA_DEV.DM to role user_dimagi;
--grant select on future tables in schema DM_ETL_REPLACE_TEST_BHA_DEV.INTEGRATION to role user_dimagi;
--grant select on future tables in schema DM_ETL_REPLACE_TEST_BHA_DEV.UTIL to role user_dimagi;
//grant select on future tables in schema DM_ETL_REPLACE_TEST_BHA_DEV.DL to role user_etl;
//grant select on future tables in schema DM_ETL_REPLACE_TEST_BHA_DEV.DM to role user_etl;
//grant select on future tables in schema DM_ETL_REPLACE_TEST_BHA_DEV.INTEGRATION to role user_etl;
//grant select on future tables in schema DM_ETL_REPLACE_TEST_BHA_DEV.UTIL to role user_etl;
//grant DELETE, INSERT, SELECT, TRUNCATE, UPDATE on future tables in schema DM_ETL_REPLACE_TEST_BHA_DEV.UTIL to role DW_UTIL_RW;
//grant DELETE, INSERT, SELECT, TRUNCATE, UPDATE on future tables in schema DM_ETL_REPLACE_TEST_BHA_DEV.INTEGRATION to role DW_UTIL_RW;

--grant select on future views in schema DM_ETL_REPLACE_TEST_BHA_DEV.DL to role user_dimagi;
--grant select on future views in schema DM_ETL_REPLACE_TEST_BHA_DEV.DM to role user_dimagi;
--grant select on future views in schema DM_ETL_REPLACE_TEST_BHA_DEV.INTEGRATION to role user_dimagi;
--grant select on future views in schema DM_ETL_REPLACE_TEST_BHA_DEV.UTIL to role user_dimagi;
//grant select on future views in schema DM_ETL_REPLACE_TEST_BHA_DEV.DL to role user_etl;
//grant select on future views in schema DM_ETL_REPLACE_TEST_BHA_DEV.DM to role user_etl;
//grant select on future views in schema DM_ETL_REPLACE_TEST_BHA_DEV.INTEGRATION to role user_etl;
//grant select on future views in schema DM_ETL_REPLACE_TEST_BHA_DEV.UTIL to role user_etl;
//grant DELETE, INSERT, SELECT, TRUNCATE, UPDATE on future views in schema DM_ETL_REPLACE_TEST_BHA_DEV.UTIL to role DW_UTIL_RW;
//grant DELETE, INSERT, SELECT, TRUNCATE, UPDATE on future views in schema DM_ETL_REPLACE_TEST_BHA_DEV.INTEGRATION to role DW_UTIL_RW;

//grant usage on future procedures in schema DM_ETL_REPLACE_TEST_BHA_DEV.UTIL to role user_etl;

//// setup INTEGRATION schema - more ////

use role sysadmin;
use schema INTEGRATION;

/* -- in dbt --

--edited to add replace string for DB that sp will dynamically sub in
create or replace view generate_case_views(case_type, vw_type, sql_text) as
select case_type, vw_type,
    'create or replace view <<dm_db>>.INTEGRATION.VW_CASE_' || replace(upper(case_type),'-','_') || '_' || vt.vw_type 
    || '(\n   DOMAIN\n   ,ID\n   ,LAST_UPDATED\n   ,' || 
    (case when vw_type = 'STG' then '' else 'DELETED_IN_COMMCARE\n   ,ARCHIVED_FORM_DATETME\n   ,ARCHIVED_FORM_ID\n   ,' end) ||
    listagg(ifnull(FIELD_NAME_OVERRIDE, FIELD_ALIAS), '\n   ,') || '\n) as \n' ||
    'select\n   DOMAIN\n   ,ID\n   ,SYSTEM_QUERY_TS::datetime LAST_UPDATED\n   ,' || 
    (case when vw_type = 'STG' then '' else 'DELETED_FLAG DELETED_IN_COMMCARE\n   ,DELETED_DATE::datetime ARCHIVED_FORM_DATETME\n   ,DELETED_FORMID ARCHIVED_FORM_ID\n   ,' end) 
    || listagg('nullif(JSON:' || field_name || '::string, \'\')::' || ifnull(data_type_override, field_data_type) || ' ' || ifnull(field_name_override, field_alias), '\n   ,') 
    || '\nfrom dl.src_cases_raw' || max(vt.src_type) || ' \nwhere JSON:properties.case_type::string = \'' || case_type || '\'' 
    || (case when vw_type = 'STG' then '' else ' and not deleted_flag' end) || ';' sql_text
from config_case_fields
full join (select $1 vw_type, $2 src_type from (values ('STG', '_STAGE'), ('ALL', ''))) vt
where include_in_dim
group by case_type, vw_type
;

--edited to add replace string for DB that sp will dynamically sub in
create or replace view generate_fixture_views(fixture_type, vw_type, sql_text) as
select fixture_type, vw_type,
    'create or replace view <<dm_db>>.INTEGRATION.VW_FIXTURE_' || replace(upper(fixture_type),'-','_') || '_' || vt.vw_type 
    || '(\n   DOMAIN\n   ,ID\n   ,LAST_UPDATED\n   ,' || 
    listagg(ifnull(FIELD_NAME_OVERRIDE, FIELD_ALIAS), '\n   ,') || '\n) as \n' ||
    'select\n   DOMAIN\n   ,ID\n   ,SYSTEM_QUERY_TS::datetime LAST_UPDATED\n   ,' || 
    listagg('nullif(JSON:' || field_name || '::string, \'\')::' || ifnull(data_type_override, field_data_type) || ' ' || ifnull(field_name_override, field_alias), '\n   ,') 
    || '\nfrom dl.src_fixtures_raw' || max(vt.src_type) || ' \nwhere JSON:fixture_type::string = \'' || fixture_type || '\'' 
    || (case when vw_type = 'STG' then '' else '' end) || ';' sql_text
from config_fixture_fields
full join (select $1 vw_type, $2 src_type from (values ('STG', '_STAGE'), ('ALL', ''))) vt
where include_in_dim
group by fixture_type, vw_type
;

--edited to add replace string for DB that sp will dynamically sub in
create or replace view generate_location_views(vw_type, sql_text) as
select vw_type,
    'create or replace view <<dm_db>>.INTEGRATION.VW_LOCATION_' || vt.vw_type 
    || '(\n   DOMAIN\n   ,ID\n   ,LAST_UPDATED\n   ,' || 
    listagg(ifnull(FIELD_NAME_OVERRIDE, FIELD_ALIAS), '\n   ,') || '\n) as \n' ||
    'select\n   DOMAIN\n   ,ID\n   ,SYSTEM_QUERY_TS::datetime LAST_UPDATED\n   ,' || 
    listagg('nullif(JSON:' || field_name || '::string, \'\')::' || ifnull(data_type_override, field_data_type) || ' ' || ifnull(field_name_override, field_alias), '\n   ,') 
    || '\nfrom dl.src_locations_raw' || max(vt.src_type)
    || (case when vw_type = 'STG' then '' else '' end) || ';' sql_text
from config_location_fields
full join (select $1 vw_type, $2 src_type from (values ('STG', '_STAGE'), ('ALL', ''))) vt
where include_in_dim
group by vw_type
;

--edited to add replace string for DB that sp will dynamically sub in
create or replace view generate_web_user_views(vw_type, sql_text) as
select vw_type,
    'create or replace view <<dm_db>>.INTEGRATION.VW_WEB_USER_' || vt.vw_type 
    || '(\n   DOMAIN\n   ,ID\n   ,LAST_UPDATED\n   ,' || 
    listagg(ifnull(FIELD_NAME_OVERRIDE, FIELD_ALIAS), '\n   ,') || '\n) as \n' ||
    'select\n   DOMAIN\n   ,ID\n   ,SYSTEM_QUERY_TS::datetime LAST_UPDATED\n   ,' || 
    listagg('nullif(JSON:' || field_name || '::string, \'\')::' || ifnull(data_type_override, field_data_type) || ' ' || ifnull(field_name_override, field_alias), '\n   ,') 
    || '\nfrom dl.src_web_users_raw' || max(vt.src_type)
    || (case when vw_type = 'STG' then '' else '' end) || ';' sql_text
from config_web_user_fields
full join (select $1 vw_type, $2 src_type from (values ('STG', '_STAGE'), ('ALL', ''))) vt
where include_in_dim
group by vw_type
;

--edited to add replace string for DB that sp will dynamically sub in
create or replace view generate_action_time_views(vw_type, sql_text) as
select vw_type,
    'create or replace view <<dm_db>>.INTEGRATION.VW_ACTION_TIME_' || vt.vw_type 
    || '(\n   DOMAIN\n   ,ID\n   ,LAST_UPDATED\n   ,' || 
    listagg(ifnull(FIELD_NAME_OVERRIDE, FIELD_ALIAS), '\n   ,') || '\n) as \n' ||
    'select\n   DOMAIN\n   ,ID\n   ,SYSTEM_QUERY_TS::datetime LAST_UPDATED\n   ,' || 
    listagg('nullif(JSON:' || field_name || '::string, \'\')::' || ifnull(data_type_override, field_data_type) || ' ' || ifnull(field_name_override, field_alias), '\n   ,') 
    || '\nfrom dl.src_action_times_raw' || max(vt.src_type)
    || (case when vw_type = 'STG' then '' else '' end) || ';' sql_text
from config_action_time_fields
full join (select $1 vw_type, $2 src_type from (values ('STG', '_STAGE'), ('ALL', ''))) vt
where include_in_dim
group by vw_type
;

create or replace view generate_all_views(source, source_type, vw_type, sql_text) as 
select 'case' source, case_type source_type, vw_type, sql_text from generate_case_views
union
select 'fixture' source, fixture_type source_type, vw_type, sql_text from generate_fixture_views
union
select 'location' source, null source_type, vw_type, sql_text from generate_location_views
union
select 'web-user' source, null source_type, vw_type, sql_text from generate_web_user_views
union
select 'action_times' source, null source_type, vw_type, sql_text from generate_web_user_views
;

--edited to add replace string for DB that sp will dynamically sub in
create or replace view generate_case_table_delete_updates(case_type, vw_type, sql_text) as
select case_type, vw_type,
    'DELETE FROM <<dm_db>>.DM.CASE_' || replace(upper(case_type),'-','_') || ' c ' || 
    'USING <<dm_db>>.INTEGRATION.SRC_UPDATE_CASE_DELETIONS' || max(vt.src_type) || ' f ' ||
    'WHERE c.ID = f.caseid and f.CASETYPE = \'' || case_type || '\';' sql_text
from config_case_fields
full join (select $1 vw_type, $2 src_type from (values ('STG', '_STAGE'), ('ALL', '_ALL'))) vt
where include_in_dim
group by case_type, vw_type
;

--edited to add replace string for DB that sp will dynamically sub in
create or replace view generate_case_tables(case_type, sql_text) as
select case_type, 
    'create or replace table <<dm_db>>.DM.CASE_' || replace(upper(case_type),'-','_') 
    || '(\n   DOMAIN\n   ,ID\n   ,LAST_UPDATED\n   ,' ||
    listagg(ifnull(FIELD_NAME_OVERRIDE, FIELD_ALIAS), '\n   ,') || '\n) as \n' ||
    'select\n   DOMAIN\n   ,ID\n   ,LAST_UPDATED\n   ,' 
    || listagg(ifnull(FIELD_NAME_OVERRIDE, FIELD_ALIAS), '\n   ,') || 
    '\nfrom <<dm_db>>.INTEGRATION.VW_CASE_' || replace(upper(case_type),'-','_') || '_ALL;' sql_text
from config_case_fields
where include_in_dim
group by case_type
;

--edited to add replace string for DB that sp will dynamically sub in
create or replace view generate_fixture_tables(fixture_type, sql_text) as
select fixture_type, 
    'create or replace table <<dm_db>>.DM.FIXTURE_' || replace(upper(fixture_type),'-','_') 
    || '(\n   DOMAIN\n   ,ID\n   ,LAST_UPDATED\n   ,' ||
    listagg(ifnull(FIELD_NAME_OVERRIDE, FIELD_ALIAS), '\n   ,') || '\n) as \n' ||
    'select\n   DOMAIN\n   ,ID\n   ,LAST_UPDATED\n   ,' 
    || listagg(ifnull(FIELD_NAME_OVERRIDE, FIELD_ALIAS), '\n   ,') || 
    '\nfrom <<dm_db>>.INTEGRATION.VW_FIXTURE_' || replace(upper(fixture_type),'-','_') || '_ALL;' sql_text
from config_fixture_fields
where include_in_dim
group by fixture_type
;

--edited to add replace string for DB that sp will dynamically sub in
create or replace view generate_location_tables(sql_text) as
select  
    'create or replace table <<dm_db>>.DM.LOCATION'
    || '(\n   DOMAIN\n   ,ID\n   ,LAST_UPDATED\n   ,' ||
    listagg(ifnull(FIELD_NAME_OVERRIDE, FIELD_ALIAS), '\n   ,') || '\n) as \n' ||
    'select\n   DOMAIN\n   ,ID\n   ,LAST_UPDATED\n   ,' 
    || listagg(ifnull(FIELD_NAME_OVERRIDE, FIELD_ALIAS), '\n   ,') || 
    '\nfrom <<dm_db>>.INTEGRATION.VW_LOCATION_ALL;' sql_text
from config_location_fields
where include_in_dim
;

--edited to add replace string for DB that sp will dynamically sub in
create or replace view generate_web_user_tables(sql_text) as
select  
    'create or replace table <<dm_db>>.DM.WEB_USER'
    || '(\n   DOMAIN\n   ,ID\n   ,LAST_UPDATED\n   ,' ||
    listagg(ifnull(FIELD_NAME_OVERRIDE, FIELD_ALIAS), '\n   ,') || '\n) as \n' ||
    'select\n   DOMAIN\n   ,ID\n   ,LAST_UPDATED\n   ,' 
    || listagg(ifnull(FIELD_NAME_OVERRIDE, FIELD_ALIAS), '\n   ,') || 
    '\nfrom <<dm_db>>.INTEGRATION.VW_WEB_USER_ALL;' sql_text
from config_web_user_fields
where include_in_dim
;

--edited to add replace string for DB that sp will dynamically sub in
create or replace view generate_action_time_tables(sql_text) as
select  
    'create or replace table <<dm_db>>.DM.ACTION_TIME'
    || '(\n   DOMAIN\n   ,ID\n   ,LAST_UPDATED\n   ,' ||
    listagg(ifnull(FIELD_NAME_OVERRIDE, FIELD_ALIAS), '\n   ,') || '\n) as \n' ||
    'select\n   DOMAIN\n   ,ID\n   ,LAST_UPDATED\n   ,' 
    || listagg(ifnull(FIELD_NAME_OVERRIDE, FIELD_ALIAS), '\n   ,') || 
    '\nfrom <<dm_db>>.INTEGRATION.VW_ACTION_TIME_ALL;' sql_text
from config_action_time_fields
where include_in_dim
;

--edited to add replace string for DB that sp will dynamically sub in
create or replace view generate_case_table_incr_load(case_type, sql_text) as
select case_type, 
    'merge into <<dm_db>>.DM.CASE_' || replace(upper(case_type),'-','_') || ' T using \n' ||
    '(select \n   DOMAIN\n   ,ID\n   ,LAST_UPDATED\n   ,' || 
    listagg(ifnull(FIELD_NAME_OVERRIDE, FIELD_ALIAS), '\n   ,') ||
    ' from <<dm_db>>.INTEGRATION.VW_CASE_' || replace(upper(case_type),'-','_') || '_STG) S \n' ||
    'on T.ID = S.ID when matched then update set \n   T.DOMAIN = S.DOMAIN\n   ,T.ID = S.ID\n   ,T.LAST_UPDATED = S.LAST_UPDATED\n   ,' || 
    listagg('T.' || ifnull(FIELD_NAME_OVERRIDE, FIELD_ALIAS) || '= S.' || ifnull(FIELD_NAME_OVERRIDE, FIELD_ALIAS), '\n   ,') || 
    '\nwhen not matched then insert(\n   DOMAIN\n   ,ID\n   ,LAST_UPDATED\n   ,' 
    || listagg(ifnull(FIELD_NAME_OVERRIDE, FIELD_ALIAS), '\n   ,') || ') \n' || 
    'values (\n   S.DOMAIN\n   ,S.ID\n   ,S.LAST_UPDATED\n   ,' 
    || listagg('S.' || ifnull(FIELD_NAME_OVERRIDE, FIELD_ALIAS), '\n   ,') || ');' sql_text
from config_case_fields
where include_in_dim
group by case_type
;

--edited to add replace string for DB that sp will dynamically sub in
create or replace view generate_fixture_table_incr_load(fixture_type, sql_text) as
select fixture_type, 
    'merge into <<dm_db>>.DM.FIXTURE_' || replace(upper(fixture_type),'-','_') || ' T using \n' ||
    '(select \n   DOMAIN\n   ,ID\n   ,LAST_UPDATED\n   ,' || 
    listagg(ifnull(FIELD_NAME_OVERRIDE, FIELD_ALIAS), '\n   ,') ||
    ' from <<dm_db>>.INTEGRATION.VW_FIXTURE_' || replace(upper(fixture_type),'-','_') || '_STG) S \n' ||
    'on T.ID = S.ID when matched then update set \n   T.DOMAIN = S.DOMAIN\n   ,T.ID = S.ID\n   ,T.LAST_UPDATED = S.LAST_UPDATED\n   ,' || 
    listagg('T.' || ifnull(FIELD_NAME_OVERRIDE, FIELD_ALIAS) || '= S.' || ifnull(FIELD_NAME_OVERRIDE, FIELD_ALIAS), '\n   ,') || 
    '\nwhen not matched then insert(\n   DOMAIN\n   ,ID\n   ,LAST_UPDATED\n   ,' 
    || listagg(ifnull(FIELD_NAME_OVERRIDE, FIELD_ALIAS), '\n   ,') || ') \n' || 
    'values (\n   S.DOMAIN\n   ,S.ID\n   ,S.LAST_UPDATED\n   ,' 
    || listagg('S.' || ifnull(FIELD_NAME_OVERRIDE, FIELD_ALIAS), '\n   ,') || ');' sql_text
from config_fixture_fields
where include_in_dim
group by fixture_type
;

--edited to add replace string for DB that sp will dynamically sub in
create or replace view generate_location_table_incr_load(sql_text) as
select 
    'merge into <<dm_db>>.DM.LOCATION T using \n' ||
    '(select \n   DOMAIN\n   ,ID\n   ,LAST_UPDATED\n   ,' || 
    listagg(ifnull(FIELD_NAME_OVERRIDE, FIELD_ALIAS), '\n   ,') ||
    ' from <<dm_db>>.INTEGRATION.VW_LOCATION_STG) S \n' ||
    'on T.ID = S.ID when matched then update set \n   T.DOMAIN = S.DOMAIN\n   ,T.ID = S.ID\n   ,T.LAST_UPDATED = S.LAST_UPDATED\n   ,' || 
    listagg('T.' || ifnull(FIELD_NAME_OVERRIDE, FIELD_ALIAS) || '= S.' || ifnull(FIELD_NAME_OVERRIDE, FIELD_ALIAS), '\n   ,') || 
    '\nwhen not matched then insert(\n   DOMAIN\n   ,ID\n   ,LAST_UPDATED\n   ,' 
    || listagg(ifnull(FIELD_NAME_OVERRIDE, FIELD_ALIAS), '\n   ,') || ') \n' || 
    'values (\n   S.DOMAIN\n   ,S.ID\n   ,S.LAST_UPDATED\n   ,' 
    || listagg('S.' || ifnull(FIELD_NAME_OVERRIDE, FIELD_ALIAS), '\n   ,') || ');' sql_text
from config_location_fields
where include_in_dim
;

--edited to add replace string for DB that sp will dynamically sub in
create or replace view generate_web_user_table_incr_load(sql_text) as
select 
    'merge into <<dm_db>>.DM.WEB_USER T using \n' ||
    '(select \n   DOMAIN\n   ,ID\n   ,LAST_UPDATED\n   ,' || 
    listagg(ifnull(FIELD_NAME_OVERRIDE, FIELD_ALIAS), '\n   ,') ||
    ' from <<dm_db>>.INTEGRATION.VW_WEB_USER_STG) S \n' ||
    'on T.ID = S.ID when matched then update set \n   T.DOMAIN = S.DOMAIN\n   ,T.ID = S.ID\n   ,T.LAST_UPDATED = S.LAST_UPDATED\n   ,' || 
    listagg('T.' || ifnull(FIELD_NAME_OVERRIDE, FIELD_ALIAS) || '= S.' || ifnull(FIELD_NAME_OVERRIDE, FIELD_ALIAS), '\n   ,') || 
    '\nwhen not matched then insert(\n   DOMAIN\n   ,ID\n   ,LAST_UPDATED\n   ,' 
    || listagg(ifnull(FIELD_NAME_OVERRIDE, FIELD_ALIAS), '\n   ,') || ') \n' || 
    'values (\n   S.DOMAIN\n   ,S.ID\n   ,S.LAST_UPDATED\n   ,' 
    || listagg('S.' || ifnull(FIELD_NAME_OVERRIDE, FIELD_ALIAS), '\n   ,') || ');' sql_text
from config_web_user_fields
where include_in_dim
;

--edited to add replace string for DB that sp will dynamically sub in
create or replace view generate_action_time_table_incr_load(sql_text) as
select 
    'merge into <<dm_db>>.DM.ACTION_TIME T using \n' ||
    '(select \n   DOMAIN\n   ,ID\n   ,LAST_UPDATED\n   ,' || 
    listagg(ifnull(FIELD_NAME_OVERRIDE, FIELD_ALIAS), '\n   ,') ||
    ' from <<dm_db>>.INTEGRATION.VW_ACTION_TIME_STG) S \n' ||
    'on T.ID = S.ID when matched then update set \n   T.DOMAIN = S.DOMAIN\n   ,T.ID = S.ID\n   ,T.LAST_UPDATED = S.LAST_UPDATED\n   ,' || 
    listagg('T.' || ifnull(FIELD_NAME_OVERRIDE, FIELD_ALIAS) || '= S.' || ifnull(FIELD_NAME_OVERRIDE, FIELD_ALIAS), '\n   ,') || 
    '\nwhen not matched then insert(\n   DOMAIN\n   ,ID\n   ,LAST_UPDATED\n   ,' 
    || listagg(ifnull(FIELD_NAME_OVERRIDE, FIELD_ALIAS), '\n   ,') || ') \n' || 
    'values (\n   S.DOMAIN\n   ,S.ID\n   ,S.LAST_UPDATED\n   ,' 
    || listagg('S.' || ifnull(FIELD_NAME_OVERRIDE, FIELD_ALIAS), '\n   ,') || ');' sql_text
from config_action_time_fields
where include_in_dim
;

-- edited to insert counts into new message log table
create or replace view count_dm_cases(source, source_type, sql_text) as 
select 'case' source, case_type source_type, 'insert into <<dm_db>>.util.message_log (TASK_ID, EXECUTION_ID, TYPE, SUBTYPE, MESSAGE) ' ||
    'select <<task_id>> task_id, <<exec_id>> execution_id, \'SQLMessage\' type, \'DM_CASE_COUNT\' subtype, ' ||
    'object_construct(\'domain\', domain, \'case_type\', \'' || case_type || '\', \'count\', cnt::string) message ' ||
    'from (select domain, count(*) cnt from <<dm_db>>.DM.CASE_' || replace(upper(case_type),'-','_') || ' group by 1) c;' sql_text
from config_case_fields group by case_type
;

-- edited to insert counts into new message log table
create or replace view count_dm_fixtures(source, source_type, sql_text) as 
select 'fixture' source, fixture_type source_type, 'insert into <<dm_db>>.util.message_log (TASK_ID, EXECUTION_ID, TYPE, SUBTYPE, MESSAGE) ' ||
    'select <<task_id>> task_id, <<exec_id>> execution_id, \'SQLMessage\' type, \'DM_FIXTURE_COUNT\' subtype, ' ||
    'object_construct(\'domain\', domain, \'fixture_type\', \'' || fixture_type || '\', \'count\', cnt::string) message ' ||
    'from (select domain, count(*) cnt from <<dm_db>>.DM.FIXTURE_' || replace(upper(fixture_type),'-','_') || ' group by 1) c;' sql_text
from config_fixture_fields group by fixture_type
;

-- edited to insert counts into new message log table
create or replace view count_dm_locations(source, source_type, sql_text) as 
select 'location' source, null::string source_type, 'insert into <<dm_db>>.util.message_log (TASK_ID, EXECUTION_ID, TYPE, SUBTYPE, MESSAGE) ' ||
    'select <<task_id>> task_id, <<exec_id>> execution_id, \'SQLMessage\' type, \'DM_LOCATION_COUNT\' subtype, ' ||
    'object_construct(\'domain\', domain, \'count\', cnt::string) message ' ||
    'from (select domain, count(*) cnt from <<dm_db>>.DM.LOCATION group by 1) c;' sql_text
;

-- edited to insert counts into new message log table
create or replace view count_dm_web_users(source, source_type, sql_text) as 
select 'web-user' source, null::string source_type, 'insert into <<dm_db>>.util.message_log (TASK_ID, EXECUTION_ID, TYPE, SUBTYPE, MESSAGE) ' ||
    'select <<task_id>> task_id, <<exec_id>> execution_id, \'SQLMessage\' type, \'DM_WEB_USER_COUNT\' subtype, ' ||
    'object_construct(\'domain\', domain, \'count\', cnt::string) message ' ||
    'from (select domain, count(*) cnt from <<dm_db>>.DM.WEB_USER group by 1) c;' sql_text
;

-- edited to insert counts into new message log table
create or replace view count_dm_action_times(source, source_type, sql_text) as 
select 'action_times' source, null::string source_type, 'insert into <<dm_db>>.util.message_log (TASK_ID, EXECUTION_ID, TYPE, SUBTYPE, MESSAGE) ' ||
    'select <<task_id>> task_id, <<exec_id>> execution_id, \'SQLMessage\' type, \'DM_ACTION_TIME_COUNT\' subtype, ' ||
    'object_construct(\'domain\', domain, \'count\', cnt::string) message ' ||
    'from (select domain, count(*) cnt from <<dm_db>>.DM.ACTION_TIME group by 1) c;' sql_text
;

--needed to add action times to this
create or replace view generate_all_tables(source, source_type, sql_text) as 
with qrys as (
    select 1 as ord, 'case' source, case_type source_type, sql_text from generate_case_tables
    union
    select 2 as ord, 'fixture' source, fixture_type source_type, sql_text from generate_fixture_tables
    union
    select 3 as ord, 'location' source, null source_type, sql_text from generate_location_tables
    union
    select 4 as ord, 'web-user' source, null source_type, sql_text from generate_web_user_tables
    union
    select 5 as ord, 'action_times' source, null source_type, sql_text from generate_action_time_tables
    union 
    select 6 as ord, 'case' source, case_type source_type, sql_text from generate_case_table_delete_updates where vw_type = 'ALL'
    union
    select 7 as ord, source, source_type, sql_text from count_dm_cases
    union
    select 8 as ord, source, source_type, sql_text from count_dm_fixtures
    union
    select 9 as ord, source, source_type, sql_text from count_dm_locations
    union
    select 10 as ord, source, source_type, sql_text from count_dm_web_users
    union
    select 11 as ord, source, source_type, sql_text from count_dm_action_times
)
select source, source_type, sql_text from qrys order by ord asc
;

create or replace view generate_all_tables_incr_load(source, source_type, sql_text) as 
with qrys as (
    select 1 as ord, 'case' source, case_type source_type, sql_text from generate_case_table_incr_load
    union
    select 2 as ord, 'fixture' source, fixture_type source_type, sql_text from generate_fixture_table_incr_load
    union
    --below will recreate location table entirely from location_raw, speial need for bha location redesign projet
    select 3 as ord, 'location' source, null source_type, sql_text from generate_location_tables
    --below will do merge changes from location_raw
    --select 3 as ord, 'location' source, null source_type, sql_text from generate_location_table_incr_load --previous incr view
    union
    select 4 as ord, 'web-user' source, null source_type, sql_text from generate_web_user_table_incr_load
    union
    select 5 as ord, 'action_times' source, null source_type, sql_text from generate_action_time_table_incr_load
    union 
    select 6 as ord, 'case' source, case_type source_type, sql_text from generate_case_table_delete_updates where vw_type = 'STG'
    union
    select 7 as ord, source, source_type, sql_text from count_dm_cases
    union
    select 8 as ord, source, source_type, sql_text from count_dm_fixtures
    union
    select 9 as ord, source, source_type, sql_text from count_dm_locations
    union
    select 10 as ord, source, source_type, sql_text from count_dm_web_users
    union
    select 11 as ord, source, source_type, sql_text from count_dm_action_times
)
select source, source_type, sql_text from qrys order by ord asc
;
*/

//// setup sql_job table in UTIL schema ////

-- ************ ALL EXISTING STATEMENTS HAVE EDITS, WE WILL WANT TO DELETE FROM SQL_JOB_STEPS AND RELOAD

use role sysadmin;
use schema util;

insert into sql_job(JOB_NAME, JOB_DESC) values('Raw Data Load', 'Loads all successful records from raw stage to raw and load new fields meta to config');

set raw_jobid = (select job_id from sql_job where job_name='Raw Data Load');

-- !!!!!!!!!!!!!!!! REMOVE BELOW SECTION FOR CICT PROJECTS !!!!!!!!!!!!!!!!
insert into sql_job_step(JOB_ID, STEP_ORDER, STEP_SQL) values($raw_jobid, 100, 'begin transaction;');
--insert into sql_job_step(JOB_ID, STEP_ORDER, STEP_SQL) values($raw_jobid, 200, 'TRUNCATE TABLE <<dl_db>>.<<dl_schema>>.fixtures_raw;');
insert into sql_job_step(JOB_ID, STEP_ORDER, STEP_SQL) values($raw_jobid, 300, 'MERGE INTO <<dl_db>>.<<dl_schema>>.fixtures_raw T USING ' ||
                                                              '(select * from <<dl_db>>.<<dl_schema>>.fixtures_raw_stage) AS S ' ||
                                                              'ON T.ID = S.ID WHEN MATCHED THEN UPDATE SET T.DOMAIN=S.DOMAIN, T.JSON=S.JSON, T.ID=S.ID, T.SYSTEM_QUERY_TS = S.SYSTEM_QUERY_TS, T.TASK_ID = S.TASK_ID, T.EXECUTION_ID = S.EXECUTION_ID, T.METADATA = S.METADATA, T.METADATA_FILENAME = S.METADATA_FILENAME ' ||
                                                              'WHEN NOT MATCHED THEN INSERT(DOMAIN, JSON, ID, SYSTEM_QUERY_TS, TASK_ID, EXECUTION_ID, METADATA, METADATA_FILENAME) VALUES(S.DOMAIN, S.JSON, S.ID, S.SYSTEM_QUERY_TS, S.TASK_ID, S.EXECUTION_ID, S.METADATA, S.METADATA_FILENAME);');
insert into sql_job_step(JOB_ID, STEP_ORDER, STEP_SQL) values($raw_jobid, 400, 'commit;');
-- !!!!!!!!!!!!!!!! REMOVE ABOVE SECTION FOR CICT PROJECTS !!!!!!!!!!!!!!!! 

insert into sql_job_step(JOB_ID, STEP_ORDER, STEP_SQL) values($raw_jobid, 450, 
        'insert into <<dm_db>>.util.message_log (TASK_ID, EXECUTION_ID, TYPE, SUBTYPE, MESSAGE) ' ||
        'select <<task_id>> task_id, <<exec_id>> execution_id, \'SQLMessage\' type, \'DL_FIXTURE_COUNT\' subtype, ' ||
        'object_construct(\'domain\', domain, \'fixture_type\', fixture_type, \'count\', cnt::string) message ' ||
        'from (select domain, JSON:fixture_type::string fixture_type, count(*) cnt ' || 
        'from <<dl_db>>.<<dl_schema>>.FIXTURES_RAW group by 1,2);');
insert into sql_job_step(JOB_ID, STEP_ORDER, STEP_SQL) values($raw_jobid, 500, 'begin transaction;');
insert into sql_job_step(JOB_ID, STEP_ORDER, STEP_SQL) values($raw_jobid, 600, 'MERGE INTO <<dl_db>>.<<dl_schema>>.cases_raw T USING ' || 
                                                              '(select * from <<dl_db>>.<<dl_schema>>.cases_raw_stage) AS S ' ||
                                                              'ON T.ID = S.ID WHEN MATCHED THEN UPDATE SET T.DOMAIN=S.DOMAIN, T.JSON=S.JSON, T.ID=S.ID, T.SYSTEM_QUERY_TS = S.SYSTEM_QUERY_TS, T.TASK_ID = S.TASK_ID, T.EXECUTION_ID = S.EXECUTION_ID, T.METADATA = S.METADATA, T.METADATA_FILENAME = S.METADATA_FILENAME ' ||
                                                              'WHEN NOT MATCHED THEN INSERT(DOMAIN, JSON, ID, SYSTEM_QUERY_TS, TASK_ID, EXECUTION_ID, METADATA, METADATA_FILENAME) VALUES(S.DOMAIN, S.JSON, S.ID, S.SYSTEM_QUERY_TS, S.TASK_ID, S.EXECUTION_ID, S.METADATA, S.METADATA_FILENAME);');
insert into sql_job_step(JOB_ID, STEP_ORDER, STEP_SQL) values($raw_jobid, 700, 'commit;');
insert into sql_job_step(JOB_ID, STEP_ORDER, STEP_SQL) values($raw_jobid, 800, 'begin transaction;');
insert into sql_job_step(JOB_ID, STEP_ORDER, STEP_SQL) values($raw_jobid, 900, 'MERGE INTO <<dl_db>>.<<dl_schema>>.forms_raw T USING ' || 
                                                              '(select * from <<dl_db>>.<<dl_schema>>.forms_raw_stage) AS S ' ||
                                                              'ON T.ID = S.ID WHEN MATCHED THEN UPDATE SET T.DOMAIN=S.DOMAIN, T.JSON=S.JSON, T.ID=S.ID, T.SYSTEM_QUERY_TS = S.SYSTEM_QUERY_TS, T.TASK_ID = S.TASK_ID, T.EXECUTION_ID = S.EXECUTION_ID, T.METADATA = S.METADATA, T.METADATA_FILENAME = S.METADATA_FILENAME ' ||
                                                              'WHEN NOT MATCHED THEN INSERT(DOMAIN, JSON, ID, SYSTEM_QUERY_TS, TASK_ID, EXECUTION_ID, METADATA, METADATA_FILENAME) VALUES(S.DOMAIN, S.JSON, S.ID, S.SYSTEM_QUERY_TS, S.TASK_ID, S.EXECUTION_ID, S.METADATA, S.METADATA_FILENAME);');
insert into sql_job_step(JOB_ID, STEP_ORDER, STEP_SQL) values($raw_jobid, 1000, 'commit;');
insert into sql_job_step(JOB_ID, STEP_ORDER, STEP_SQL) values($raw_jobid, 1050, 
        'insert into <<dm_db>>.util.message_log (TASK_ID, EXECUTION_ID, TYPE, SUBTYPE, MESSAGE) ' ||
        'select <<task_id>> task_id, <<exec_id>> execution_id, \'SQLMessage\' type, \'DL_FORM_COUNT\' subtype, ' ||
        'object_construct(\'domain\', domain, \'count\', cnt::string) message ' ||
        'from (select domain, count(*) cnt ' || 
        'from <<dl_db>>.<<dl_schema>>.FORMS_RAW group by 1);');

insert into sql_job_step(JOB_ID, STEP_ORDER, STEP_SQL) values($raw_jobid, 1100, 'begin transaction;');
--insert into sql_job_step(JOB_ID, STEP_ORDER, STEP_SQL) values($raw_jobid, 1150, 'TRUNCATE TABLE <<dl_db>>.<<dl_schema>>.locations_raw;');
insert into sql_job_step(JOB_ID, STEP_ORDER, STEP_SQL) values($raw_jobid, 1200, 'MERGE INTO <<dl_db>>.<<dl_schema>>.locations_raw T USING ' || 
                                                              '(select * from <<dl_db>>.<<dl_schema>>.locations_raw_stage) AS S ' ||
                                                              'ON T.ID = S.ID WHEN MATCHED THEN UPDATE SET T.DOMAIN=S.DOMAIN, T.JSON=S.JSON, T.ID=S.ID, T.SYSTEM_QUERY_TS = S.SYSTEM_QUERY_TS, T.TASK_ID = S.TASK_ID, T.EXECUTION_ID = S.EXECUTION_ID, T.METADATA = S.METADATA, T.METADATA_FILENAME = S.METADATA_FILENAME ' ||
                                                              'WHEN NOT MATCHED THEN INSERT(DOMAIN, JSON, ID, SYSTEM_QUERY_TS, TASK_ID, EXECUTION_ID, METADATA, METADATA_FILENAME) VALUES(S.DOMAIN, S.JSON, S.ID, S.SYSTEM_QUERY_TS, S.TASK_ID, S.EXECUTION_ID, S.METADATA, S.METADATA_FILENAME);');
// the following step 1250 is only needed if choose to do location recreation from view GENERATE_ALL_TABLES_INCR_LOAD
insert into sql_job_step(JOB_ID, STEP_ORDER, STEP_SQL) values($raw_jobid, 1250, 'DELETE FROM <<dl_db>>.<<dl_schema>>.locations_raw where task_id <> (select max(task_id) from <<dl_db>>.<<dl_schema>>.locations_raw);');
insert into sql_job_step(JOB_ID, STEP_ORDER, STEP_SQL) values($raw_jobid, 1300, 'commit;');
insert into sql_job_step(JOB_ID, STEP_ORDER, STEP_SQL) values($raw_jobid, 1350, 
        'insert into <<dm_db>>.util.message_log (TASK_ID, EXECUTION_ID, TYPE, SUBTYPE, MESSAGE) ' ||
        'select <<task_id>> task_id, <<exec_id>> execution_id, \'SQLMessage\' type, \'DL_LOCATION_COUNT\' subtype, ' ||
        'object_construct(\'domain\', domain, \'count\', cnt::string) message ' ||
        'from (select domain, count(*) cnt ' || 
        'from <<dl_db>>.<<dl_schema>>.LOCATIONS_RAW group by 1);');

insert into sql_job_step(JOB_ID, STEP_ORDER, STEP_SQL) values($raw_jobid, 1360, 'begin transaction;');
insert into sql_job_step(JOB_ID, STEP_ORDER, STEP_SQL) values($raw_jobid, 1370, 'MERGE INTO <<dl_db>>.<<dl_schema>>.web_users_raw T USING ' || 
                                                              '(select * from <<dl_db>>.<<dl_schema>>.web_users_raw_stage) AS S ' ||
                                                              'ON T.ID = S.ID WHEN MATCHED THEN UPDATE SET T.DOMAIN=S.DOMAIN, T.JSON=S.JSON, T.ID=S.ID, T.SYSTEM_QUERY_TS = S.SYSTEM_QUERY_TS, T.TASK_ID = S.TASK_ID, T.EXECUTION_ID = S.EXECUTION_ID, T.METADATA = S.METADATA, T.METADATA_FILENAME = S.METADATA_FILENAME ' ||
                                                              'WHEN NOT MATCHED THEN INSERT(DOMAIN, JSON, ID, SYSTEM_QUERY_TS, TASK_ID, EXECUTION_ID, METADATA, METADATA_FILENAME) VALUES(S.DOMAIN, S.JSON, S.ID, S.SYSTEM_QUERY_TS, S.TASK_ID, S.EXECUTION_ID, S.METADATA, S.METADATA_FILENAME);');
// the following step 1375 is only needed if choose to do web_users recreation daily
insert into sql_job_step(JOB_ID, STEP_ORDER, STEP_SQL) values($raw_jobid, 1375, 'DELETE FROM <<dl_db>>.<<dl_schema>>.web_users_raw where task_id <> (select max(task_id) from <<dl_db>>.<<dl_schema>>.web_users_raw);');
insert into sql_job_step(JOB_ID, STEP_ORDER, STEP_SQL) values($raw_jobid, 1380, 'commit;');
insert into sql_job_step(JOB_ID, STEP_ORDER, STEP_SQL) values($raw_jobid, 1382, 
        'insert into <<dm_db>>.util.message_log (TASK_ID, EXECUTION_ID, TYPE, SUBTYPE, MESSAGE) ' ||
        'select <<task_id>> task_id, <<exec_id>> execution_id, \'SQLMessage\' type, \'DL_WEB_USER_COUNT\' subtype, ' ||
        'object_construct(\'domain\', domain, \'count\', cnt::string) message ' ||
        'from (select domain, count(*) cnt ' || 
        'from <<dl_db>>.<<dl_schema>>.WEB_USERS_RAW group by 1);');

insert into sql_job_step(JOB_ID, STEP_ORDER, STEP_SQL) values($raw_jobid, 1384, 'begin transaction;');
insert into sql_job_step(JOB_ID, STEP_ORDER, STEP_SQL) values($raw_jobid, 1386, 'MERGE INTO <<dl_db>>.<<dl_schema>>.action_times_raw T USING ' || 
                                                              '(select * from <<dl_db>>.<<dl_schema>>.action_times_raw_stage) AS S ' ||
                                                              'ON T.ID = S.ID WHEN MATCHED THEN UPDATE SET T.DOMAIN=S.DOMAIN, T.JSON=S.JSON, T.ID=S.ID, T.SYSTEM_QUERY_TS = S.SYSTEM_QUERY_TS, T.TASK_ID = S.TASK_ID, T.EXECUTION_ID = S.EXECUTION_ID, T.METADATA = S.METADATA, T.METADATA_FILENAME = S.METADATA_FILENAME ' ||
                                                              'WHEN NOT MATCHED THEN INSERT(DOMAIN, JSON, ID, SYSTEM_QUERY_TS, TASK_ID, EXECUTION_ID, METADATA, METADATA_FILENAME) VALUES(S.DOMAIN, S.JSON, S.ID, S.SYSTEM_QUERY_TS, S.TASK_ID, S.EXECUTION_ID, S.METADATA, S.METADATA_FILENAME);');
insert into sql_job_step(JOB_ID, STEP_ORDER, STEP_SQL) values($raw_jobid, 1388, 'commit;');
insert into sql_job_step(JOB_ID, STEP_ORDER, STEP_SQL) values($raw_jobid, 1390, 
        'insert into <<dm_db>>.util.message_log (TASK_ID, EXECUTION_ID, TYPE, SUBTYPE, MESSAGE) ' ||
        'select <<task_id>> task_id, <<exec_id>> execution_id, \'SQLMessage\' type, \'DL_ACTION_TIME_COUNT\' subtype, ' ||
        'object_construct(\'domain\', domain, \'count\', cnt::string) message ' ||
        'from (select domain, count(*) cnt ' || 
        'from <<dl_db>>.<<dl_schema>>.ACTION_TIMES_RAW group by 1);');

insert into sql_job_step(JOB_ID, STEP_ORDER, STEP_SQL) values($raw_jobid, 1400, 'begin transaction;');
insert into sql_job_step(JOB_ID, STEP_ORDER, STEP_SQL) values($raw_jobid, 1500, 'INSERT INTO <<dm_db>>.INTEGRATION.CONFIG_CASE_FIELDS SELECT CFNT.* ' || 
                                                              'FROM <<dm_db>>.INTEGRATION.CASE_FIELDS_CONFIG_STG CFNT ' ||
                                                              'LEFT JOIN <<dm_db>>.INTEGRATION.CONFIG_CASE_FIELDS CCF on CFNT.CASE_TYPE = CCF.CASE_TYPE and  CFNT.FIELD_NAME = CCF.FIELD_NAME ' ||
                                                              'WHERE NULLIF(CFNT.CASE_TYPE,\'\') is not null and CCF.FIELD_NAME is NULL;');
insert into sql_job_step(JOB_ID, STEP_ORDER, STEP_SQL) values($raw_jobid, 1600, 'commit;');
insert into sql_job_step(JOB_ID, STEP_ORDER, STEP_SQL) values($raw_jobid, 1610, 'begin transaction;');
insert into sql_job_step(JOB_ID, STEP_ORDER, STEP_SQL) values($raw_jobid, 1620, 'INSERT INTO <<dm_db>>.INTEGRATION.CONFIG_FIXTURE_FIELDS SELECT CFNT.* ' || 
                                                              'FROM <<dm_db>>.INTEGRATION.FIXTURE_FIELDS_CONFIG_STG CFNT ' ||
                                                              'LEFT JOIN <<dm_db>>.INTEGRATION.CONFIG_FIXTURE_FIELDS CCF on CFNT.FIXTURE_TYPE = CCF.FIXTURE_TYPE and  CFNT.FIELD_NAME = CCF.FIELD_NAME ' ||
                                                              'WHERE NULLIF(CFNT.FIXTURE_TYPE,\'\') is not null and CCF.FIELD_NAME is NULL;');
insert into sql_job_step(JOB_ID, STEP_ORDER, STEP_SQL) values($raw_jobid, 1630, 'commit;');
insert into sql_job_step(JOB_ID, STEP_ORDER, STEP_SQL) values($raw_jobid, 1640, 'begin transaction;');
insert into sql_job_step(JOB_ID, STEP_ORDER, STEP_SQL) values($raw_jobid, 1650, 'INSERT INTO <<dm_db>>.INTEGRATION.CONFIG_LOCATION_FIELDS SELECT CFNT.* ' || 
                                                              'FROM <<dm_db>>.INTEGRATION.LOCATION_FIELDS_CONFIG_STG CFNT ' ||
                                                              'LEFT JOIN <<dm_db>>.INTEGRATION.CONFIG_LOCATION_FIELDS CCF on CFNT.FIELD_NAME = CCF.FIELD_NAME ' ||
                                                              'WHERE CCF.FIELD_NAME is NULL;');
insert into sql_job_step(JOB_ID, STEP_ORDER, STEP_SQL) values($raw_jobid, 1660, 'commit;');

insert into sql_job_step(JOB_ID, STEP_ORDER, STEP_SQL) values($raw_jobid, 1670, 'begin transaction;');
insert into sql_job_step(JOB_ID, STEP_ORDER, STEP_SQL) values($raw_jobid, 1680, 'INSERT INTO <<dm_db>>.INTEGRATION.CONFIG_WEB_USER_FIELDS SELECT CFNT.* ' || 
                                                              'FROM <<dm_db>>.INTEGRATION.WEB_USER_FIELDS_CONFIG_STG CFNT ' ||
                                                              'LEFT JOIN <<dm_db>>.INTEGRATION.CONFIG_WEB_USER_FIELDS CCF on CFNT.FIELD_NAME = CCF.FIELD_NAME ' ||
                                                              'WHERE CCF.FIELD_NAME is NULL;');
insert into sql_job_step(JOB_ID, STEP_ORDER, STEP_SQL) values($raw_jobid, 1690, 'commit;');

insert into sql_job_step(JOB_ID, STEP_ORDER, STEP_SQL) values($raw_jobid, 1691, 'begin transaction;');
insert into sql_job_step(JOB_ID, STEP_ORDER, STEP_SQL) values($raw_jobid, 1692, 'INSERT INTO <<dm_db>>.INTEGRATION.CONFIG_ACTION_TIME_FIELDS SELECT CFNT.* ' || 
                                                              'FROM <<dm_db>>.INTEGRATION.ACTION_TIME_FIELDS_CONFIG_STG CFNT ' ||
                                                              'LEFT JOIN <<dm_db>>.INTEGRATION.CONFIG_ACTION_TIME_FIELDS CCF on CFNT.FIELD_NAME = CCF.FIELD_NAME ' ||
                                                              'WHERE CCF.FIELD_NAME is NULL;');
insert into sql_job_step(JOB_ID, STEP_ORDER, STEP_SQL) values($raw_jobid, 1693, 'commit;');

insert into sql_job_step(JOB_ID, STEP_ORDER, STEP_SQL) values($raw_jobid, 1700, 'CREATE OR REPLACE TABLE <<dm_db>>.DL.CASES_RAW COPY GRANTS CLONE  <<dl_db>>.<<dl_schema>>.CASES_RAW;');
insert into sql_job_step(JOB_ID, STEP_ORDER, STEP_SQL) values($raw_jobid, 1800, 'CREATE OR REPLACE TABLE <<dm_db>>.DL.FIXTURES_RAW COPY GRANTS CLONE  <<dl_db>>.<<dl_schema>>.FIXTURES_RAW;');
insert into sql_job_step(JOB_ID, STEP_ORDER, STEP_SQL) values($raw_jobid, 1900, 'CREATE OR REPLACE TABLE <<dm_db>>.DL.FORMS_RAW COPY GRANTS CLONE  <<dl_db>>.<<dl_schema>>.FORMS_RAW;');
insert into sql_job_step(JOB_ID, STEP_ORDER, STEP_SQL) values($raw_jobid, 2000, 'CREATE OR REPLACE TABLE <<dm_db>>.DL.LOCATIONS_RAW COPY GRANTS CLONE  <<dl_db>>.<<dl_schema>>.LOCATIONS_RAW;');
insert into sql_job_step(JOB_ID, STEP_ORDER, STEP_SQL) values($raw_jobid, 2200, 'CREATE OR REPLACE TABLE <<dm_db>>.DL.WEB_USERS_RAW COPY GRANTS CLONE  <<dl_db>>.<<dl_schema>>.WEB_USERS_RAW;');
insert into sql_job_step(JOB_ID, STEP_ORDER, STEP_SQL) values($raw_jobid, 2300, 'CREATE OR REPLACE TABLE <<dm_db>>.DL.ACTION_TIMES_RAW COPY GRANTS CLONE  <<dl_db>>.<<dl_schema>>.ACTION_TIMES_RAW;');


insert into sql_job(JOB_NAME, JOB_DESC) values('Raw Data Stage Delete', 'Delete all rows from raw stage tables that are part of a successful run');

set del_jobid = (select job_id from sql_job where job_name='Raw Data Stage Delete');

insert into sql_job_step(JOB_ID, STEP_ORDER, STEP_SQL) values($del_jobid, 100, 'begin transaction;');
insert into sql_job_step(JOB_ID, STEP_ORDER, STEP_SQL) values($del_jobid, 125, 'UPDATE <<dl_db>>.<<dl_schema>>.CASES_RAW c ' || 
                                                              'SET c.DELETED_FLAG = TRUE, c.DELETED_DATE = f.formdate, c.DELETED_FORMID = f.formid ' ||
                                                              'FROM <<dm_db>>.INTEGRATION.SRC_UPDATE_CASE_DELETIONS_STAGE f ' ||
                                                              'WHERE c.ID = f.caseid and not c.DELETED_FLAG;');
insert into sql_job_step(JOB_ID, STEP_ORDER, STEP_SQL) values($del_jobid, 150, 'commit;');
insert into sql_job_step(JOB_ID, STEP_ORDER, STEP_SQL) values($del_jobid, 165, 
        'insert into <<dm_db>>.util.message_log (TASK_ID, EXECUTION_ID, TYPE, SUBTYPE, MESSAGE) ' ||
        'select <<task_id>> task_id, <<exec_id>> execution_id, \'SQLMessage\' type, \'DL_CASE_COUNT\' subtype, ' ||
        'object_construct(\'domain\', domain, \'case_type\', case_type, \'total_count\', cnt::string, \'delete_count\', cnt_a::string) message ' ||
        'from (select domain, JSON:properties.case_type::string case_type, count(*) cnt, sum(DELETED_FLAG::integer) cnt_a ' || 
        'from <<dl_db>>.<<dl_schema>>.CASES_RAW group by 1,2);');
insert into sql_job_step(JOB_ID, STEP_ORDER, STEP_SQL) values($del_jobid, 175, 'begin transaction;');
insert into sql_job_step(JOB_ID, STEP_ORDER, STEP_SQL) values($del_jobid, 200, 'DELETE FROM <<dl_db>>.<<dl_schema>>.fixtures_raw_stage;');
insert into sql_job_step(JOB_ID, STEP_ORDER, STEP_SQL) values($del_jobid, 300, 'DELETE FROM <<dl_db>>.<<dl_schema>>.cases_raw_stage;');
insert into sql_job_step(JOB_ID, STEP_ORDER, STEP_SQL) values($del_jobid, 400, 'DELETE FROM <<dl_db>>.<<dl_schema>>.forms_raw_stage;');
insert into sql_job_step(JOB_ID, STEP_ORDER, STEP_SQL) values($del_jobid, 500, 'DELETE FROM <<dl_db>>.<<dl_schema>>.locations_raw_stage;');
insert into sql_job_step(JOB_ID, STEP_ORDER, STEP_SQL) values($del_jobid, 550, 'DELETE FROM <<dl_db>>.<<dl_schema>>.web_users_raw_stage;');
insert into sql_job_step(JOB_ID, STEP_ORDER, STEP_SQL) values($del_jobid, 560, 'DELETE FROM <<dl_db>>.<<dl_schema>>.action_times_raw_stage;');
insert into sql_job_step(JOB_ID, STEP_ORDER, STEP_SQL) values($del_jobid, 600, 'commit;');



//////                                            //////
////// BELOW ARE CALLS to make after initial load //////
//////                                            //////
use role sysadmin;

-- Ingest data from s3; for initial loads, all files should be in the same s3 bucket timestamp folder
-- then you can harcode that folder in the call below in the last parameter (remember to enclose with /)
Call metadata.procedures.sp_ingest_transform('INITIAL_LOAD', 'S3_TO_RAW', 'co-carecoordination-perf', 'DATALAKE_DEV', 'CO_CARE_COORDINATION_COMMCARE_PERF', 'DM_CO_CARE_COORD_PERF', 'S3_INGEST|', null, 'case|form|location|fixture|web-user|action_times');


--move initial load for stage to full tables and populate field info in Integration
Call metadata.procedures.sp_ingest_transform('INITIAL_LOAD', 'RAW_DATA_LOAD', 'co-carecoordination-perf', 'DATALAKE_DEV', 'CO_CARE_COORDINATION_COMMCARE_PERF', 'DM_CO_CARE_COORD_PERF', 'STAGE_TO_RAW|', null, '');


--activate all standard fields; update if some fields need to be omitted
--this will be needed below for the Update Config SQL Job
update INTEGRATION.config_case_fields set include_in_dim = true where field_alias<>'domain' and field_alias<>'id';

select case_type, lower(ifnull(field_name_override, field_alias)) field_alias2, count(*) 
from INTEGRATION.CONFIG_CASE_FIELDS where include_in_dim group by 1,2 having count(*)>1 or field_alias2 in ('domain','id');
--check the above fields and turn include in dim to false for duplicate fields that shouldn't be included 
--OR set field name override to something unique to that case type

--Below query will isolate any individual column values that cannot be parsed to the data type set in the config
--make sure to set the data type override appropriately so the views won't error
select v.*, c.field_data_type from INTEGRATION.CASE_FIELD_VALUES_ALL v left join INTEGRATION.CONFIG_CASE_FIELDS c on c.case_type = v.case_type and c.field_name = v.field_name
where v.field_value is not null and v.field_value<>'' and (
    ifnull(c.data_type_override, c.field_data_type) = 'Date' and v.date_value is null or
    ifnull(c.data_type_override, c.field_data_type) = 'Datetime' and v.datetime_value is null or
    ifnull(c.data_type_override, c.field_data_type) = 'Boolean' and v.boolean_value is null or
    ifnull(c.data_type_override, c.field_data_type) = 'Number' and v.number_value is null)
;

-- One time create views and tables
Call metadata.procedures.sp_ingest_transform('INITIAL_LOAD', 'GENERATE_CASE_VIEWS', 'co-carecoordination-perf', 'DATALAKE_DEV', 'CO_CARE_COORDINATION_COMMCARE_PERF', 'DM_CO_CARE_COORD_PERF', 'SELECT SQL_TEXT FROM DM_CO_CARE_COORD_PERF.INTEGRATION.GENERATE_CASE_VIEWS;', null, '');
Call metadata.procedures.sp_ingest_transform('INITIAL_LOAD', 'GENERATE_CASE_TABLES', 'co-carecoordination-perf', 'DATALAKE_DEV', 'CO_CARE_COORDINATION_COMMCARE_PERF', 'DM_CO_CARE_COORD_PERF', 'SELECT SQL_TEXT FROM DM_CO_CARE_COORD_PERF.INTEGRATION.GENERATE_CASE_TABLES;', null, '');

-- If data type issues occur, override the data type in the case config so all values make it through

--activate all standard fields; update if some fields need to be omitted
--this will be needed below for the Update Config SQL Job
update INTEGRATION.config_fixture_fields set include_in_dim = true where field_alias<>'domain' and field_alias<>'id';

select fixture_type, lower(ifnull(field_name_override, field_alias)) field_alias2, count(*) 
from INTEGRATION.CONFIG_FIXTURE_FIELDS where include_in_dim group by 1,2 having count(*)>1 or field_alias2 in ('domain','id');
--check the above fields and turn include in dim to false for duplicate fields that shouldn't be included 
--OR set field name override to something unique to that case type

--Below query will isolate any individual column values that cannot be parsed to the data type set in the config
--make sure to set the data type override appropriately so the views won't error
select v.*, c.field_data_type from INTEGRATION.FIXTURE_FIELD_VALUES_ALL v left join INTEGRATION.CONFIG_FIXTURE_FIELDS c on c.fixture_type = v.fixture_type and c.field_name = v.field_name
where v.field_value is not null and v.field_value<>'' and (
    ifnull(c.data_type_override, c.field_data_type) = 'Date' and v.date_value is null or
    ifnull(c.data_type_override, c.field_data_type) = 'Datetime' and v.datetime_value is null or
    ifnull(c.data_type_override, c.field_data_type) = 'Boolean' and v.boolean_value is null or
    ifnull(c.data_type_override, c.field_data_type) = 'Number' and v.number_value is null)
;

-- One time create views and tables
Call metadata.procedures.sp_ingest_transform('INITIAL_LOAD', 'GENERATE_FIXTURE_VIEWS', 'co-carecoordination-perf', 'DATALAKE_DEV', 'CO_CARE_COORDINATION_COMMCARE_PERF', 'DM_CO_CARE_COORD_PERF', 'SELECT SQL_TEXT FROM DM_CO_CARE_COORD_PERF.INTEGRATION.GENERATE_FIXTURE_VIEWS;', null, '');
Call metadata.procedures.sp_ingest_transform('INITIAL_LOAD', 'GENERATE_FIXTURE_TABLES', 'co-carecoordination-perf', 'DATALAKE_DEV', 'CO_CARE_COORDINATION_COMMCARE_PERF', 'DM_CO_CARE_COORD_PERF', 'SELECT SQL_TEXT FROM DM_CO_CARE_COORD_PERF.INTEGRATION.GENERATE_FIXTURE_TABLES;', null, '');

-- If data type issues occur, override the data type in the fixture config so all values make it through

--activate all standard fields; update if some fields need to be omitted
--this will be needed below for the Update Config SQL Job
update INTEGRATION.config_location_fields set include_in_dim = true where field_alias<>'domain' and field_alias<>'id';

select lower(ifnull(field_name_override, field_alias)) field_alias2, count(*) 
from INTEGRATION.CONFIG_LOCATION_FIELDS where include_in_dim group by 1 having count(*)>1 or field_alias2 in ('domain','id');
--check the above fields and turn include in dim to false for duplicate fields that shouldn't be included 
--OR set field name override to something unique to that case type

--Below query will isolate any individual column values that cannot be parsed to the data type set in the config
--make sure to set the data type override appropriately so the views won't error
select v.*, c.field_data_type from INTEGRATION.LOCATION_FIELD_VALUES_ALL v left join INTEGRATION.CONFIG_LOCATION_FIELDS c on c.field_name = v.field_name
where v.field_value is not null and v.field_value<>'' and (
    ifnull(c.data_type_override, c.field_data_type) = 'Date' and v.date_value is null or
    ifnull(c.data_type_override, c.field_data_type) = 'Datetime' and v.datetime_value is null or
    ifnull(c.data_type_override, c.field_data_type) = 'Boolean' and v.boolean_value is null or
    ifnull(c.data_type_override, c.field_data_type) = 'Number' and v.number_value is null)
;

-- One time create views and tables
Call metadata.procedures.sp_ingest_transform('INITIAL_LOAD', 'GENERATE_LOCATION_VIEWS', 'co-carecoordination-perf', 'DATALAKE_DEV', 'CO_CARE_COORDINATION_COMMCARE_PERF', 'DM_CO_CARE_COORD_PERF', 'SELECT SQL_TEXT FROM DM_CO_CARE_COORD_PERF.INTEGRATION.GENERATE_LOCATION_VIEWS;', null, '');
Call metadata.procedures.sp_ingest_transform('INITIAL_LOAD', 'GENERATE_LOCATION_TABLES', 'co-carecoordination-perf', 'DATALAKE_DEV', 'CO_CARE_COORDINATION_COMMCARE_PERF', 'DM_CO_CARE_COORD_PERF', 'SELECT SQL_TEXT FROM DM_CO_CARE_COORD_PERF.INTEGRATION.GENERATE_LOCATION_TABLES;', null, '');

-- If data type issues occur, override the data type in the location config so all values make it through

--activate all standard fields; update if some fields need to be omitted
--this will be needed below for the Update Config SQL Job
update INTEGRATION.config_web_user_fields set include_in_dim = true where field_alias<>'domain' and field_alias<>'id' and not(field_alias like 'view_tableau_list%' and field_alias <> 'view_tableau_list') and not(field_alias like 'view_report_list%' and field_alias <> 'view_report_list') and not(field_alias like 'phone_numbers%' and field_alias <> 'phone_numbers');

--manual data type override
update INTEGRATION.config_web_user_fields set DATA_TYPE_OVERRIDE = 'Number(38,0)' where field_alias like 'view_tableau_list%' and field_alias <> 'view_tableau_list';

select lower(ifnull(field_name_override, field_alias)) field_alias2, count(*) 
from INTEGRATION.CONFIG_WEB_USER_FIELDS where include_in_dim group by 1 having count(*)>1 or field_alias2 in ('domain','id');
--check the above fields and turn include in dim to false for duplicate fields that shouldn't be included 
--OR set field name override to something unique to that case type

--Below query will isolate any individual column values that cannot be parsed to the data type set in the config
--make sure to set the data type override appropriately so the views won't error
select v.*, c.field_data_type from INTEGRATION.WEB_USER_FIELD_VALUES_ALL v left join INTEGRATION.CONFIG_WEB_USER_FIELDS c on c.field_name = v.field_name
where v.field_value is not null and v.field_value<>'' and (
    ifnull(c.data_type_override, c.field_data_type) = 'Date' and v.date_value is null or
    ifnull(c.data_type_override, c.field_data_type) = 'Datetime' and v.datetime_value is null or
    ifnull(c.data_type_override, c.field_data_type) = 'Boolean' and v.boolean_value is null or
    ifnull(c.data_type_override, c.field_data_type) = 'Number' and v.number_value is null)
;

-- One time create views and tables
Call metadata.procedures.sp_ingest_transform('INITIAL_LOAD', 'GENERATE_WEB_USER_VIEWS', 'co-carecoordination-perf', 'DATALAKE_DEV', 'CO_CARE_COORDINATION_COMMCARE_PERF', 'DM_CO_CARE_COORD_PERF', 'SELECT SQL_TEXT FROM DM_CO_CARE_COORD_PERF.INTEGRATION.GENERATE_WEB_USER_VIEWS;', null, '');
Call metadata.procedures.sp_ingest_transform('INITIAL_LOAD', 'GENERATE_WEB_USER_TABLES', 'co-carecoordination-perf', 'DATALAKE_DEV', 'CO_CARE_COORDINATION_COMMCARE_PERF', 'DM_CO_CARE_COORD_PERF', 'SELECT SQL_TEXT FROM DM_CO_CARE_COORD_PERF.INTEGRATION.GENERATE_WEB_USER_TABLES;', null, '');

// for action times //

--activate all standard fields; update if some fields need to be omitted
--this will be needed below for the Update Config SQL Job
update INTEGRATION.config_action_time_fields set include_in_dim = true where field_alias<>'domain' and field_alias<>'id';

select lower(ifnull(field_name_override, field_alias)) field_alias2, count(*) 
from INTEGRATION.CONFIG_ACTION_TIME_FIELDS where include_in_dim group by 1 having count(*)>1 or field_alias2 in ('domain','id');
--check the above fields and turn include in dim to false for duplicate fields that shouldn't be included 
--OR set field name override to something unique to that case type

--Below query will isolate any individual column values that cannot be parsed to the data type set in the config
--make sure to set the data type override appropriately so the views won't error
select v.*, c.field_data_type from INTEGRATION.ACTION_TIME_FIELD_VALUES_ALL v left join INTEGRATION.CONFIG_ACTION_TIME_FIELDS c on c.field_name = v.field_name
where v.field_value is not null and v.field_value<>'' and (
    ifnull(c.data_type_override, c.field_data_type) = 'Date' and v.date_value is null or
    ifnull(c.data_type_override, c.field_data_type) = 'Datetime' and v.datetime_value is null or
    ifnull(c.data_type_override, c.field_data_type) = 'Boolean' and v.boolean_value is null or
    ifnull(c.data_type_override, c.field_data_type) = 'Number' and v.number_value is null)
;

-- One time create views and tables
Call metadata.procedures.sp_ingest_transform('INITIAL_LOAD', 'GENERATE_ACTION_TIME_VIEWS', 'co-carecoordination-perf', 'DATALAKE_DEV', 'CO_CARE_COORDINATION_COMMCARE_PERF', 'DM_CO_CARE_COORD_PERF', 'SELECT SQL_TEXT FROM DM_CO_CARE_COORD_PERF.INTEGRATION.GENERATE_ACTION_TIME_VIEWS;', null, '');
Call metadata.procedures.sp_ingest_transform('INITIAL_LOAD', 'GENERATE_ACTION_TIME_TABLES', 'co-carecoordination-perf', 'DATALAKE_DEV', 'CO_CARE_COORDINATION_COMMCARE_PERF', 'DM_CO_CARE_COORD_PERF', 'SELECT SQL_TEXT FROM DM_CO_CARE_COORD_PERF.INTEGRATION.GENERATE_ACTION_TIME_TABLES;', null, '');

-- If data type issues occur, override the data type in the location config so all values make it through

-- delete staged data from initial load
Call metadata.procedures.sp_ingest_transform('INITIAL_LOAD', 'STAGE_DELETE', 'co-carecoordination-perf', 'DATALAKE_DEV', 'CO_CARE_COORDINATION_COMMCARE_PERF', 'DM_CO_CARE_COORD_PERF', 'STAGE_DELETE|', null, '');

-- make new SQL Job with steps for the config
-- use the final UPDATE statements from each source type above, adding the dm_db placeholder and escaping quotes

insert into util.sql_job(JOB_NAME, JOB_DESC) values('Update Config', 'Update field configs for all sources, these may need occasional changes');

set up_jobid = (select job_id from util.sql_job where job_name='Update Config');

insert into util.sql_job_step(JOB_ID, STEP_ORDER, STEP_SQL) values($up_jobid, 100, 'update <<dm_db>>.INTEGRATION.config_case_fields set include_in_dim = true where field_alias<>\'domain\' and field_alias<>\'id\';');
insert into util.sql_job_step(JOB_ID, STEP_ORDER, STEP_SQL) values($up_jobid, 200, 'update <<dm_db>>.INTEGRATION.config_fixture_fields set include_in_dim = true where field_alias<>\'domain\' and field_alias<>\'id\';');
insert into util.sql_job_step(JOB_ID, STEP_ORDER, STEP_SQL) values($up_jobid, 300, 'update <<dm_db>>.INTEGRATION.config_location_fields set include_in_dim = true where field_alias<>\'domain\' and field_alias<>\'id\';');
insert into util.sql_job_step(JOB_ID, STEP_ORDER, STEP_SQL) values($up_jobid, 400, 'update <<dm_db>>.INTEGRATION.config_web_user_fields set include_in_dim = true where field_alias<>\'domain\' and field_alias<>\'id\' and not(field_alias like \'view_tableau_list%\' and field_alias <> \'view_tableau_list\') and not(field_alias like \'view_report_list%\' and field_alias <> \'view_report_list\') and not(field_alias like \'phone_numbers%\' and field_alias <> \'phone_numbers\');');
insert into util.sql_job_step(JOB_ID, STEP_ORDER, STEP_SQL) values($up_jobid, 500, 'update <<dm_db>>.INTEGRATION.config_action_time_fields set include_in_dim = true where field_alias<>\'domain\' and field_alias<>\'id\';');

--TASKS

--hourly call; ingest from s3 and incrementally add to dm
CREATE OR REPLACE TASK DATALAKE_DEV.CO_CARE_COORDINATION_COMMCARE_PERF.S3_INT_TASK
	schedule='USING CRON 00 * * * * America/New_York'
	error_integration=SNS_INT_OBJ
AS 
DECLARE 
    task_result string default null;
    task_exception EXCEPTION (-20003, 'Task had an error');
BEGIN
    Call metadata.procedures_dev.sp_ingest_transform('S3_DATA_LOAD', 'task_call_sp_ingest_transform', 'co-carecoordination-perf', 'DATALAKE_DEV', 'CO_CARE_COORDINATION_COMMCARE_PERF', 'DM_CO_CARE_COORD_PERF', 'S3_INGEST|STAGE_TO_RAW|INCR_TABLES|STAGE_DELETE|', null, 'case|form|location|fixture|web-user|action_times') into :task_result;
    IF (task_result ilike '%error%') THEN 
        RAISE task_exception;
    END IF;
END
;

ALTER TASK DATALAKE_DEV.CO_CARE_COORDINATION_COMMCARE_PERF.S3_INT_TASK RESUME;

-- once daily recreate call; recreates views and tables, eventually we can add push to commcare as a step
CREATE OR REPLACE TASK DATALAKE_DEV.CO_CARE_COORDINATION_COMMCARE_PERF.S3_INT_TASK_DAILY
//    schedule='USING CRON 45 * * * * America/New_York'
	schedule='USING CRON 30 01 * * * America/New_York'
	error_integration=SNS_INT_OBJ
AS 
DECLARE 
    task_result string default null;
    task_exception EXCEPTION (-20003, 'Task had an error');
BEGIN
    Call metadata.procedures_dev.sp_ingest_transform('DATA_CONFIG_UPDATE', 'task_call_sp_ingest_transform', 'co-carecoordination-perf', 'DATALAKE_DEV', 'CO_CARE_COORDINATION_COMMCARE_PERF', 'DM_CO_CARE_COORD_PERF', 'UPDATE_CONFIG|RECREATE_VIEWS|RECREATE_TABLES|', null, '') into :task_result;
    IF (task_result ilike '%error%') THEN 
        RAISE task_exception;
    END IF;
END
;

ALTER TASK DATALAKE_DEV.CO_CARE_COORDINATION_COMMCARE_PERF.S3_INT_TASK_DAILY RESUME;