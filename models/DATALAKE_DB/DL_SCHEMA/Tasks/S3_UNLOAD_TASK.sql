-- version control only, will not be compiled or deployed

-- dev
create or replace task DATALAKE_DEV.CO_CARE_COORDINATION_COMMCARE_DEV.S3_UNLOAD_TASK
	schedule='USING CRON 05 05 * * * America/New_York'
	error_integration=SNS_INT_OBJ
	as DECLARE 
    task_result string default null;
    task_exception EXCEPTION (-20003, 'Task had an error');
BEGIN
    Call metadata.procedures.sp_data_unload('CLIENT_PAYLOAD_UPDATE', 'task_call_sp_data_unload', 'co-carecoordination-dev', 'DATALAKE_DEV', 'CO_CARE_COORDINATION_COMMCARE_DEV', 'DM_CO_CARE_COORD_DEV', 'UNLOAD_SF_TO_S3_AWS|', null) into :task_result;
    IF (task_result ilike '%error%') THEN 
        RAISE task_exception;
    END IF;
END;

-- qa
create or replace task DATALAKE_QA.CO_CARE_COORDINATION_COMMCARE_UAT.S3_UNLOAD_TASK
    warehouse=COMPUTE_WH
	schedule='USING CRON 05 06 * * * America/New_York'
	error_integration=SNS_INT_OBJ
	as DECLARE 
    task_result string default null;
    task_exception EXCEPTION (-20003, 'Task had an error');
BEGIN
    Call metadata.procedures.sp_data_unload('CLIENT_PAYLOAD_UPDATE', 'task_call_sp_data_unload', 'co-carecoordination-uat', 'DATALAKE_QA', 'CO_CARE_COORDINATION_COMMCARE_UAT', 'DM_CO_CARE_COORD_QA', 'UNLOAD_SF_TO_S3_AWS|', null) into :task_result;
    IF (task_result ilike '%error%') THEN 
        RAISE task_exception;
    END IF;
END;

-- prod
create or replace task DATALAKE_PROD.CO_CARE_COORDINATION_COMMCARE_PROD.S3_UNLOAD_TASK
	schedule='USING CRON 15 05 * * * America/New_York'
	error_integration=SNS_INT_OBJ
	as DECLARE 
    task_result string default null;
    task_exception EXCEPTION (-20003, 'Task had an error');
BEGIN
    Call metadata.procedures.sp_data_unload('CLIENT_PAYLOAD_UPDATE', 'task_call_sp_data_unload', 'co-carecoordination', 'DATALAKE_PROD', 'CO_CARE_COORDINATION_COMMCARE_PROD', 'DM_CO_CARE_COORD_PROD', 'UNLOAD_SF_TO_S3_AWS|', null) into :task_result;
    IF (task_result ilike '%error%') THEN 
        RAISE task_exception;
    END IF;
END;

-- bha-location-redesign-1
create or replace task DATALAKE_DEV.BHA_LOCATION_REDESIGN_1.S3_UNLOAD_TASK
	schedule='USING CRON 15 05 * * * America/New_York'
	error_integration=SNS_INT_OBJ
	as DECLARE 
    task_result string default null;
    task_exception EXCEPTION (-20003, 'Task had an error');
BEGIN
    Call metadata.procedures.sp_data_unload('CLIENT_PAYLOAD_UPDATE', 'task_call_sp_data_unload', 'bha-location-redesign-1', 'DATALAKE_DEV', 'BHA_LOCATION_REDESIGN_1', 'DM_CO_CARE_COORD_BHA_LOCATION_REDESIGN', 'UNLOAD_SF_TO_S3_AWS|', null) into :task_result;
    IF (task_result ilike '%error%') THEN 
        RAISE task_exception;
    END IF;
END;