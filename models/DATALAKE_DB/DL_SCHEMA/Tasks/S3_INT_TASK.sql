-- version control only, will not be compiled or deployed

-- dev
CREATE OR REPLACE TASK DATALAKE_DEV.CO_CARE_COORDINATION_COMMCARE_DEV.S3_INT_TASK
	schedule='USING CRON 00 23 * * * America/New_York'
	error_integration=SNS_INT_OBJ
AS 
DECLARE 
    task_result string default null;
    task_exception EXCEPTION (-20003, 'Task had an error');
BEGIN
    Call metadata.procedures.sp_ingest_transform('S3_DATA_LOAD', 'task_call_sp_ingest_transform', 'co-carecoordination-dev', 'DATALAKE_DEV', 'CO_CARE_COORDINATION_COMMCARE_DEV', 'DM_CO_CARE_COORD_DEV', 'S3_INGEST|STAGE_TO_RAW|INCR_TABLES|STAGE_DELETE|', null, 'case|form|location|fixture|web-user|action_times') into :task_result;
    IF (task_result ilike '%error%') THEN 
        RAISE task_exception;
    END IF;
END
;

-- qa
CREATE OR REPLACE TASK DATALAKE_QA.CO_CARE_COORDINATION_COMMCARE_UAT.S3_INT_TASK
	schedule='USING CRON 00 * * * * America/New_York'
	error_integration=SNS_INT_OBJ
AS 
DECLARE 
    task_result string default null;
    task_exception EXCEPTION (-20003, 'Task had an error');
BEGIN
    Call metadata.procedures.sp_ingest_transform('S3_DATA_LOAD', 'task_call_sp_ingest_transform', 'co-carecoordination-uat', 'DATALAKE_QA', 'CO_CARE_COORDINATION_COMMCARE_UAT', 'DM_CO_CARE_COORD_QA', 'S3_INGEST|STAGE_TO_RAW|INCR_TABLES|STAGE_DELETE|', null, 'case|form|location|fixture|web-user|action_times') into :task_result;
    IF (task_result ilike '%error%') THEN 
        RAISE task_exception;
    END IF;
END
;

-- prod
CREATE OR REPLACE TASK DATALAKE_PROD.CO_CARE_COORDINATION_COMMCARE_PROD.S3_INT_TASK
	schedule='USING CRON 00 * * * * America/New_York'
	error_integration=SNS_INT_OBJ
AS 
DECLARE 
    task_result string default null;
    task_exception EXCEPTION (-20003, 'Task had an error');
BEGIN
    Call metadata.procedures.sp_ingest_transform('S3_DATA_LOAD', 'task_call_sp_ingest_transform', 'co-carecoordination', 'DATALAKE_PROD', 'CO_CARE_COORDINATION_COMMCARE_PROD', 'DM_CO_CARE_COORD_PROD', 'S3_INGEST|STAGE_TO_RAW|INCR_TABLES|STAGE_DELETE|', null, 'case|form|location|fixture|web-user|action_times') into :task_result;
    IF (task_result ilike '%error%') THEN 
        RAISE task_exception;
    END IF;
END
;

-- bha-location-redesign-1
CREATE OR REPLACE TASK DATALAKE_DEV.BHA_LOCATION_REDESIGN_1.S3_INT_TASK
	schedule='USING CRON 00 * * * * America/New_York'
	error_integration=SNS_INT_OBJ
AS 
DECLARE 
    task_result string default null;
    task_exception EXCEPTION (-20003, 'Task had an error');
BEGIN
    Call metadata.procedures.sp_ingest_transform('S3_DATA_LOAD', 'task_call_sp_ingest_transform', 'bha-location-redesign-1', 'DATALAKE_DEV', 'BHA_LOCATION_REDESIGN_1', 'DM_CO_CARE_COORD_BHA_LOCATION_REDESIGN', 'S3_INGEST|STAGE_TO_RAW|INCR_TABLES|STAGE_DELETE|', null, 'case|form|location|fixture|web-user|action_times') into :task_result;
    IF (task_result ilike '%error%') THEN 
        RAISE task_exception;
    END IF;
END
;