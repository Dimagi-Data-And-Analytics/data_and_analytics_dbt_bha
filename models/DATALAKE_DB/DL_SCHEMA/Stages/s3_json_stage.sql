-- version control only, will not be compiled or deployed

-- dev
CREATE OR REPLACE STAGE s3_json_stage
    url='s3://commcare-snowflake-data-sync/co-carecoordination-dev/snowflake-copy/'
    Storage_integration = s3_int_obj
    file_format = json_file_format;

-- qa
CREATE OR REPLACE STAGE s3_json_stage
    url='s3://commcare-snowflake-data-sync/co-carecoordination-uat/snowflake-copy/'
    Storage_integration = s3_int_obj
    file_format = json_file_format;

-- prod
CREATE OR REPLACE STAGE s3_json_stage
    url='s3://commcare-snowflake-data-sync/co-carecoordination-prod/snowflake-copy/'
    Storage_integration = s3_int_obj
    file_format = json_file_format;