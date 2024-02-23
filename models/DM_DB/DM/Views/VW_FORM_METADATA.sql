with 
dl_table_data_forms as (
      select * from  {{ source('dl_table_data', 'FORMS_RAW') }}
), 

final as (
 select id as formid
    ,JSON:form['@name']::string form_name    
    ,JSON:form.meta.timeStart::timestamp form_timeStart
    ,JSON:form.meta.timeEnd::timestamp form_timeEnd
    ,JSON:form.meta.username::string username
    ,JSON:received_on::timestamp received_on
    ,JSON:form.meta.userID::string userID
    ,JSON:form.meta['@xmlns']::string meta_xmlns
    ,JSON:form['@xmlns']::string form_xmlns
    ,JSON:form.meta.appVersion::string meta_appVersion
    ,JSON:form.meta.deviceID::string meta_deviceID
    ,JSON:metadata.location::string location
    ,JSON:app_id::string app_id
    ,JSON:build_id::string build_id
    ,JSON:form['@version']::integer version
    ,JSON:edited_on::timestamp edited_on
    ,JSON:submit_ip::string submit_ip
 from dl_table_data_forms
)

select 
	formid as FORM_ID,
	FORM_NAME,
	form_timeStart as TIME_START_FORM,
	form_timeEnd as TIME_END_FORM,
	USERNAME,
	RECEIVED_ON,
	userID as USER_ID,
	meta_xmlns as XMLNS_META,
	form_xmlns as XMLNS_FORM,
	meta_appVersion as APPVERSION_META,
	meta_deviceID as DEVICE_ID_META,
	LOCATION,
	APP_ID,
	BUILD_ID,
	VERSION,
	EDITED_ON,
	SUBMIT_IP
from final