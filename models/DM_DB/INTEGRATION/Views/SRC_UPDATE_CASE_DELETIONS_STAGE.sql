with final as (
select distinct case when typeof(caselist.this) = 'ARRAY' then caselist.value:"@case_id"::string else caselist.this:"@case_id"::string end CASEID, 
case when typeof(caselist.this) = 'ARRAY' then caselist.value:create.case_type::string else caselist.this:create.case_type::string end CASETYPE, 
JSON:id::string FORMID, JSON:server_modified_on::string FORMDATE
    from SRC_FORMS_RAW_STAGE,
    lateral flatten(input => JSON:form.case) caselist
    where JSON:archived = 'true'and CASETYPE is not null
)

select 
	CASEID,
	CASETYPE,
	FORMID,
	FORMDATE
from final