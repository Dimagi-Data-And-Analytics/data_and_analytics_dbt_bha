with 
dm_table_data_web_users as (
      select * from  {{ source('dm_table_data', 'WEB_USER') }}
), 

dm_table_data_commcare_user as (
      select * from  {{ source('dm_table_data', 'CASE_COMMCARE_USER') }}
), 
 
 dm_table_data_clinic as (
      select * from  {{ source('dm_table_data', 'CASE_CLINIC') }}
), 

state_user as (

    select ccu.case_id,  ccu.email, ccu.date_opened, location_id, location_type_name, location_type_code  
    from dm_table_data_commcare_user ccu left join location l on ccu.commcare_location_ids = l.id 
    where ccu.statewide_user = TRUE

), 

tableau_users_fixture as 
(

    select id, 'HQ/' || username as username from dm_table_data_web_users
),

user_clinic as
    (
        select 
        ccu.email, 
        tuf.username,
        case when su.location_type_code = 'state' and statewide_user = TRUE
            then (select listagg(distinct(owner_id), ' ') as all_clinics from dm_table_data_clinic)
            else ccu.commcare_location_ids
        end as location_list 
        
        from dm_table_data_commcare_user ccu inner join tableau_users_fixture tuf on upper(tuf.id) = upper(ccu.hq_user_id) left join state_user su on su.case_id = ccu.case_id
    ),

flat_list as (
select 
    email,
    username,
   replace(a.value::string, '_', ' ') as location_id
    
from user_clinic, lateral flatten(input=> split(location_list, ' ')) a
)

select 
	email,
	username,
--	location_id,
    case_clinic.case_id as clinic_list
from flat_list inner join case_clinic on flat_list.location_id = case_clinic.owner_id