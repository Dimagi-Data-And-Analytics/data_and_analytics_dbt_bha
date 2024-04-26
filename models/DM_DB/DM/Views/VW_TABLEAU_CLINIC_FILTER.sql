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

tableau_users_fixture as 
(

    select email, 'HQ/' || username as username from dm_table_data_web_users
),


recent_user as
    (

        select max(date_opened) max_date, email from dm_table_data_commcare_user group by email
    ),


user_clinic as
    (
        select 
        ccu.email, 
        tuf.username,
        case when ccu.commcare_location_ids is null and statewide_user = TRUE
            then (select listagg(distinct(owner_id), ' ') as all_clinics from dm_table_data_clinic)
            else ccu.commcare_location_ids
        end as location_list 
        
        from dm_table_data_commcare_user ccu inner join recent_user rc on ccu.email = rc.email and ccu.date_opened = rc.max_date
        inner join tableau_users_fixture tuf on upper(tuf.email) = upper(ccu.email)     
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