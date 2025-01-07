with 
dm_table_data_clinic as (
      select * from  {{ source('dm_table_data', 'CASE_CLINIC') }}
), 
dm_table_data_commcare_user as (
      select * from  {{ source('dm_table_data', 'CASE_COMMCARE_USER') }}
), 
split_users as(
    select 
        hq_user_id, value as location_id 
    from 
        dm_table_data_commcare_user,
    lateral split_to_table(dm_table_data_commcare_user.commcare_location_ids, ' ') 
),

forms_by_facility as (
    select F.form_id, F.form_name, F.received_on, F.user_id, c.case_name as facility_name
    from {{ ref('VW_FORM_METADATA') }} as F
    join split_users as U
    on F.user_id = U.hq_user_id
    join dm_table_data_clinic as C
    on u.location_id = c.owner_id
    where F.form_name = 'Create Profile and Refer' 
        or F.form_name = 'Add Additional Referrals' 
        or F.form_name = 'Outgoing Referral Details' 
        or F.form_name = 'Respond to Referral Request' 
        or F.form_name = 'Update Bed Availability' 
        or F.form_name = 'Update Bed Availability - Mobile'
)
select * from forms_by_facility
