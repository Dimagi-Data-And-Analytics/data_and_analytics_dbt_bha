with 
dm_table_data_client as (
      select * from  {{ source('dm_table_data', 'CASE_CLIENT') }}
), 
 
dm_table_data_alias as (
      select * from  {{ source('dm_table_data', 'CASE_ALIAS') }}
), 

sort_client as
(
    select  
        case_id,
        date_opened,
        social_security_number,
        medicaid_id,
        dob, 
        potential_duplicate_case_ids, 
        duplicate_primary_case_id, 
        non_duplicate_case_ids,
        duplicate_case_ids,
        first_name,
        last_name,
        full_name,
        last_updated
        
    from dm_table_data_client
    where closed = 0 
    order by date_opened
),

-- PART 1 of Scenario Client to Alias on FN,LN if you find any look at the Aliase's Client for more Aliases to match Client to Alias DOB
client_alias_potential as (

	select 
    	l1.case_id as case_id_client_a,
        l1.date_opened as date_opened_client_a,
        l2.case_id as alias_b_case_id,
        l2.date_opened as date_opened_alias_b,
        l2.parent_case_id as parent_case_id_alias_b,
        l1.social_security_number as ssn,
        l2.social_security_number as ssn2,
        l1.medicaid_id as medicaid_id,
        l2.medicaid_id as medicaid_id2,
        l1.dob as dob_display1,
        l2.dob as dob_display2,
        l1.potential_duplicate_case_ids,
        l1.duplicate_primary_case_id,
        l3.potential_duplicate_index_case_ids,
        l1.non_duplicate_case_ids,
        l1.duplicate_case_ids,
        --REGEXP_REPLACE(UPPER(l1.dob), '[^A-Z0-9 ]', '') as dob1,
        --REGEXP_REPLACE(UPPER(l2.dob), '[^A-Z0-9 ]', '') as dob2,
        l1.dob as dob1,
        l2.dob as dob2,
        REGEXP_REPLACE(UPPER(l1.full_name), '[^A-Z0-9 ]', '') as full_name1,
        REGEXP_REPLACE(UPPER(l1.first_name), '[^A-Z0-9 ]', '') as first_name1,
        REGEXP_REPLACE(UPPER(l1.last_name), '[^A-Z0-9 ]', '') as last_name1,
        REGEXP_REPLACE(UPPER(l2.first_name), '[^A-Z0-9 ]', '') as first_name2,
        REGEXP_REPLACE(UPPER(l2.last_name), '[^A-Z0-9 ]', '') as last_name2,
    	l1.first_name as index_first_name, 
    	l1.last_name as index_last_name, 
    	l2.first_name as match_first_name, 
    	l2.last_name as match_last_name, 
        soundex(first_name1) as soundex1,
        soundex(first_name2) as soundex2,
        editdistance(first_name1, first_name2) as ed,
    	'Alias DOB Match' as match_type,
        l1.last_updated 
    from sort_client l1 cross join dm_table_data_alias l2 
    	 inner join dm_table_data_client l3 on l2.parent_case_id = l3.case_id
   	
    where
        	l2.closed = 0 
        and l3.closed = 0
        and l1.case_id <> l2.parent_case_id -- we are looking for duplicates so make sure Case IDs don't match
        and  --demographic check (fn, ln)
                (
                 --check firstname and lastname     
                  (
                     (len(first_name1) < 3 and editdistance(first_name1, first_name2) = 0) or
                     ((len(first_name1) >= 3 and len(first_name1) <6 ) and editdistance(first_name1, first_name2) <= 1) or 
                     (len(first_name1) > 5 and editdistance(first_name1, first_name2) <= 2) or
                     (soundex(first_name1) = soundex(first_name2)) or 
                     (first_name1 = first_name2)

                   )
                         and
                   (  
                     (len(last_name1) < 3 and editdistance(last_name1, last_name2) = 0) or
                     ((len(last_name1) >= 3 and len(last_name1) <6 ) and editdistance(last_name1, last_name2) <= 1) or 
                     (len(last_name1) > 5 and editdistance(last_name1, last_name2) <= 2) or
                     (soundex(last_name1) = soundex(last_name2)) or 
                     (last_name1 = last_name2)
                   )
                     
                 )

),

-- PART 2 of Scenario Client to Alias on FN,LN if you find any look at the Aliase's Client for more Aliases to match Client to Alias DOB
alias_dob_search as (


        select 
        	l1.case_id_client_a as case_id,
            l1.date_opened_client_a as date_opened_client_a,
            l2.parent_case_id as matched_case_id,
            l2.date_opened as date_opened_client_b,
            l1.ssn,
            l2.social_security_number as ssn2,
            l1.medicaid_id as medicaid_id,
            l2.medicaid_id as medicaid_id2,
            l1.dob_display1,
            l2.dob as dob_display2,
            l1.potential_duplicate_case_ids,
            l1.duplicate_primary_case_id,
            l3.potential_duplicate_index_case_ids,
            l1.non_duplicate_case_ids,
            l1.duplicate_case_ids,
            alias_b_case_id,
            parent_case_id_alias_b as client_b_case_id, 
            l2.case_id as alias_c_case_id, 
            l1.dob1 as client_a_dob,
            l2.dob as alias_c_dob,
            --REGEXP_REPLACE(UPPER(l2.dob), '[^A-Z0-9 ]', '') as alias_c_dob,
	        l1.full_name1,
            REGEXP_REPLACE(UPPER(l2.case_name), '[^A-Z0-9 ]', '') as full_name2,
    	    l1.first_name1,
            REGEXP_REPLACE(UPPER(l2.first_name), '[^A-Z0-9 ]', '') as first_name2,
        	l1.last_name1,
        	REGEXP_REPLACE(UPPER(l2.last_name), '[^A-Z0-9 ]', '') as last_name2,
    		l1.index_first_name, 
    		l1.index_last_name, 
    		l2.first_name as match_first_name, 
    		l2.last_name as match_last_name,
            l1.match_type,
            l1.last_updated
            
         from client_alias_potential l1 
        	inner join dm_table_data_alias l2 
            on l2.parent_case_id = l1.parent_case_id_alias_b
            -- add this to get the potential_duplicate_index_case_ids
            left join dm_table_data_client l3 on l2.parent_case_id = l3.case_id
            where 
   
   			--    incremental logic 
			       not array_contains(l2.parent_case_id::variant, split(ifnull(l1.potential_duplicate_case_ids,''),' '))
			       and not array_contains(l1.case_id_client_a::variant, split(ifnull(l3.potential_duplicate_index_case_ids,''),' ')) and
                    
            ( (len(client_a_dob) < 3 and editdistance(client_a_dob, alias_c_dob) = 0) or
            ((len(client_a_dob) >= 3 and len(client_a_dob) <6 ) and editdistance(client_a_dob, alias_c_dob) <= 1) or 
            (len(client_a_dob) > 5 and editdistance(client_a_dob, alias_c_dob) <= 2) or
            (client_a_dob = alias_c_dob) )
    ),
        
--client to client matching (ssn or medicaid) or (fn and ln and dob)
client_search as (
    select
        l1.case_id as case_id,
        l1.date_opened as date_opened,
        l2.case_id as matched_case_id,
        l2.date_opened as date_opened_client_b,
        l1.social_security_number as ssn,
        l2.social_security_number as ssn2,
        l1.medicaid_id as medicaid_id,
        l2.medicaid_id as medicaid_id2,
        l1.dob as dob_display1,
        l2.dob as dob_display2,
        l1.potential_duplicate_case_ids,
        l1.duplicate_primary_case_id,
        l2.potential_duplicate_index_case_ids,
        l1.non_duplicate_case_ids,
        l1.duplicate_case_ids,
      --  REGEXP_REPLACE(UPPER(l1.dob), '[^A-Z0-9 ]', '') as dob1,
      --  REGEXP_REPLACE(UPPER(l2.dob), '[^A-Z0-9 ]', '') as dob2,
        l1.dob as dob1,
        l2.dob as dob2,
        REGEXP_REPLACE(UPPER(l1.full_name), '[^A-Z0-9 ]', '') as full_name1,
        REGEXP_REPLACE(UPPER(l1.first_name), '[^A-Z0-9 ]', '') as first_name1,
        REGEXP_REPLACE(UPPER(l1.last_name), '[^A-Z0-9 ]', '') as last_name1,
        REGEXP_REPLACE(UPPER(l2.full_name), '[^A-Z0-9 ]', '') as full_name2,
        REGEXP_REPLACE(UPPER(l2.first_name), '[^A-Z0-9 ]', '') as first_name2,
        REGEXP_REPLACE(UPPER(l2.last_name), '[^A-Z0-9 ]', '') as last_name2,
    	l1.first_name as index_first_name, 
    	l1.last_name as index_last_name, 
    	l2.first_name as match_first_name, 
    	l2.last_name as match_last_name, 
    	'Client Match' as match_type,
        l1.last_updated 
    	
    from
        sort_client l1 inner join dm_table_data_client l2 -- match case
    where
  --    incremental logic  
        not array_contains(l2.case_id::variant, split(ifnull(l1.potential_duplicate_case_ids,''),' '))
        and not array_contains(l1.case_id::variant, split(ifnull(l2.potential_duplicate_index_case_ids,''),' ')) and
       
     l2.closed = 0 and
         l1.case_id <> l2.case_id -- we are looking for duplicates so make sure Case IDs don't match
        and (
               (l1.social_security_number = l2.social_security_number or l1.medicaid_id = l2.medicaid_id) or
             
                --demographic check
              ( 
                (
                 --check firstname and lastname     
                  (
                     (len(first_name1) < 3 and editdistance(first_name1, first_name2) = 0) or
                     ((len(first_name1) >= 3 and len(first_name1) <6 ) and editdistance(first_name1, first_name2) <= 1) or 
                     (len(first_name1) > 5 and editdistance(first_name1, first_name2) <= 2) or
                     (soundex(first_name1) = soundex(first_name2)) or 
                     (first_name1 = first_name2)

                   )

                         and
                   (  
                     (len(last_name1) < 3 and editdistance(last_name1, last_name2) = 0) or
                     ((len(last_name1) >= 3 and len(last_name1) <6 ) and editdistance(last_name1, last_name2) <= 1) or 
                     (len(last_name1) > 5 and editdistance(last_name1, last_name2) <= 2) or
                     (soundex(last_name1) = soundex(last_name2)) or 
                     (last_name1 = last_name2)
                   )
                     
                 )

                and
                   -- check dob (with firstname and lastname) 
                ( 
                    (len(dob1) < 3 and editdistance(dob1, dob2) = 0) or
                    ((len(dob1) >= 3 and len(dob1) <6 ) and editdistance(dob1, dob2) <= 1) or 
                    (len(dob1) > 5 and editdistance(dob1, dob2) <= 2) or
                    (dob1 = dob2)
                )
            )
       
        )
),

--client to alias matching ssn or medicaid only
alias_search as (
    select
        l1.case_id as case_id,
        l1.date_opened as date_opened,
        l2.parent_case_id as matched_case_id,
        l3.date_opened as date_opened_client_b,
        l1.social_security_number as ssn,
        l2.social_security_number as ssn2,
        l1.medicaid_id as medicaid_id,
        l2.medicaid_id as medicaid_id2,
        l1.dob as dob_display1,
        l2.dob as dob_display2,
        l1.potential_duplicate_case_ids,
        l1.duplicate_primary_case_id,
        l3.potential_duplicate_index_case_ids,
        l1.non_duplicate_case_ids,
        l1.duplicate_case_ids,
        --REGEXP_REPLACE(UPPER(l1.dob), '[^A-Z0-9 ]', '') as dob1,
        --REGEXP_REPLACE(UPPER(l2.dob), '[^A-Z0-9 ]', '') as dob2,
        l1.dob as dob1,
        l2.dob as dob2,
        REGEXP_REPLACE(UPPER(l1.first_name), '[^A-Z0-9 ]', '') as first_name1,
        REGEXP_REPLACE(UPPER(l1.last_name), '[^A-Z0-9 ]', '') as last_name1,
        REGEXP_REPLACE(UPPER(l2.first_name), '[^A-Z0-9 ]', '') as first_name2,
        REGEXP_REPLACE(UPPER(l2.last_name), '[^A-Z0-9 ]', '') as last_name2,
    	l1.first_name as index_first_name, 
    	l1.last_name as index_last_name, 
    	l2.first_name as match_first_name, 
    	l2.last_name as match_last_name,
        editdistance(first_name1, first_name2),
        soundex(first_name1), 
        soundex(first_name2),
        case 
        	
            when (ssn is not null and ssn2 is not null) and ssn=ssn2  then 'ssn match'
            else 'no match'
            end as ssn_match,
        case 
        	
            when (l1.medicaid_id is not null and l2.medicaid_id is not null) and l1.medicaid_id = l2.medicaid_id  then 'medicaid match'
            else 'no match'
            end as med_match,
        
    	'Alias Match' as match_type,
        l1.last_updated 
    	
    from
        
        sort_client l1
        
        cross join dm_table_data_alias l2 
    	inner join dm_table_data_client l3 on l2.parent_case_id = l3.case_id

    	
    where
--    incremental logic 
        not array_contains(l3.case_id::variant, split(ifnull(l1.potential_duplicate_case_ids,''),' '))
        and not array_contains(l1.case_id::variant, split(ifnull(l3.potential_duplicate_index_case_ids,''),' ')) and

         l2.closed = 0
    	and l3.closed = 0
        and l1.case_id <> l3.case_id -- we are looking for duplicates so make sure Case IDs don't match
        and (
                (
                	(
                		l1.social_security_number = l2.social_security_number
                 	) 
                    
                    or 
                	
                    (
                    	l1.medicaid_id = l2.medicaid_id
                    )
                )
           )
),

--client to alias matching on fn,ln and client to alias to client dob match (demographics)
alias_search_demo as (
    select
        l1.case_id as case_id,
        l1.date_opened as date_opened,
        l2.parent_case_id as matched_case_id,
        l3.date_opened as date_opened_client_b,
        l1.social_security_number as ssn,
        l2.social_security_number as ssn2,
        l1.medicaid_id as medicaid_id,
        l2.medicaid_id as medicaid_id2,
        l1.dob as dob_display1,
        l3.dob as dob_display2,
        l1.potential_duplicate_case_ids,
        l1.duplicate_primary_case_id,
        l3.potential_duplicate_index_case_ids,
        l1.non_duplicate_case_ids,
        l1.duplicate_case_ids,
       -- REGEXP_REPLACE(UPPER(l1.dob), '[^A-Z0-9 ]', '') as dob1,
       --REGEXP_REPLACE(UPPER(l3.dob), '[^A-Z0-9 ]', '') as dob2,
        l1.dob as dob1,
        l3.dob as dob2,
        REGEXP_REPLACE(UPPER(l1.first_name), '[^A-Z0-9 ]', '') as first_name1,
        REGEXP_REPLACE(UPPER(l1.last_name), '[^A-Z0-9 ]', '') as last_name1,
        REGEXP_REPLACE(UPPER(l2.first_name), '[^A-Z0-9 ]', '') as first_name2,
        REGEXP_REPLACE(UPPER(l2.last_name), '[^A-Z0-9 ]', '') as last_name2,
    	l1.first_name as index_first_name, 
    	l1.last_name as index_last_name, 
    	l2.first_name as match_first_name, 
    	l2.last_name as match_last_name,
        editdistance(first_name1, first_name2),
        soundex(first_name1), 
        soundex(first_name2),
        case 
        	
            when (ssn is not null and ssn2 is not null) and ssn=ssn2  then 'ssn match'
            else 'no match'
            end as ssn_match,
        case 
        	
            when (l1.medicaid_id is not null and l2.medicaid_id is not null) and l1.medicaid_id = l2.medicaid_id  then 'medicaid match'
            else 'no match'
            end as med_match,
        
    	'Alias Demo Match' as match_type,
        l1.last_updated 
    	
    from
        
        sort_client l1
        
        cross join dm_table_data_alias l2 
    	inner join dm_table_data_client l3 on l2.parent_case_id = l3.case_id

    	
    where
--    ** incremental logic 
        not array_contains(l3.case_id::variant, split(ifnull(l1.potential_duplicate_case_ids,''),' '))
        and not array_contains(l1.case_id::variant, split(ifnull(l3.potential_duplicate_index_case_ids,''),' ')) and

         l2.closed = 0
    	and l3.closed = 0
        and l1.case_id <> l3.case_id -- we are looking for duplicates so make sure Case IDs don't match
        and  ( 
                (
                 --check firstname and lastname     
                  (
                     (len(first_name1) < 3 and editdistance(first_name1, first_name2) = 0) or
                     ((len(first_name1) >= 3 and len(first_name1) <6 ) and editdistance(first_name1, first_name2) <= 1) or 
                     (len(first_name1) > 5 and editdistance(first_name1, first_name2) <= 2) or
                     (soundex(first_name1) = soundex(first_name2)) or 
                     (first_name1 = first_name2)

                   )

                         and
                   (  
                     (len(last_name1) < 3 and editdistance(last_name1, last_name2) = 0) or
                     ((len(last_name1) >= 3 and len(last_name1) <6 ) and editdistance(last_name1, last_name2) <= 1) or 
                     (len(last_name1) > 5 and editdistance(last_name1, last_name2) <= 2) or
                     (soundex(last_name1) = soundex(last_name2)) or 
                     (last_name1 = last_name2)
                   )
                     
                 )

                and
                   -- check dob (with firstname and lastname) 
                ( 
                    (len(dob1) < 3 and editdistance(dob1, dob2) = 0) or
                    ((len(dob1) >= 3 and len(dob1) <6 ) and editdistance(dob1, dob2) <= 1) or 
                    (len(dob1) > 5 and editdistance(dob1, dob2) <= 2) or
                    (dob1 = dob2)
                )
            )
),

--get all match types together
all_matches as (
(select
    case_id as index_case_id,
    date_opened_client_a as index_case_date_opened,
    matched_case_id as match_case_id,
    date_opened_client_b as match_case_date_opened,
    potential_duplicate_case_ids,
    duplicate_primary_case_id,
    potential_duplicate_index_case_ids,
    non_duplicate_case_ids,
    duplicate_case_ids,
   
    null as first_name_match, 
    null as last_name_match, 
    null as ssn_match,
 
    case 
    when len(client_a_dob) < 3 and editdistance(client_a_dob, alias_c_dob) = 0 then TRUE
    when (len(client_a_dob) >= 3 and len(client_a_dob) <6 ) and editdistance(client_a_dob, alias_c_dob) <= 1 then TRUE
    when len(client_a_dob) > 5 and editdistance(client_a_dob, alias_c_dob) <= 2 then TRUE
    when client_a_dob = alias_c_dob then TRUE
    else null
    end as dob_match,
 
	null as medicaid_match,
   
    --adding details to validate the scores
   	index_first_name, 
   	match_first_name, 
  	index_last_name, 
  	match_last_name,
    to_date(dob_display1) as index_dob,
    to_date(dob_display2) as match_dob,
    ssn as index_ssn,
    ssn2 as match_ssn,
    medicaid_id,
    medicaid_id2,
 	match_type,
    convert_timezone('UTC', 'America/Denver',last_updated) as last_updated_mtn
from
    alias_dob_search
where
     
    case_id <> matched_case_id 
    and (contains(non_duplicate_case_ids, match_case_id) = FALSE or non_duplicate_case_ids is null)
    order by case_id)

UNION

(select
    case_id as index_case_id,
    date_opened as index_case_date_opened,
    matched_case_id as match_case_id,
    date_opened_client_b as match_case_date_opened,
    potential_duplicate_case_ids,
    duplicate_primary_case_id,
    potential_duplicate_index_case_ids,
    non_duplicate_case_ids,
    duplicate_case_ids,
   
    case 
    when len(first_name1) < 3 and editdistance(first_name1, first_name2) = 0 then TRUE
    when (len(first_name1) >= 3 and len(first_name1) <6 ) and editdistance(first_name1, first_name2) <= 1 then TRUE
    when len(first_name1) > 5 and editdistance(first_name1, first_name2) <= 2 then TRUE
    when soundex(first_name1) = soundex(first_name2) then TRUE
    when first_name1 = first_name2 then TRUE
    else null
    end as first_name_match,
 
    case 
    when len(last_name1) < 3 and editdistance(last_name1, last_name2) = 0 then TRUE
    when (len(last_name1) >= 3 and len(last_name1) <6 ) and editdistance(last_name1, last_name2) <= 1 then TRUE
    when len(last_name1) > 5 and editdistance(last_name1, last_name2) <= 2 then TRUE
    when soundex(last_name1) = soundex(last_name2) then TRUE
    when last_name1 = last_name2 then TRUE
    else null
    end as last_name_match,
 
    case 
    when ssn = ssn2 then TRUE
    else null
    end as ssn_match,
 
    case 
    when len(dob1) < 3 and editdistance(dob1, dob2) = 0 then TRUE
    when (len(dob1) >= 3 and len(dob1) <6 ) and editdistance(dob1, dob2) <= 1 then TRUE
    when len(dob1) > 5 and editdistance(dob1, dob2) <= 2 then TRUE
    when dob1 = dob2 then TRUE
    else null
    end as dob_match,
 
    case 
    when medicaid_id = medicaid_id2 then TRUE
    else null
    end as medicaid_match,
   
    --adding details to validate the scores
   	index_first_name, 
   	match_first_name, 
  	index_last_name, 
  	match_last_name,
    to_date(dob_display1) as index_dob,
    to_date(dob_display2) as match_dob,
    ssn as index_ssn,
    ssn2 as match_ssn,
    medicaid_id,
    medicaid_id2,
 	match_type,
    convert_timezone('UTC', 'America/Denver',last_updated) as last_updated_mtn
from
    client_search
where
     
    case_id <> matched_case_id 
    and (contains(non_duplicate_case_ids, match_case_id) = FALSE or non_duplicate_case_ids is null)
    order by case_id)
    
   UNION
   
   (select
    case_id as index_case_id,
    date_opened as index_case_date_opened,
    matched_case_id as match_case_id,
    date_opened_client_b as match_case_date_opened,
    potential_duplicate_case_ids,
    duplicate_primary_case_id,
    potential_duplicate_index_case_ids,
    non_duplicate_case_ids,
    duplicate_case_ids,
    case 
        when len(first_name1) < 3 and editdistance(first_name1, first_name2) = 0 then TRUE
        when (len(first_name1) >= 3 and len(first_name1) <6 ) and editdistance(first_name1, first_name2) <= 1 then TRUE
        when len(first_name1) > 5 and editdistance(first_name1, first_name2) <= 2 then TRUE
        when soundex(first_name1) = soundex(first_name2) then TRUE
        when first_name1 = first_name2 then TRUE
    else null
    end as first_name_match,
 
    case 
        when len(last_name1) < 3 and editdistance(last_name1, last_name2) = 0 then TRUE
        when (len(last_name1) >= 3 and len(last_name1) <6 ) and editdistance(last_name1, last_name2) <= 1 then TRUE
        when len(last_name1) > 5 and editdistance(last_name1, last_name2) <= 2 then TRUE
        when soundex(last_name1) = soundex(last_name2) then TRUE
        when last_name1 = last_name2 then TRUE
    else null
    end as last_name_match,
 
    case 
        when ssn = ssn2 then TRUE
    else null
    end as ssn_match,
 
    case 
        when len(dob1) < 3 and editdistance(dob1, dob2) = 0 then TRUE
        when (len(dob1) >= 3 and len(dob1) <6 ) and editdistance(dob1, dob2) <= 1 then TRUE
        when len(dob1) > 5 and editdistance(dob1, dob2) <= 2 then TRUE
        when dob1 = dob2 then TRUE
    else null
    end as dob_match,
 
    case 
        when medicaid_id = medicaid_id2 then TRUE
    else null
    end as medicaid_match,
   
    --adding details to validate the scores
   index_first_name, 
   match_first_name, 
   index_last_name, 
   match_last_name,
    to_date(dob_display1) as index_dob,
    to_date(dob_display2) as match_dob,
    ssn as index_ssn,
    ssn2 as match_ssn,
    medicaid_id,
    medicaid_id2,
 	match_type,
    convert_timezone('UTC', 'America/Denver',last_updated) as last_updated_mtn
    
from
    alias_search
where
   
    case_id <> matched_case_id
    and (contains(non_duplicate_case_ids, match_case_id) = FALSE or non_duplicate_case_ids is null) -- using index non_duplcate_case_ids
    order by case_id)

UNION

(select
    case_id as index_case_id,
    date_opened as index_case_date_opened,
    matched_case_id as match_case_id,
    date_opened_client_b as match_case_date_opened,
    potential_duplicate_case_ids,
    duplicate_primary_case_id,
    potential_duplicate_index_case_ids,
    non_duplicate_case_ids,
    duplicate_case_ids,
    case 
        when len(first_name1) < 3 and editdistance(first_name1, first_name2) = 0 then TRUE
        when (len(first_name1) >= 3 and len(first_name1) <6 ) and editdistance(first_name1, first_name2) <= 1 then TRUE
        when len(first_name1) > 5 and editdistance(first_name1, first_name2) <= 2 then TRUE
        when soundex(first_name1) = soundex(first_name2) then TRUE
        when first_name1 = first_name2 then TRUE
    else null
    end as first_name_match,
 
    case 
        when len(last_name1) < 3 and editdistance(last_name1, last_name2) = 0 then TRUE
        when (len(last_name1) >= 3 and len(last_name1) <6 ) and editdistance(last_name1, last_name2) <= 1 then TRUE
        when len(last_name1) > 5 and editdistance(last_name1, last_name2) <= 2 then TRUE
        when soundex(last_name1) = soundex(last_name2) then TRUE
        when last_name1 = last_name2 then TRUE
    else null
    end as last_name_match,
 
    null as ssn_match,
 
    case 
        when len(dob1) < 3 and editdistance(dob1, dob2) = 0 then TRUE
        when (len(dob1) >= 3 and len(dob1) <6 ) and editdistance(dob1, dob2) <= 1 then TRUE
        when len(dob1) > 5 and editdistance(dob1, dob2) <= 2 then TRUE
        when dob1 = dob2 then TRUE
    else null
    end as dob_match,
 
    null as medicaid_match,
   
    --adding details to validate the scores
   index_first_name, 
   match_first_name, 
   index_last_name, 
   match_last_name,
    to_date(dob_display1) as index_dob,
    to_date(dob_display2) as match_dob,
    ssn as index_ssn,
    ssn2 as match_ssn,
    medicaid_id,
    medicaid_id2,
 	match_type,
    convert_timezone('UTC', 'America/Denver',last_updated) as last_updated_mtn
    
from
    alias_search_demo
where
   
    case_id <> matched_case_id
    and (contains(non_duplicate_case_ids, match_case_id) = FALSE or non_duplicate_case_ids is null) -- using index non_duplcate_case_ids
    order by case_id)
    order by index_case_id ),


--compare the new matches to the matchees that are already in case_client 
sorted_matches as ( --against previous matches
    select count(*) OVER (PARTITION BY ARRAY_TO_STRING(ARRAY_SORT( ARRAY_CONSTRUCT(INDEX_CASE_ID, MATCH_CASE_ID )),' ') order by index_case_date_opened) as                 OPENED_ORDER,
    iff(opened_order = 1, 'keep', null) as keep, 
    INDEX_CASE_ID, 
    index_case_date_opened, 
    MATCH_CASE_ID, 
    ARRAY_TO_STRING(ARRAY_SORT( ARRAY_CONSTRUCT(INDEX_CASE_ID, MATCH_CASE_ID )),' ') as combo, match_status 
from(

        select 
            INDEX_CASE_ID, 
            index_case_date_opened, 
            MATCH_CASE_ID ,
            'new' as Match_status
            
        from all_matches
        
        union 
        select 
            case_id, 
            date_opened, 
            m.value::string as match_ids,
            'old' as Match_status

        from dm_table_data_client, LATERAL FLATTEN(input=>split(potential_duplicate_case_ids, ' ') )m where potential_duplicate_case_ids <> '' and potential_duplicate_case_ids is not null and closed = FALSE)

),

--and keep only those with the oldest open date and that are new
final as (
select 
    s.INDEX_CASE_ID, 
    s.MATCH_CASE_ID,
    a.potential_duplicate_case_ids,
    a.duplicate_primary_case_id,
    a.potential_duplicate_index_case_ids,
    a.non_duplicate_case_ids,
    a.duplicate_case_ids,
    a.first_name_match,
    a.last_name_match,
    a.ssn_match,
    a.dob_match,
    a.medicaid_match,
    a.index_first_name, 
    a.match_first_name, 
    a.index_last_name, 
    a.match_last_name,
    a.index_dob,
    a.match_dob,
    a.index_ssn,
    a.match_ssn,
    a.medicaid_id,
    a.medicaid_id2,
 	a.match_type,
    a.last_updated_mtn
from sorted_matches s left join all_matches a on a.index_case_id = s.index_case_id and a.match_case_id = s.match_case_id where opened_order = 1 and match_status = 'new'
)

select
	INDEX_CASE_ID,
	MATCH_CASE_ID,
	POTENTIAL_DUPLICATE_CASE_IDS,
	DUPLICATE_PRIMARY_CASE_ID,
	POTENTIAL_DUPLICATE_INDEX_CASE_IDS,
	NON_DUPLICATE_CASE_IDS,
	DUPLICATE_CASE_IDS,
	FIRST_NAME_MATCH,
	LAST_NAME_MATCH,
	SSN_MATCH,
	DOB_MATCH,
	MEDICAID_MATCH,
	INDEX_FIRST_NAME,
	MATCH_FIRST_NAME,
	INDEX_LAST_NAME,
	MATCH_LAST_NAME,
	INDEX_DOB,
	MATCH_DOB,
	INDEX_SSN,
	MATCH_SSN,
	MEDICAID_ID,
	MEDICAID_ID2,
	MATCH_TYPE,
	LAST_UPDATED_MTN
from final