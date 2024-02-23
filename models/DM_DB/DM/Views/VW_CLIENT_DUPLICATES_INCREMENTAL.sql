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
        social_security_number, 
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

case_search as (
    select
        l1.case_id as case_id,
        l2.case_id as matched_case_id,
        l1.social_security_number as ssn,
        l2.social_security_number as ssn2,
        l1.dob as dob_display1,
        l2.dob as dob_display2,
        l1.potential_duplicate_case_ids,
        l1.duplicate_primary_case_id,
        l2.potential_duplicate_index_case_ids,
        l1.non_duplicate_case_ids,
        l1.duplicate_case_ids,
        REGEXP_REPLACE(UPPER(l1.dob), '[^A-Z0-9 ]', '') as dob1,
        REGEXP_REPLACE(UPPER(l2.dob), '[^A-Z0-9 ]', '') as dob2,
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
        sort_client l1
        --DM.CASE_CLIENT l1 -- index case
        inner join dm_table_data_client l2 -- match case
    where
        not array_contains(l2.case_id::variant, split(ifnull(l1.potential_duplicate_case_ids,''),' '))
        and not array_contains(l1.case_id::variant, split(ifnull(l2.potential_duplicate_index_case_ids,''),' '))
--        and l1.case_id not in l2.potential_duplicate_index_case_ids
--        and l1.closed = 0
        and l2.closed = 0
        and l1.case_id < l2.case_id -- only scan through table from one side
        and l1.case_id <> l2.case_id -- we are looking for duplicates so make sure Case IDs don't match
        and -- make sure dob matches or dob month/day, month/year, day/year match.  I've seen the year, day, or month off on some but everything else is the same.
        (
            l1.dob = l2.dob
            or (
                (
                    month(to_date(l1.dob)) = month(to_date(l2.dob))
                )
                and day(to_date(l1.dob)) = day(to_date(l2.dob))
            )
            or (
                (
                    month(to_date(l1.dob)) = month(to_date(l2.dob))
                )
                and year(to_date(l1.dob)) = year(to_date(l2.dob))
            )
            or 
            (
                (
                    day(to_date(l1.dob)) = day(to_date(l2.dob))
                )
                and year(to_date(l1.dob)) = year(to_date(l2.dob))
            )
        )
),

alias_search as (
    select
        l1.case_id as case_id,
        l2.parent_case_id as matched_case_id,
        l1.social_security_number as ssn,
        l3.social_security_number as ssn2,
        l1.dob as dob_display1,
        l3.dob as dob_display2,
        l1.potential_duplicate_case_ids,
        l1.duplicate_primary_case_id,
        l3.potential_duplicate_index_case_ids,
        l1.non_duplicate_case_ids,
        l1.duplicate_case_ids,
        REGEXP_REPLACE(UPPER(l1.dob), '[^A-Z0-9 ]', '') as dob1,
        REGEXP_REPLACE(UPPER(l3.dob), '[^A-Z0-9 ]', '') as dob2,
        REGEXP_REPLACE(UPPER(l1.first_name), '[^A-Z0-9 ]', '') as first_name1,
        REGEXP_REPLACE(UPPER(l1.last_name), '[^A-Z0-9 ]', '') as last_name1,
        REGEXP_REPLACE(UPPER(l2.first_name), '[^A-Z0-9 ]', '') as first_name2,
        REGEXP_REPLACE(UPPER(l2.last_name), '[^A-Z0-9 ]', '') as last_name2,
    	l1.first_name as index_first_name, 
    	l1.last_name as index_last_name, 
    	l2.first_name as match_first_name, 
    	l2.last_name as match_last_name, 
    	'Alias Match' as match_type,
        l1.last_updated 
    	
    from
        
        sort_client l1
        
        cross join dm_table_data_alias l2 
    	inner join dm_table_data_client l3 on l2.parent_case_id = l3.case_id

    	
    where
        not array_contains(l3.case_id::variant, split(ifnull(l1.potential_duplicate_case_ids,''),' '))
        and not array_contains(l1.case_id::variant, split(ifnull(l3.potential_duplicate_index_case_ids,''),' '))

        and l2.closed = 0
    	and l3.closed = 0
        and l1.case_id < l2.parent_case_id -- only scan through table from one side
        and l1.case_id <> l3.case_id -- we are looking for duplicates so make sure Case IDs don't match
        and -- make sure dob matches or dob month/day, month/year, day/year match.  I've seen the year, day, or month off on some but everything else is the same.
        (
            l1.dob = l3.dob
            or (
                (
                    month(to_date(l1.dob)) = month(to_date(l3.dob))
                )
                and day(to_date(l1.dob)) = day(to_date(l3.dob))
            )
            or (
                (
                    month(to_date(l1.dob)) = month(to_date(l3.dob))
                )
                and year(to_date(l1.dob)) = year(to_date(l3.dob))
            )
            or (
                (
                    day(to_date(l1.dob)) = day(to_date(l3.dob))
                )
                and year(to_date(l1.dob)) = year(to_date(l3.dob))
            )
        )
),

final as (
select * from(

(select
    case_id as index_case_id,
    matched_case_id as match_case_id,
    potential_duplicate_case_ids,
    duplicate_primary_case_id,
    potential_duplicate_index_case_ids,
    non_duplicate_case_ids,
    duplicate_case_ids,

    round(
        100 * (
            1.0 -(
                editdistance(first_name1, first_name2) / greatest(length(first_name1), length(first_name2))
            )
        ),
        0
    ) as first_name_score,
    round(
        100 * (
            1.0 -(
                editdistance(last_name1, last_name2) / greatest(length(last_name1), length(last_name2))
            )
        ),
        0
    ) as last_name_score,
    

    round(
        100 * (
            1.0 -(
                editdistance(dob1, dob2) / greatest(length(dob1), length(dob2))
            )
        ),
        0
    ) as dob_score,
    round(
        100 * (
            1.0 -(
                editdistance(ssn, ssn2) / greatest(length(ssn), length(ssn2))
            )
        ),
        0
    ) as ssn_score,
   
 
     round(100* (1.0 -(
        editdistance(soundex(first_name1), soundex(first_name2)) / greatest(
            length(soundex(first_name1)),
            length(soundex(first_name2))
        )
     )), 0) as first_name_score_soundex,
     round(100* (1.0 -(
         editdistance(soundex(last_name1), soundex(last_name2)) / greatest(
             length(soundex(last_name1)),
             length(soundex(last_name2))
         )
     )), 0) as last_name_score_soundex,

    --adding a third type that scores 0 to 100, 100 being exact match
    JAROWINKLER_SIMILARITY(first_name1, first_name2) as first_name_score_jar,
    JAROWINKLER_SIMILARITY(last_name1, last_name2) as last_name_score_jar,
    JAROWINKLER_SIMILARITY(dob1, dob2) as dob_score_jar,
    JAROWINKLER_SIMILARITY(ssn, ssn2) as ssn_score_jar,
    
    --adding details to validate the scores
   index_first_name, 
   match_first_name, 
   index_last_name, 
   match_last_name,
    to_date(dob_display1) as index_dob,
    to_date(dob_display2) as match_dob,
    ssn as index_ssn,
    ssn2 as match_ssn,
 	match_type,
    convert_timezone('UTC', 'America/Denver',last_updated) as last_updated_mtn
from
    case_search
where
-- blocking so we aren't pulling in low score matches (non matches)
    (
        -- editdistance fn ln
       ( first_name_score >= 75 and last_name_score >= 75)
     
        or
       -- jarowinkler fn ln 
       (first_name_score_jar >=75 and last_name_score_jar >=75 and ssn_score_jar >=75)
        or 
        -- editdistance ssn
        (ssn_score = 100)
        or 
        -- jarowinkler ssn
        (ssn_score_jar = 100)
        or
        -- soundex fn ln
        (first_name_score_soundex >= 75 and last_name_score_soundex >=75)
        
        
    )
    and case_id <> matched_case_id 
    and (contains(non_duplicate_case_ids, match_case_id) = FALSE or non_duplicate_case_ids is null)
    order by case_id)
    
   UNION
   
   (select
    case_id as index_case_id,
    matched_case_id as match_case_id,
    potential_duplicate_case_ids,
    duplicate_primary_case_id,
    potential_duplicate_index_case_ids,
    non_duplicate_case_ids,
    duplicate_case_ids,
       --Takes two strings and returns a similarity score between 0 and 100. 100 being exact match, 0 being no match.
    round(
        100 * (
            1.0 -(
                editdistance(first_name1, first_name2) / greatest(length(first_name1), length(first_name2))
            )
        ),
        0
    ) as first_name_score,
    round(
        100 * (
            1.0 -(
                editdistance(last_name1, last_name2) / greatest(length(last_name1), length(last_name2))
            )
        ),
        0
    ) as last_name_score,
    
    --added DOB check because I noticed dups that had the same ssn and month/day for dob but not the same year
    round(
        100 * (
            1.0 -(
                editdistance(dob1, dob2) / greatest(length(dob1), length(dob2))
            )
        ),
        0
    ) as dob_score,
    round(
        100 * (
            1.0 -(
                editdistance(ssn, ssn2) / greatest(length(ssn), length(ssn2))
            )
        ),
        0
    ) as ssn_score,
   
    --now use editdistance and soundex to score first and last name phonetics only. Not useful for dob and ssn.
     round(100* (1.0 -(
        editdistance(soundex(first_name1), soundex(first_name2)) / greatest(
            length(soundex(first_name1)),
            length(soundex(first_name2))
        )
     )), 0) as first_name_score_soundex,
     round(100* (1.0 -(
         editdistance(soundex(last_name1), soundex(last_name2)) / greatest(
             length(soundex(last_name1)),
             length(soundex(last_name2))
         )
     )), 0) as last_name_score_soundex,

    --adding a third type that scores 0 to 100, 100 being exact match
    JAROWINKLER_SIMILARITY(first_name1, first_name2) as first_name_score_jar,
    JAROWINKLER_SIMILARITY(last_name1, last_name2) as last_name_score_jar,
    JAROWINKLER_SIMILARITY(dob1, dob2) as dob_score_jar,
    JAROWINKLER_SIMILARITY(ssn, ssn2) as ssn_score_jar,
    
    --adding details to validate the scores
   index_first_name, 
   match_first_name, 
   index_last_name, 
   match_last_name,
    to_date(dob_display1) as index_dob,
    to_date(dob_display2) as match_dob,
    ssn as index_ssn,
    ssn2 as match_ssn,
 	match_type,
    convert_timezone('UTC', 'America/Denver',last_updated) as last_updated_mtn
    
from
    alias_search
where
   
    (
        -- editdistance fn ln
       ( first_name_score >= 75 and last_name_score >= 75)
     
        or
       -- jarowinkler fn ln 
       (first_name_score_jar >=75 and last_name_score_jar >=75 and ssn_score_jar >=75)
        or 
        -- editdistance ssn
        (ssn_score = 100)
        or 
        -- jarowinkler ssn
        (ssn_score_jar = 100)
        or
        -- soundex fn ln
        (first_name_score_soundex >= 75 and last_name_score_soundex >=75)
        
        
    )
    and case_id <> matched_case_id
    and (contains(non_duplicate_case_ids, match_case_id) = FALSE or non_duplicate_case_ids is null) -- using index non_duplcate_case_ids
    order by case_id))
    order by index_case_id
)

select 
	INDEX_CASE_ID,
	MATCH_CASE_ID,
	POTENTIAL_DUPLICATE_CASE_IDS,
	DUPLICATE_PRIMARY_CASE_ID,
	POTENTIAL_DUPLICATE_INDEX_CASE_IDS,
	NON_DUPLICATE_CASE_IDS,
	DUPLICATE_CASE_IDS,
	FIRST_NAME_SCORE,
	LAST_NAME_SCORE,
	DOB_SCORE,
	SSN_SCORE,
	FIRST_NAME_SCORE_SOUNDEX,
	LAST_NAME_SCORE_SOUNDEX,
	FIRST_NAME_SCORE_JAR,
	LAST_NAME_SCORE_JAR,
	DOB_SCORE_JAR,
	SSN_SCORE_JAR,
	INDEX_FIRST_NAME,
	MATCH_FIRST_NAME,
	INDEX_LAST_NAME,
	MATCH_LAST_NAME,
	INDEX_DOB,
	MATCH_DOB,
	INDEX_SSN,
	MATCH_SSN,
	MATCH_TYPE,
	LAST_UPDATED_MTN
from final