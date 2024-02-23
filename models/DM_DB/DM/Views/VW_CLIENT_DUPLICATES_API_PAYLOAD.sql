with matches as (select distinct INDEX_CASE_ID, MATCH_CASE_ID, potential_duplicate_index_case_ids, potential_duplicate_case_ids
                from DM.VW_CLIENT_DUPLICATES_INCREMENTAL_SIMPLE
) 
, closed_index as (select MATCH_CASE_ID, POTENTIAL_DUPLICATE_INDEX_CASE_IDS INDEX_CASE_ID, ifnull(CLOSED_FLAG,0) CLOSED_FLAG, 
					ifnull(MATCH_CLOSED_FLAG,0)::integer MATCH_CLOSED_FLAG
                   from DM.VW_CLIENT_POTENTIAL_DUPLICATE_INDEX_UPDATE
                   where nullif(trim(MATCH_CASE_ID),'') is not null and nullif(trim(POTENTIAL_DUPLICATE_INDEX_CASE_IDS),'') is not null
)
, closed_match as (select INDEX_CASE_ID, DUPLICATE_CASE_ID MATCH_CASE_ID, ifnull(CLOSED_FLAG,0) CLOSED_FLAG, ifnull(CLOSED_INDEX,0)::integer INDEX_CLOSED_FLAG 
                   from DM.VW_CLIENT_POTENTIAL_DUPLICATE_UPDATE
                   where nullif(trim(INDEX_CASE_ID),'') is not null and nullif(trim(DUPLICATE_CASE_ID),'') is not null
)
, all_matches as (
  select distinct INDEX_CASE_ID, MATCH_CASE_ID, 0 CLOSED_INDEX, 0 CLOSED_MATCH from matches --get new matches
  union
  select distinct INDEX_CASE_ID, a.value::string MATCH_CASE_ID, 0 CLOSED_INDEX, 0 CLOSED_MATCH 
    from matches, lateral flatten(input=> split(trim(potential_duplicate_case_ids), ' ')) a   --get existing match case ids
      where nullif(trim(potential_duplicate_case_ids), '') is not null and nullif(a.value::string,'') is not null
  union
  select distinct a.value::string INDEX_CASE_ID, MATCH_CASE_ID, 0 CLOSED_INDEX, 0 CLOSED_MATCH 
    from matches, lateral flatten(input=> split(trim(potential_duplicate_index_case_ids), ' ')) a --get existing index case ids
      where nullif(trim(potential_duplicate_index_case_ids), '') is not null and nullif(a.value::string,'') is not null
  union
  select INDEX_CASE_ID, MATCH_CASE_ID, CLOSED_FLAG CLOSED_INDEX, MATCH_CLOSED_FLAG CLOSED_MATCH from closed_index --get closed index cases
  union
  select INDEX_CASE_ID, MATCH_CASE_ID, INDEX_CLOSED_FLAG CLOSED_INDEX, CLOSED_FLAG CLOSED_MATCH from closed_match --get closed match cases
)
, distinct_indexes as (
  select distinct INDEX_CASE_ID, case when max(CLOSED_MATCH)=0 then MATCH_CASE_ID else null::string end MATCH_CASE_ID 
    from all_matches group by 1,all_matches.MATCH_CASE_ID --only bring match ids that aren't closed
)
, distinct_matches as (
  select distinct case when max(CLOSED_INDEX)=0 then INDEX_CASE_ID else null::string end INDEX_CASE_ID, MATCH_CASE_ID 
    from all_matches group by all_matches.INDEX_CASE_ID,2 --only bring index ids that aren't closed
)
, match_case_ids as ( --create the json payload below
    select object_construct('create', false, 'case_id', v.MATCH_CASE_ID, 'properties', 
                            object_construct('potential_duplicate_index_case_ids', 
                                                trim(listagg(v.INDEX_CASE_ID, ' ') --concatenate index cases for each matched case
                                                     within group(order by v.INDEX_CASE_ID asc nulls last),' ') --put empties at the end so they get trimmed out
                                            ) 
                           ) payload
  from distinct_matches v 
    group by v.MATCH_CASE_ID
)
, index_case_ids as ( --create the json payload below
    select object_construct('create', false, 'case_id', v.INDEX_CASE_ID, 'properties', 
                            object_construct('potential_duplicate_case_ids', 
                                                trim(listagg(v.MATCH_CASE_ID, ' ') --concatenate matched cases for each index case
                                                     within group(order by v.MATCH_CASE_ID asc nulls last),' ') --put empties at the end so they get trimmed out
                                            ) 
                           ) payload
  from distinct_indexes v 
    group by v.INDEX_CASE_ID
)
, payloads as ( --union two ctes above
  select payload from match_case_ids
  union
  select payload from index_case_ids
)
, numbered_payloads as ( --add row numbers and groupings of 100 at a time
  select row_number() over(order by payload) rownum, ceil(rownum/100) grouping, payload from payloads
),

final as (
select grouping, '[' || listagg(payload::string, ',') || ']' payload --concatenate payloads into arrays of up to 100
from numbered_payloads group by 1
)

select
	GROUPING,
	PAYLOAD
from final