select username from {{ ref('VW_TABLEAU_DEDUPE_FILTER')}} where username not like 'HQ/%'