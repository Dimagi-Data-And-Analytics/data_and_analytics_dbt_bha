select *
from {{ref('VW_CLIENT_DUPLICATES')}}
where (FIRST_NAME_SCORE is not null and (FIRST_NAME_SCORE < 0 and FIRST_NAME_SCORE > 100))
   or (LAST_NAME_SCORE is not null and (LAST_NAME_SCORE < 0 and LAST_NAME_SCORE > 100))
   or (DOB_SCORE is not null and (DOB_SCORE < 0 and DOB_SCORE > 100))
   or (SSN_SCORE is not null and (SSN_SCORE < 0 and SSN_SCORE > 100))
   or (FIRST_NAME_SCORE_SOUNDEX is not null and (FIRST_NAME_SCORE_SOUNDEX < 0 and FIRST_NAME_SCORE_SOUNDEX > 100))
   or (LAST_NAME_SCORE_SOUNDEX is not null and (LAST_NAME_SCORE_SOUNDEX < 0 and LAST_NAME_SCORE_SOUNDEX > 100))
   or (FIRST_NAME_SCORE_JAR is not null and (FIRST_NAME_SCORE_JAR < 0 and FIRST_NAME_SCORE_JAR > 100))
   or (LAST_NAME_SCORE_JAR is not null and (LAST_NAME_SCORE_JAR < 0 and LAST_NAME_SCORE_JAR > 100))
   or (DOB_SCORE_JAR is not null and (DOB_SCORE_JAR < 0 and DOB_SCORE_JAR > 100))
   or (SSN_SCORE_JAR is not null and (SSN_SCORE_JAR < 0 and SSN_SCORE_JAR > 100))