-- TASK: Output the distribution of the number of days between consecutive visits for each person_id to each provider_id
/*
  DEFINITION: 
    consecutive visit for each person to each provider is defined as a visit by a person 
    - to each provider (same or different than the first visit)
    - on different dates
    
    Example:
      person A visits provider B on date C, then visits provider D (or B) on date E
      this is a consecutive visit of person A to different (or the same) providers on different dates
*/
WITH cte AS (
  -- this selects the unique combinations of person-provider-dates
  SELECT
    DISTINCT person_id as person, procedure_dat as dates, provider_id as provider
  FROM `bigquery-public-data.cms_synthetic_patient_data_omop.procedure_occurrence`
  ORDER BY person DESC, dates DESC, provider DESC
),
tmp AS (
  /*
    this table shifts the person and dates columns from the cte one row up
    then puts them as separate columns
  */
  SELECT
    person,
    dates,
    LEAD(person) OVER (ORDER BY person DESC, dates DESC) as person_shifted,
    LEAD(dates) OVER (ORDER BY person DESC, dates DESC) as dates_shifted
  FROM cte
  ORDER BY person DESC, dates DESC
),
tmp1 AS (
  /*
    it takes the difference between person and person_shifted
    and puts them as separate columns

    because person is just IDs, we can take it's difference
    when the difference is 0, it means it is the same person

    it also takes the difference of days between two visits
  */
  SELECT
    person,
    dates,
    dates_shifted,
    (person - person_shifted) as person_diff,
    DATE_DIFF(dates, dates_shifted, DAY) as dates_diff
  FROM tmp
  ORDER BY person DESC, dates DESC
)
-- this outputs the distribution of the number of days between consecutive visits for each person_id to each provider_id
-- we put person_diff = 0 condition to ensure that visit dates of two different visitors are not being differenced 
SELECT 
  dates_diff, 
  COUNT(dates_diff) as count_occurrence 
FROM tmp1 
WHERE person_diff = 0
GROUP BY dates_diff
ORDER BY count_occurrence DESC