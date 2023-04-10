-- TASK: Output the distribution of the number of days between consecutive visits for each person_id to each provider_id
/*
  DEFINITION: 
    consecutive visit for each person to each provider is defined as visit by a person 
    - to THE SAME provider
    - on different dates
    - without a visit to another provider in between
    
    Example:
      person A visits provider B on date C
      on date D person A visits provider B again
      
      this is a consecutive visit of person A to provider B

    On the other hand:
      person A visits provider B on date C, then visits provider E on date F
      then returns to provider B on date G

      this is a non-consecutive visit of person A to provider B

  DISCLAIMER:

    I gave another solution to the problem with a different defition of consecutive visits
*/
WITH cte AS (
  -- this selects the unique combinations of person-provider-dates
  SELECT
    DISTINCT person_id as person, provider_id as provider, procedure_dat as dates
  FROM `bigquery-public-data.cms_synthetic_patient_data_omop.procedure_occurrence`
  ORDER BY person DESC, provider DESC, dates DESC
),
tmp AS (
  /*
    this table shifts the person, provider and dates columns from the cte one row up
    then puts them as separate columns
    in order to keep the entire table ordered in one way, person is used within OVER clause
  */
  SELECT
    person,
    provider,
    dates,
    LEAD(person) OVER (ORDER BY person DESC, provider DESC, dates DESC) as person_shifted,
    LEAD(provider) OVER (ORDER BY person DESC, provider DESC, dates DESC) as provider_shifted,
    LEAD(dates) OVER (ORDER BY person DESC, provider DESC, dates DESC) as dates_shifted
  FROM cte
  ORDER BY person DESC, provider DESC, dates DESC
),
tmp1 AS (
  /*
    it takes the difference between person and person_shifted
    and puts them as separate columns,
    same for providers

    because person and providers are just IDs, we can take their difference
    when the difference between them is 0, it means it is the same person (or provider)
  */
  SELECT
    person,
    provider,
    dates,
    dates_shifted,
    (person - person_shifted) as person_diff,
    (provider - provider_shifted) as provider_diff
  FROM tmp
  ORDER BY person DESC, provider DESC, dates DESC
),
tmp2 AS (
  /*
    this filters out all the rows where difference between person and person_shifted are not 0
    then takes difference between dates and dates_shifted to calculate how many days have passed
    since the last visit
  */
  SELECT
    person,
    provider,
    dates,
    DATE_DIFF(dates, dates_shifted, DAY) as dates_diff
  FROM tmp1
  WHERE person_diff = 0 AND provider_diff = 0
  ORDER BY person DESC, provider DESC, dates DESC
)
-- this counts the occurances of dates_diff
SELECT
  dates_diff,
  COUNT(dates_diff) as count_occurance
FROM tmp2
GROUP BY dates_diff
ORDER BY count_occurance DESC, dates_diff DESC