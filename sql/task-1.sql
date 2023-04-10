-- TASK: Output the date of 1st visit to each provider_id by person_id for each year
WITH cte AS (
  -- this will select the unique combintaions of person, provider and dates
  SELECT
    DISTINCT person_id as person, provider_id as provider, procedure_dat as dates,
    EXTRACT(year FROM procedure_dat) as years
  FROM `bigquery-public-data.cms_synthetic_patient_data_omop.procedure_occurrence`
  ORDER BY person_id DESC, provider_id DESC, dates ASC
),
tmp AS (
  /* 
    this partitions the table into person-provider-year chunks
    then orders the partitions by dates in ascending order
    then takes the first date from each chunk
  */
  SELECT
    person,
    provider,
    years,
    FIRST_VALUE(dates) OVER (PARTITION BY person, provider, years ORDER BY dates ASC) as first_visit,
    dates  
  FROM cte
  ORDER BY person DESC, provider DESC, years DESC
)
-- this returns the date of 1st visit to each provider_id by person_id for each year
SELECT 
  DISTINCT person, provider, years, first_visit
FROM tmp
ORDER BY person DESC, provider DESC, years DESC;