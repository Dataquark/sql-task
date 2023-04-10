-- TASK: Find the top 3 provider_id for each person_id by the number of visits using all available data
WITH cte AS (
  -- this will select the unique combinatons of person, provider and dates
  SELECT
    DISTINCT person_id as person, provider_id as provider, procedure_dat as dates
  FROM `bigquery-public-data.cms_synthetic_patient_data_omop.procedure_occurrence`
  ORDER BY person_id DESC, provider_id DESC, dates DESC
),
all_visit_count AS (
  /*
    this partitions the data into person-provider chunks ordered by provider
    then it counts the person for each chunk
  */
  SELECT
    person,
    provider,
    dates,
    COUNT(person) OVER (PARTITION BY person, provider ORDER BY provider DESC) as visit_count
  FROM cte
  ORDER BY person DESC
),
distinct_visit_count AS (
  -- this selects the distinct combinations of person-provider-visit_count from the previous table
  SELECT 
    DISTINCT person, provider, visit_count
  FROM all_visit_count
  ORDER BY person DESC, visit_count DESC
)
-- this final call selects the top 3 provider_id for each person_id by the number of visits
SELECT
  * 
FROM (
      -- this subquery adds sequential integers to each chunk from the previous table
      SELECT 
        ROW_NUMBER() OVER (PARTITION BY person ORDER BY visit_count DESC) as rank_order,
        *
      FROM distinct_visit_count
      ORDER BY person DESC)
WHERE rank_order <=3
ORDER BY person DESC
