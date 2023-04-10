/* 
  TASK:
  For each year, calculate the percentage of new and disenrolled person_id. 
  A disenrolled person is the person that had visits in the previous year but doesn't have visits in the current year. 
  A new person is a person that didn't have visits in the previous year but has visits in the current year.
*/
WITH cte AS (
  --this selects the unique combinations of person-provider-date
  SELECT
    DISTINCT person_id as person, provider_id as provider, procedure_dat as dates
  FROM `bigquery-public-data.cms_synthetic_patient_data_omop.procedure_occurrence`
  ORDER BY person_id DESC, provider_id DESC, dates DESC
),
pivoted as (
  /*
    it sums the count of visits to all providers for each year
    and groups the table by person
  */
  SELECT 
    person, 
    SUM(_2007) as y_2007, 
    SUM(_2008) as y_2008,
    SUM(_2009) as y_2009,
    SUM(_2010) as y_2010
  FROM
  (
    -- this extracts the year from the dates for pivoting purposes
    SELECT
      dates,
      provider,
      person, 
      EXTRACT(year FROM dates) AS years
    FROM cte
    GROUP BY person, provider, dates
    ORDER BY person DESC
  ) AS tmp
  PIVOT(
    /* 
      this pivots the table and adds four columns with years,
      then counts the visits of earch person to each provider for each year
    */
    COUNT(dates)
    FOR years IN (
      2007,
      2008,
      2009,
      2010
    )
  ) AS pivot_table
  GROUP BY person
),
diffed_pivot AS (
  /*
    this creates conditions for each person for being a new or disenrolled visitor
    NEW:
    - if the difference of their visits between two years is not equal to current year's visits or is equal 0, then they are not a new visitor
    - otherwise they are a new visitor

    DISENROLLED:
    - if the difference of their visit between two years is not equal to previous year's visits or is equal 0, then they are not a disenrolled visitor
    - otherwise they are disenrolled
  */
  SELECT
    person,
    y_2007,
    y_2008,
    y_2009,
    y_2010,
    CASE
      WHEN ((y_2008 - y_2007) != y_2008 OR (y_2008 - y_2007) = 0) THEN "no"
      ELSE "yes"
    END AS new_2008,
    CASE
      WHEN ((y_2009 - y_2008) != y_2009 OR (y_2009 - y_2008) = 0) THEN "no"
      ELSE "yes"
    END AS new_2009,
    CASE
      WHEN ((y_2010 - y_2009) != y_2010 OR (y_2010 - y_2009) = 0) THEN "no"
      ELSE "yes"
    END AS new_2010,
    CASE
      WHEN ((y_2007 - y_2008) != y_2007 OR (y_2007 - y_2008) = 0) THEN "no"
      ELSE "yes"
    END AS disenrolled_2008,
    CASE
      WHEN ((y_2008 - y_2009) != y_2008 OR (y_2008 - y_2009) = 0) THEN "no"
      ELSE "yes"
    END AS disenrolled_2009,
    CASE
      WHEN ((y_2009 - y_2010) != y_2009 OR (y_2009 - y_2010) = 0) THEN "no"
      ELSE "yes"
    END AS disenrolled_2010
  FROM pivoted
  GROUP BY person, y_2007, y_2008, y_2009, y_2010
)
-- this table calculates the percentages of new and disenrolled visitors for each year
SELECT
  ROUND(COUNTIF(new_2008='yes') / SUM(y_2008) * 100, 4) as new_2008,
  ROUND(COUNTIF(new_2009='yes') / SUM(y_2009) * 100, 4) as new_2009,
  ROUND(COUNTIF(new_2010='yes') / SUM(y_2010) * 100, 4) as new_2010,
  ROUND(COUNTIF(disenrolled_2008='yes') / SUM(y_2008) * 100, 4) as disenrolled_2008,
  ROUND(COUNTIF(disenrolled_2009='yes') / SUM(y_2009) * 100, 4) as disenrolled_2009,
  ROUND(COUNTIF(disenrolled_2010='yes') / SUM(y_2010) * 100, 4) as disenrolled_2010
FROM diffed_pivot