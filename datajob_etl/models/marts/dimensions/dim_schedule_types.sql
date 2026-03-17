{{ config(materialized='table') }}

WITH unique_schedule_types AS (
    SELECT DISTINCT 
        job_schedule_type_clean AS name
    FROM {{ ref('stg_job_postings') }}
    WHERE job_schedule_type_clean IS NOT NULL
)

SELECT ROW_NUMBER() OVER (
        ORDER BY name
    ) AS id, name
FROM unique_schedule_types
ORDER BY name