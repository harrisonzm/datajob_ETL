{{ config(
    materialized='table',
    indexes=[
        {'columns': ['id'], 'unique': True},
        {'columns': ['company_name_clean']},
        {'columns': ['job_country_clean']},
        {'columns': ['job_location_clean']},
        {'columns': ['job_via_clean']},
        {'columns': ['job_schedule_type']},
        {'columns': ['job_title_short_clean']}
    ]
) }}


WITH source_data AS (
    SELECT * FROM {{ source('raw', 'job_posting') }}
),

cleaned_data AS (
    SELECT
        id,

-- Clean job_title_short
CASE
    WHEN TRIM(job_title_short) = '' THEN NULL
    ELSE TRIM(job_title_short)
END AS job_title_short_clean,

-- Clean job_title
CASE
    WHEN TRIM(job_title) = '' THEN NULL
    ELSE TRIM(
        REGEXP_REPLACE(
            job_title,
            '[^\w\s+().-]',
            '',
            'g'
        )
    )
END AS job_title_clean,

-- Clean job_location
CASE
    WHEN TRIM(job_location) = '' THEN NULL
    ELSE TRIM(
        REGEXP_REPLACE(
            job_location,
            '[^\w\s+().-]',
            '',
            'g'
        )
    )
END AS job_location_clean,

-- Clean job_via (remove prefixes)
CASE
    WHEN TRIM(job_via) = '' THEN NULL
    WHEN LOWER(TRIM(job_via)) LIKE 'via %' THEN TRIM(
        SUBSTRING(
            job_via
            FROM 5
        )
    )
    WHEN LOWER(TRIM(job_via)) LIKE 'melalui %' THEN TRIM(
        SUBSTRING(
            job_via
            FROM 9
        )
    )
    ELSE TRIM(
        REGEXP_REPLACE(
            job_via,
            '[^\w\s+().-]',
            '',
            'g'
        )
    )
END AS job_via_clean,

-- Standardize job_schedule_type
CASE
    WHEN TRIM(job_schedule_type) = '' THEN NULL
    ELSE TRIM(job_schedule_type)
END AS job_schedule_type,

-- Boolean fields (already processed in extraction)
job_work_from_home,

-- Clean search_location
CASE
    WHEN TRIM(search_location) = '' THEN NULL
    ELSE TRIM(search_location)
END AS search_location_clean,

-- Date field (already processed in extraction)
job_posted_date,

-- Boolean fields
job_no_degree_mention, job_health_insurance,

-- Clean job_country
CASE
    WHEN TRIM(job_country) = '' THEN NULL
    ELSE TRIM(job_country)
END AS job_country_clean,

-- Clean salary_rate
CASE
    WHEN TRIM(salary_rate) = '' THEN NULL
    ELSE TRIM(salary_rate)
END AS salary_rate_clean,

-- Numeric fields (already processed in extraction)
salary_year_avg, salary_hour_avg,

-- Clean company_name
CASE
    WHEN TRIM(company_name) = '' THEN NULL
    ELSE TRIM(
        REGEXP_REPLACE(
            company_name,
            '[^\w\s+().-]',
            '',
            'g'
        )
    )
END AS company_name_clean,

-- Arrays and JSON (already processed in extraction)
job_skills, job_type_skills FROM source_data )

SELECT * FROM cleaned_data