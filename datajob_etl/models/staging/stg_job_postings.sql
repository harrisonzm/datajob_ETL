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

-- Clean job_via (remove prefixes like "Via -", "Melalui -", etc.)
CASE
    WHEN TRIM(job_via) = '' THEN NULL
    -- Si contiene " - ", eliminar todo hasta después del " - "
    WHEN job_via LIKE '%-%' THEN TRIM(
        SUBSTRING(
            job_via
            FROM POSITION(' - ' IN job_via) + 3
        )
    )
    -- Si empieza con "via " (case insensitive), eliminar el prefijo
    WHEN LOWER(TRIM(job_via)) LIKE 'via %' THEN TRIM(
        SUBSTRING(
            job_via
            FROM 5
        )
    )
    -- Si empieza con "melalui " (indonesio para "via"), eliminar el prefijo
    WHEN LOWER(TRIM(job_via)) LIKE 'melalui %' THEN TRIM(
        SUBSTRING(
            job_via
            FROM 9
        )
    )
    -- Limpiar caracteres especiales no deseados
    ELSE TRIM(
        REGEXP_REPLACE(
            job_via,
            '[^\w\s+().-]',
            '',
            'g'
        )
    )
END AS job_via_clean,

-- Standardize job_schedule_type (normalize specific cases found in data)
CASE
    WHEN TRIM(job_schedule_type) = '' THEN NULL
    -- "Pekerjaan tetap" (indonesio) = Full-time
    WHEN LOWER(TRIM(job_schedule_type)) = 'pekerjaan tetap' THEN 'Full-time'
    -- "Per diem" se mantiene como está (es un término aceptado internacionalmente)
    WHEN LOWER(TRIM(job_schedule_type)) = 'per diem' THEN 'On Demand'
    -- Mantener otros valores como están
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

-- Clean salary_rate: solo mantener si tiene valor correspondiente
CASE
    WHEN LOWER(TRIM(salary_rate)) = 'year'
    AND salary_year_avg IS NULL THEN NULL
    -- Si es 'hour' pero no hay salary_hour_avg, poner NULL
    WHEN LOWER(TRIM(salary_rate)) = 'hour'
    AND salary_hour_avg IS NULL THEN NULL
    -- Si es 'week' pero no hay salary_year_avg ni salary_hour_avg, poner NULL
    WHEN LOWER(TRIM(salary_rate)) != 'hour'
    AND LOWER(TRIM(salary_rate)) != 'year' THEN NULL
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