{{ config(
    materialized='table',
    indexes=[
        {'columns': ['job_posting_id']},
        {'columns': ['skill_name']},
        {'columns': ['type_name']},
        {'columns': ['skill_name', 'type_name']}
    ]
) }}

WITH source_data AS (
    SELECT * FROM {{ ref('stg_job_postings') }}
),

-- Descomponer job_skills array en filas individuales (sin tipo)
job_skills_unnested AS (
    SELECT
        id AS job_posting_id,
        TRIM(skill) AS skill_name,
        'direct' AS skill_source,
        NULL AS type_name
    FROM source_data, UNNEST (job_skills) AS skill
    WHERE
        job_skills IS NOT NULL
        AND skill IS NOT NULL
        AND TRIM(skill) != ''
),

-- Descomponer job_type_skills JSON en filas individuales (con tipo)
job_type_skills_unnested AS (
    SELECT
        id AS job_posting_id,
        TRIM(skill_value) AS skill_name,
        'typed' AS skill_source,
        type_key AS type_name
    FROM source_data,
    LATERAL (
        SELECT 
            key AS type_key,
            jsonb_array_elements_text(value::jsonb) AS skill_value
        FROM jsonb_each(job_type_skills::jsonb)
    ) AS skills_expanded
    WHERE
        job_type_skills IS NOT NULL
        AND jsonb_typeof(job_type_skills::jsonb) = 'object'
        AND TRIM(skill_value) != ''
)

-- Combine both skill sources and deduplicate
SELECT DISTINCT
    job_posting_id,
    LOWER(TRIM(skill_name)) AS skill_name,
    LOWER(TRIM(type_name)) AS type_name,
    skill_source
FROM (
        SELECT *
        FROM job_skills_unnested
        UNION ALL
        SELECT *
        FROM job_type_skills_unnested
    ) combined_skills
WHERE
    skill_name IS NOT NULL
    AND TRIM(skill_name) != ''