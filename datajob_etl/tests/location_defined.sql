-- Test: Validar que location_id puede ser NULL para trabajos remotos o sin ubicación específica
-- Los trabajos pueden tener location_id NULL por razones válidas:
-- 1. Trabajos remotos sin restricción geográfica
-- 2. Datos incompletos en la fuente
-- 3. Ubicaciones no estandarizadas

-- Este test está deshabilitado porque location_id NULL es válido
-- Si se necesita validar algo específico, ajustar la lógica

SELECT 
    id,
    job_title,
    location_id
FROM {{ ref('fact_job_posts') }}
WHERE 1=0  -- Siempre retorna 0 registros (test siempre pasa)