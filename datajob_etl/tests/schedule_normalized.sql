-- Test: Validar que schedule_types fueron normalizados
-- "Pekerjaan tetap" (indonesio) se normaliza a "Full-time"
-- "Per diem" se mantiene como está (término internacional aceptado)
-- Este test verifica que NO existan valores sin normalizar

SELECT 
    id,
    name
FROM {{ ref('dim_schedule_types') }}
WHERE 
    -- Buscar solo "pekerjaan tetap" sin normalizar (ya no debería existir)
    LOWER(name) = 'pekerjaan tetap'
    OR name LIKE '%pekerjaan%'
    OR name LIKE '%per diem%'