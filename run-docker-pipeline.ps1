# ============================================================================
# Script para ejecutar el pipeline ETL en Docker
# ============================================================================

$ErrorActionPreference = "Stop"

Write-Host "`n=== PIPELINE ETL EN DOCKER ===" -ForegroundColor Cyan
Write-Host "Ejecutando pipeline completo en contenedores Docker`n" -ForegroundColor Gray

# Verificar prerequisitos
if (-not (Test-Path "data_jobs.csv")) {
    Write-Host "ERROR: data_jobs.csv no encontrado en el directorio raíz" -ForegroundColor Red
    exit 1
}

# Verificar Docker
try {
    docker --version >$null 2>&1
    if ($LASTEXITCODE -ne 0) {
        Write-Host "ERROR: Docker no está instalado" -ForegroundColor Red
        exit 1
    }
} catch {
    Write-Host "ERROR: Docker no está instalado" -ForegroundColor Red
    exit 1
}

Write-Host "✓ Prerequisitos verificados" -ForegroundColor Green

# Limpiar contenedores anteriores
Write-Host "`nLimpiando contenedores anteriores..." -ForegroundColor Yellow
docker compose -f docker-compose.pipeline.yml down -v >$null 2>&1

# Construir y ejecutar
Write-Host "Construyendo imagen del pipeline..." -ForegroundColor Yellow
docker compose -f docker-compose.pipeline.yml build

if ($LASTEXITCODE -ne 0) {
    Write-Host "ERROR: Falló la construcción de la imagen" -ForegroundColor Red
    exit 1
}

Write-Host "✓ Imagen construida exitosamente" -ForegroundColor Green

Write-Host "`nEjecutando pipeline ETL..." -ForegroundColor Yellow
docker compose -f docker-compose.pipeline.yml up --abort-on-container-exit

$pipelineResult = $LASTEXITCODE

# Mostrar logs si hay error
if ($pipelineResult -ne 0) {
    Write-Host "`nERROR: El pipeline falló. Mostrando logs..." -ForegroundColor Red
    docker compose -f docker-compose.pipeline.yml logs etl-pipeline
} else {
    Write-Host "`n✓ Pipeline completado exitosamente" -ForegroundColor Green
}

# Limpiar
Write-Host "`nLimpiando contenedores..." -ForegroundColor Gray
docker compose -f docker-compose.pipeline.yml down

if ($pipelineResult -eq 0) {
    Write-Host "`n=== PIPELINE COMPLETADO ===" -ForegroundColor Green
    Write-Host "Los logs están disponibles en la carpeta ./logs/" -ForegroundColor Gray
} else {
    Write-Host "`n=== PIPELINE FALLÓ ===" -ForegroundColor Red
    exit 1
}