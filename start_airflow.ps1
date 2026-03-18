# Script para iniciar Apache Airflow con el pipeline ETL
# Uso: .\start_airflow.ps1

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  Iniciando Apache Airflow" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

# Verificar que Docker esté corriendo
Write-Host "Verificando Docker..." -ForegroundColor Yellow
docker info > $null 2>&1
if ($LASTEXITCODE -ne 0) {
    Write-Host "ERROR: Docker no está corriendo" -ForegroundColor Red
    Write-Host "Por favor inicia Docker Desktop y vuelve a intentar" -ForegroundColor Red
    exit 1
}
Write-Host "✓ Docker está corriendo" -ForegroundColor Green
Write-Host ""

# Crear directorios necesarios
Write-Host "Creando directorios..." -ForegroundColor Yellow
$dirs = @("airflow_dags", "airflow_logs", "airflow_plugins")
foreach ($dir in $dirs) {
    if (-not (Test-Path $dir)) {
        New-Item -ItemType Directory -Path $dir | Out-Null
        Write-Host "✓ Creado: $dir" -ForegroundColor Green
    } else {
        Write-Host "✓ Existe: $dir" -ForegroundColor Green
    }
}
Write-Host ""

# Configurar UID para Airflow (solo necesario en Linux/Mac)
if ($IsLinux -or $IsMacOS) {
    Write-Host "Configurando AIRFLOW_UID..." -ForegroundColor Yellow
    "AIRFLOW_UID=50000" | Out-File -FilePath .env.airflow -Encoding utf8
    Write-Host "✓ AIRFLOW_UID configurado" -ForegroundColor Green
    Write-Host ""
}

# Verificar si Airflow ya está corriendo
Write-Host "Verificando servicios existentes..." -ForegroundColor Yellow
$running = docker compose -f docker-compose-airflow.yaml ps --services --filter "status=running" 2>$null
if ($running) {
    Write-Host "Airflow ya está corriendo. Deteniendo servicios..." -ForegroundColor Yellow
    docker compose -f docker-compose-airflow.yaml down
    Write-Host "✓ Servicios detenidos" -ForegroundColor Green
    Write-Host ""
}

# Inicializar Airflow (primera vez)
Write-Host "Inicializando Airflow..." -ForegroundColor Yellow
Write-Host "Esto puede tomar unos minutos la primera vez..." -ForegroundColor Gray
docker compose -f docker-compose-airflow.yaml up airflow-init
if ($LASTEXITCODE -ne 0) {
    Write-Host "ERROR: Falló la inicialización de Airflow" -ForegroundColor Red
    exit 1
}
Write-Host "✓ Airflow inicializado" -ForegroundColor Green
Write-Host ""

# Iniciar servicios
Write-Host "Iniciando servicios de Airflow..." -ForegroundColor Yellow
docker compose -f docker-compose-airflow.yaml up -d
if ($LASTEXITCODE -ne 0) {
    Write-Host "ERROR: Falló el inicio de servicios" -ForegroundColor Red
    exit 1
}
Write-Host "✓ Servicios iniciados" -ForegroundColor Green
Write-Host ""

# Esperar a que los servicios estén listos
Write-Host "Esperando a que los servicios estén listos..." -ForegroundColor Yellow
Start-Sleep -Seconds 10

$maxAttempts = 30
$attempt = 0
$ready = $false

while (-not $ready -and $attempt -lt $maxAttempts) {
    $attempt++
    try {
        $response = Invoke-WebRequest -Uri "http://localhost:8080/health" -TimeoutSec 2 -UseBasicParsing 2>$null
        if ($response.StatusCode -eq 200) {
            $ready = $true
        }
    } catch {
        Write-Host "." -NoNewline -ForegroundColor Gray
        Start-Sleep -Seconds 2
    }
}

Write-Host ""
if ($ready) {
    Write-Host "✓ Airflow está listo" -ForegroundColor Green
} else {
    Write-Host "⚠ Airflow está iniciando (puede tomar más tiempo)" -ForegroundColor Yellow
}
Write-Host ""

# Mostrar información de acceso
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  Airflow Iniciado Exitosamente" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "Acceso a la UI:" -ForegroundColor White
Write-Host "  URL:      " -NoNewline -ForegroundColor Gray
Write-Host "http://localhost:8080" -ForegroundColor Green
Write-Host "  Usuario:  " -NoNewline -ForegroundColor Gray
Write-Host "airflow" -ForegroundColor Green
Write-Host "  Password: " -NoNewline -ForegroundColor Gray
Write-Host "airflow" -ForegroundColor Green
Write-Host ""
Write-Host "Bases de datos:" -ForegroundColor White
Write-Host "  Airflow Metadata: " -NoNewline -ForegroundColor Gray
Write-Host "localhost:5434" -ForegroundColor Green
Write-Host "  Data Jobs:        " -NoNewline -ForegroundColor Gray
Write-Host "localhost:5433" -ForegroundColor Green
Write-Host ""
Write-Host "Próximos pasos:" -ForegroundColor White
Write-Host "  1. Abre http://localhost:8080 en tu navegador" -ForegroundColor Gray
Write-Host "  2. Inicia sesión con las credenciales de arriba" -ForegroundColor Gray
Write-Host "  3. Configura la conexión 'postgres_datajob' (ver airflow_setup.md)" -ForegroundColor Gray
Write-Host "  4. Activa el DAG 'datajob_etl_pipeline'" -ForegroundColor Gray
Write-Host ""
Write-Host "Comandos útiles:" -ForegroundColor White
Write-Host "  Ver logs:     " -NoNewline -ForegroundColor Gray
Write-Host "docker compose -f docker-compose-airflow.yaml logs -f" -ForegroundColor Cyan
Write-Host "  Detener:      " -NoNewline -ForegroundColor Gray
Write-Host "docker compose -f docker-compose-airflow.yaml down" -ForegroundColor Cyan
Write-Host "  Ver servicios:" -NoNewline -ForegroundColor Gray
Write-Host "docker compose -f docker-compose-airflow.yaml ps" -ForegroundColor Cyan
Write-Host ""
Write-Host "Documentación completa: airflow_setup.md" -ForegroundColor Yellow
Write-Host ""
