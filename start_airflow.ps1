# Script para iniciar Apache Airflow con el pipeline ETL
# Uso: .\start_airflow.ps1

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  Iniciando Apache Airflow" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

# Verificar que Docker este corriendo
Write-Host "Verificando Docker..." -ForegroundColor Yellow
docker info > $null 2>&1
if ($LASTEXITCODE -ne 0) {
    Write-Host "ERROR: Docker no esta corriendo" -ForegroundColor Red
    Write-Host "Por favor inicia Docker Desktop y vuelve a intentar" -ForegroundColor Red
    exit 1
}
Write-Host "OK Docker esta corriendo" -ForegroundColor Green
Write-Host ""

# Crear directorios necesarios
Write-Host "Creando directorios..." -ForegroundColor Yellow
$dirs = @("airflow_dags", "airflow_logs", "airflow_plugins")
foreach ($dir in $dirs) {
    if (-not (Test-Path $dir)) {
        New-Item -ItemType Directory -Path $dir | Out-Null
        Write-Host "OK Creado: $dir" -ForegroundColor Green
    }
    else {
        Write-Host "OK Existe: $dir" -ForegroundColor Green
    }
}
Write-Host ""

# Verificar si Airflow ya esta corriendo
Write-Host "Verificando servicios existentes..." -ForegroundColor Yellow
$running = docker compose ps --services --filter "status=running" 2>$null
if ($running) {
    Write-Host "Servicios ya corriendo. Deteniendo..." -ForegroundColor Yellow
    docker compose down
    Write-Host "OK Servicios detenidos" -ForegroundColor Green
    Write-Host ""
}

# Inicializar Airflow (primera vez)
Write-Host "Inicializando Airflow..." -ForegroundColor Yellow
Write-Host "Esto puede tomar unos minutos la primera vez..." -ForegroundColor Gray
docker compose up airflow-init
if ($LASTEXITCODE -ne 0) {
    Write-Host "ERROR: Fallo la inicializacion de Airflow" -ForegroundColor Red
    exit 1
}
Write-Host "OK Airflow inicializado" -ForegroundColor Green
Write-Host ""

# Iniciar servicios
Write-Host "Iniciando servicios de Airflow..." -ForegroundColor Yellow
docker compose up -d
if ($LASTEXITCODE -ne 0) {
    Write-Host "ERROR: Fallo el inicio de servicios" -ForegroundColor Red
    exit 1
}
Write-Host "OK Servicios iniciados" -ForegroundColor Green
Write-Host ""

# Esperar a que los servicios esten listos
Write-Host "Esperando a que los servicios esten listos..." -ForegroundColor Yellow
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
    }
    catch {
        Write-Host "." -NoNewline -ForegroundColor Gray
        Start-Sleep -Seconds 2
    }
}

Write-Host ""
if ($ready) {
    Write-Host "OK Airflow esta listo" -ForegroundColor Green
}
else {
    Write-Host "AVISO: Airflow esta iniciando (puede tomar mas tiempo)" -ForegroundColor Yellow
}
Write-Host ""

# Mostrar informacion de acceso
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
Write-Host "Proximos pasos:" -ForegroundColor White
Write-Host "  1. Abre http://localhost:8080 en tu navegador" -ForegroundColor Gray
Write-Host "  2. Inicia sesion con las credenciales de arriba" -ForegroundColor Gray
Write-Host "  3. Configura la conexion 'postgres_datajob' (ver airflow_setup.md)" -ForegroundColor Gray
Write-Host "  4. Activa el DAG 'datajob_etl_pipeline'" -ForegroundColor Gray
Write-Host ""
Write-Host "Comandos utiles:" -ForegroundColor White
Write-Host "  Ver logs:     " -NoNewline -ForegroundColor Gray
Write-Host "docker compose logs -f" -ForegroundColor Cyan
Write-Host "  Detener:      " -NoNewline -ForegroundColor Gray
Write-Host "docker compose down" -ForegroundColor Cyan
Write-Host "  Ver servicios:" -NoNewline -ForegroundColor Gray
Write-Host "docker compose ps" -ForegroundColor Cyan
Write-Host ""
Write-Host "Documentacion completa: airflow_setup.md" -ForegroundColor Yellow
Write-Host ""
