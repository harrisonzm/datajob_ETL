# Guía de Integración con Apache Airflow

## Instalación y Configuración

### 1. Inicializar Airflow

```powershell
# Crear directorios necesarios
mkdir airflow_dags, airflow_logs, airflow_plugins

# Iniciar todo el stack (DB + Airflow)
.\start_airflow.ps1
```

Este script automáticamente:
- Verifica Docker
- Crea directorios necesarios
- Inicializa Airflow
- Inicia PostgreSQL (datos + metadata de Airflow)
- Inicia Airflow webserver y scheduler

### 2. Acceder a la UI de Airflow

- URL: http://localhost:8080
- Usuario: `airflow`
- Password: `airflow`

### 3. Configurar Conexión a PostgreSQL

En la UI de Airflow:

1. Ve a Admin > Connections
2. Crea nueva conexión con estos datos:
   - Connection Id: `postgres_datajob`
   - Connection Type: `Postgres`
   - Host: `db`
   - Schema: `job_posting`
   - Login: `postgres`
   - Password: `postgres`
   - Port: `5432`

### 4. Activar el DAG

1. En la UI, busca `datajob_etl_pipeline`
2. Activa el toggle para habilitarlo
3. Puedes ejecutarlo manualmente con el botón "Play"

## Estructura del DAG

El DAG orquesta tu pipeline en 5 grupos de tareas:

```
validations (Validaciones)
    ├── check_csv (Verificar CSV existe)
    └── test_db_connection (Test conexión DB)
        ↓
extraction (Extracción)
    ├── run_extraction (Ejecutar extracción)
    └── verify_extraction (Verificar datos cargados)
        ↓
dbt_transformations (Transformaciones)
    ├── generate_dbt_profile (Generar profiles.yml)
    ├── dbt_deps (Instalar dependencias dbt)
    └── dbt_run (Ejecutar modelos dbt)
        ↓
quality_tests (Tests de Calidad)
    ├── dbt_test (Tests de dbt)
    └── pytest_tests (Tests de pytest)
        ↓
generate_dbt_docs (Documentación)
```

## Configuración del Schedule

Por defecto, el DAG corre diariamente a las 2 AM:

```python
schedule_interval='0 2 * * *'  # Cron expression
```

Puedes cambiarlo a:
- `'@daily'` - Diario a medianoche
- `'@weekly'` - Semanal
- `'@hourly'` - Cada hora
- `'0 */6 * * *'` - Cada 6 horas
- `None` - Solo manual

## Comandos Útiles

```powershell
# Ver logs de todos los servicios
docker compose logs -f

# Ver logs solo de Airflow
docker compose logs -f airflow-scheduler airflow-webserver

# Reiniciar servicios
docker compose restart

# Detener todo
docker compose down

# Detener y limpiar volúmenes
docker compose down -v

# Ver estado de servicios
docker compose ps

# Solo iniciar la base de datos
docker compose up -d db

# Solo iniciar Airflow
docker compose up -d airflow-webserver airflow-scheduler
```

## Monitoreo y Alertas

### Email Notifications

Configura SMTP en `compose.yaml` agregando estas variables al environment de airflow-common:

```yaml
AIRFLOW__SMTP__SMTP_HOST: smtp.gmail.com
AIRFLOW__SMTP__SMTP_PORT: 587
AIRFLOW__SMTP__SMTP_USER: tu-email@gmail.com
AIRFLOW__SMTP__SMTP_PASSWORD: tu-app-password
AIRFLOW__SMTP__SMTP_MAIL_FROM: airflow@company.com
```

### Slack Notifications

Instala el provider:

```bash
docker compose exec airflow-webserver pip install apache-airflow-providers-slack
```

Agrega callback en el DAG:

```python
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator

def slack_alert(context):
    slack_msg = f"""
    :red_circle: Task Failed
    *Task*: {context.get('task_instance').task_id}
    *Dag*: {context.get('task_instance').dag_id}
    *Execution Time*: {context.get('execution_date')}
    """
    return SlackWebhookOperator(
        task_id='slack_alert',
        http_conn_id='slack_webhook',
        message=slack_msg
    ).execute(context=context)

default_args = {
    'on_failure_callback': slack_alert,
}
```

## Troubleshooting

### Error: "Permission denied"

```powershell
# En Windows, asegúrate de que Docker tenga permisos
# En Linux/Mac:
sudo chown -R 50000:0 airflow_logs airflow_dags airflow_plugins
```

### Error: "Module not found"

El DAG necesita acceso a tu código. Verifica que el volumen esté montado en `compose.yaml`:

```yaml
volumes:
  - .:/opt/airflow/dags/datajob_etl
```

### Error: "Connection refused"

Verifica que las bases de datos estén corriendo:

```powershell
docker compose ps
```

## Ventajas de esta Integración

✅ Orquestación automática del pipeline completo
✅ Scheduling flexible (diario, semanal, etc.)
✅ Monitoreo visual en tiempo real
✅ Reintentos automáticos en caso de fallo
✅ Alertas por email/Slack
✅ Historial de ejecuciones
✅ Logs centralizados
✅ Paralelización de tareas independientes
✅ Gestión de dependencias entre tareas

## Próximos Pasos

1. Personaliza el schedule según tus necesidades
2. Configura alertas (email/Slack)
3. Agrega sensores para detectar nuevos archivos
4. Implementa backfilling para datos históricos
5. Configura variables de Airflow para parametrizar el DAG
