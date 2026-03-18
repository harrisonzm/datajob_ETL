"""
DAG de Airflow para el pipeline ETL de Data Jobs
Orquesta: Extracción -> Transformación (dbt) -> Tests de Calidad
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.task_group import TaskGroup
import sys
import os

# Agregar el directorio del proyecto al path
PROJECT_ROOT = os.getenv('DATAJOB_ETL_PATH', '/opt/airflow/dags/datajob_etl')
sys.path.insert(0, PROJECT_ROOT)

# Argumentos por defecto del DAG
default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'email': ['data-team@company.com'],
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=2),
}

# Definir el DAG
dag = DAG(
    'datajob_etl_pipeline',
    default_args=default_args,
    description='Pipeline ETL completo para análisis de empleos',
    schedule_interval='0 2 * * *',  # Diario a las 2 AM
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['etl', 'data-jobs', 'analytics'],
    max_active_runs=1,
)

# ============================================================================
# FUNCIONES PYTHON PARA LAS TAREAS
# ============================================================================

def check_csv_exists(**context):
    """Verifica que el archivo CSV exista"""
    csv_path = os.path.join(PROJECT_ROOT, 'data_jobs.csv')
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"CSV no encontrado: {csv_path}")
    
    # Obtener tamaño del archivo
    size_mb = os.path.getsize(csv_path) / (1024 * 1024)
    print(f"✓ CSV encontrado: {csv_path}")
    print(f"✓ Tamaño: {size_mb:.2f} MB")
    
    # Guardar en XCom para otras tareas
    context['ti'].xcom_push(key='csv_path', value=csv_path)
    context['ti'].xcom_push(key='csv_size_mb', value=size_mb)

def test_db_connection(**context):
    """Verifica la conexión a PostgreSQL"""
    hook = PostgresHook(postgres_conn_id='postgres_datajob')
    conn = hook.get_conn()
    cursor = conn.cursor()
    
    cursor.execute("SELECT version();")
    version = cursor.fetchone()
    print(f"✓ Conexión exitosa a PostgreSQL")
    print(f"✓ Versión: {version[0]}")
    
    cursor.close()
    conn.close()

def run_extraction(**context):
    """Ejecuta el módulo de extracción"""
    from extraction.extraction import execute_extraction
    
    csv_path = context['ti'].xcom_pull(key='csv_path', task_ids='check_csv')
    print(f"Iniciando extracción desde: {csv_path}")
    
    # Ejecutar extracción (usa método optimizado por defecto)
    success = execute_extraction(csv_path, use_copy=False)
    
    if not success:
        raise Exception("Extracción falló")
    
    print("✓ Extracción completada exitosamente")

def verify_extraction(**context):
    """Verifica que los datos se hayan cargado correctamente"""
    hook = PostgresHook(postgres_conn_id='postgres_datajob')
    
    # Contar registros
    result = hook.get_first("SELECT COUNT(*) FROM job_posting")
    count = result[0]
    
    print(f"✓ Registros en job_posting: {count:,}")
    
    # Verificar que haya datos
    if count == 0:
        raise Exception("No se cargaron datos en la tabla")
    
    # Guardar métricas
    context['ti'].xcom_push(key='records_loaded', value=count)

def generate_dbt_profile(**context):
    """Genera el archivo profiles.yml optimizado"""
    from utils.generate_dbt_profile import generate_profile
    
    print("Generando profiles.yml optimizado...")
    generate_profile()
    print("✓ profiles.yml generado")

# ============================================================================
# DEFINICIÓN DE TAREAS
# ============================================================================

# Grupo: Validaciones iniciales
with TaskGroup('validations', dag=dag) as validations:
    
    check_csv = PythonOperator(
        task_id='check_csv',
        python_callable=check_csv_exists,
        provide_context=True,
    )
    
    test_db = PythonOperator(
        task_id='test_db_connection',
        python_callable=test_db_connection,
        provide_context=True,
    )
    
    check_csv >> test_db

# Grupo: Extracción
with TaskGroup('extraction', dag=dag) as extraction:
    
    run_extract = PythonOperator(
        task_id='run_extraction',
        python_callable=run_extraction,
        provide_context=True,
        execution_timeout=timedelta(minutes=30),
    )
    
    verify_extract = PythonOperator(
        task_id='verify_extraction',
        python_callable=verify_extraction,
        provide_context=True,
    )
    
    run_extract >> verify_extract

# Grupo: Transformaciones dbt
with TaskGroup('dbt_transformations', dag=dag) as dbt_transformations:
    
    gen_profile = PythonOperator(
        task_id='generate_dbt_profile',
        python_callable=generate_dbt_profile,
        provide_context=True,
    )
    
    dbt_deps = BashOperator(
        task_id='dbt_deps',
        bash_command=f'pip install dbt-postgres==1.7.4 && cd {PROJECT_ROOT}/datajob_etl && dbt deps',
    )
    
    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command=f'cd {PROJECT_ROOT}/datajob_etl && dbt run',
        execution_timeout=timedelta(minutes=20),
    )
    
    gen_profile >> dbt_deps >> dbt_run

# Grupo: Tests de calidad
with TaskGroup('quality_tests', dag=dag) as quality_tests:
    
    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command=f'cd {PROJECT_ROOT}/datajob_etl && dbt test',
        execution_timeout=timedelta(minutes=10),
    )
    
    pytest_tests = BashOperator(
        task_id='pytest_tests',
        bash_command=f'pip install pytest pandas==2.1.4 && cd {PROJECT_ROOT} && python -m pytest tests/test_extraction.py -v',
    )
    
    [dbt_test, pytest_tests]

# Tarea final: Generar documentación
generate_docs = BashOperator(
    task_id='generate_dbt_docs',
    bash_command=f'cd {PROJECT_ROOT}/datajob_etl && dbt docs generate',
    dag=dag,
)

# ============================================================================
# FLUJO DEL DAG
# ============================================================================

validations >> extraction >> dbt_transformations >> quality_tests >> generate_docs
