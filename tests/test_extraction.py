import pytest
import pandas as pd
import os
import logging
from datetime import datetime
from sqlalchemy import text
from db.config.db import engine
from extraction.extraction import clean_df, parse_skills_string, parse_type_skills_string

# Configurar logging para tests
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('logs/test_extraction.log', encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)

class TestExtraction:
    """Tests para el proceso de extracción de datos"""
    
    def test_source_file_accessibility(self):
        """Verificar que el archivo fuente está disponible"""
        logger.info("TEST: Verificando accesibilidad del archivo fuente")
        file_path = "data_jobs.csv"
        exists = os.path.exists(file_path)
        
        if exists:
            file_size = os.path.getsize(file_path) / (1024 * 1024)  # MB
            logger.info(f"✓ Archivo encontrado: {file_path} ({file_size:.2f} MB)")
        else:
            logger.error(f"✗ Archivo no encontrado: {file_path}")
        
        assert exists
    
    def test_database_connection(self):
        """Test que la conexión a la base de datos funciona"""
        logger.info("TEST: Verificando conexión a base de datos")
        try:
            with engine.connect() as conn:
                result = conn.execute(text("SELECT 1"))
                assert result.scalar() == 1
            logger.info("✓ Conexión a base de datos exitosa")
        except Exception as e:
            logger.error(f"✗ Error de conexión: {str(e)}")
            raise
    
    def test_job_posting_table_exists(self):
        """Test que la tabla job_posting existe y tiene datos"""
        logger.info("TEST: Verificando existencia y contenido de tabla job_posting")
        
        with engine.connect() as conn:
            # Verificar que la tabla existe
            result = conn.execute(text("""
                SELECT COUNT(*) 
                FROM information_schema.tables 
                WHERE table_name = 'job_posting'
            """))
            table_exists = result.scalar() == 1
            
            if table_exists:
                logger.info("✓ Tabla 'job_posting' existe")
            else:
                logger.error("✗ Tabla 'job_posting' no existe")
            
            assert table_exists
            
            # Verificar que tiene datos
            result = conn.execute(text("SELECT COUNT(*) FROM job_posting"))
            count = result.scalar()
            
            if count > 0:
                logger.info(f"✓ Tabla contiene {count:,} registros")
            else:
                logger.error("✗ Tabla está vacía")
            
            assert count > 0
    
    def test_job_posting_schema(self):
        """Test que la tabla job_posting tiene las columnas esperadas"""
        logger.info("TEST: Verificando esquema de tabla job_posting")
        
        expected_columns = {
            'id', 'job_title_short', 'job_title', 'job_location', 'job_via',
            'job_schedule_type', 'job_work_from_home', 'search_location',
            'job_posted_date', 'job_no_degree_mention', 'job_health_insurance',
            'job_country', 'salary_rate', 'salary_year_avg', 'salary_hour_avg',
            'company_name', 'job_skills', 'job_type_skills'
        }
        
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = 'job_posting'
            """))
            actual_columns = {row[0] for row in result}
        
        missing_columns = expected_columns - actual_columns
        extra_columns = actual_columns - expected_columns
        
        if missing_columns:
            logger.error(f"✗ Columnas faltantes: {missing_columns}")
        if extra_columns:
            logger.info(f"Columnas adicionales: {extra_columns}")
        
        if expected_columns.issubset(actual_columns):
            logger.info(f"✓ Todas las {len(expected_columns)} columnas esperadas están presentes")
        
        assert expected_columns.issubset(actual_columns)
    
    def test_clean_df_function(self):
        """Test la función de limpieza de DataFrame"""
        logger.info("TEST: Verificando función clean_df")
        
        # Crear DataFrame de prueba con duplicados y valores nulos
        test_data = pd.DataFrame({
            'job_title': ['Developer', 'Developer', 'null', ''],
            'company_name': ['Company A', 'Company A', 'Company B', 'NULL'],
            'job_work_from_home': ['true', 'true', 'false', 'nan']
        })
        
        logger.debug(f"DataFrame de prueba creado: {len(test_data)} registros")
        cleaned_df = clean_df(test_data)
        logger.debug(f"DataFrame limpio: {len(cleaned_df)} registros")
        
        # Verificar que se eliminaron duplicados
        duplicates_removed = len(test_data) - len(cleaned_df)
        logger.info(f"Duplicados eliminados: {duplicates_removed}")
        assert len(cleaned_df) < len(test_data)
        
        # Verificar que se limpiaron valores nulos
        null_titles = cleaned_df['job_title'].isna().sum()
        null_companies = cleaned_df['company_name'].isna().sum()
        logger.info(f"Valores nulos detectados - job_title: {null_titles}, company_name: {null_companies}")
        
        assert null_titles >= 1
        assert null_companies >= 1
        
        logger.info("✓ Función clean_df funciona correctamente")
    
    def test_parse_skills_string(self):
        """Test el parsing de strings de skills"""
        logger.info("TEST: Verificando función parse_skills_string")
        
        test_cases = [
            ("['python', 'sql']", ['python', 'sql']),
            ("python, sql, javascript", ['python', 'sql', 'javascript']),
            ("python", ['python']),
            (None, None),
            ("", None),
            ("nan", None)
        ]
        
        for input_val, expected in test_cases:
            result = parse_skills_string(input_val)
            logger.debug(f"Input: {input_val} -> Output: {result} (Expected: {expected})")
            assert result == expected
        
        logger.info("✓ Función parse_skills_string funciona correctamente")
    
    def test_parse_type_skills_string(self):
        """Test el parsing de strings de type_skills JSON"""
        logger.info("TEST: Verificando función parse_type_skills_string")
        
        # Test caso válido
        json_str = "{'programming': ['python', 'sql'], 'cloud': ['aws']}"
        result = parse_type_skills_string(json_str)
        expected = {'programming': ['python', 'sql'], 'cloud': ['aws']}
        logger.debug(f"Parsing JSON válido: {result}")
        assert result == expected
        
        # Test casos nulos/vacíos
        null_cases = [None, "", "invalid_json"]
        for case in null_cases:
            result = parse_type_skills_string(case)
            logger.debug(f"Input: {case} -> Output: {result}")
            assert result is None
        
        logger.info("✓ Función parse_type_skills_string funciona correctamente")
    
    def test_data_quality_checks(self):
        """Test checks básicos de calidad de datos"""
        logger.info("TEST: Verificando calidad de datos")
        
        with engine.connect() as conn:
            # Test: Verificar que hay registros
            result = conn.execute(text("SELECT COUNT(*) FROM job_posting"))
            total_count = result.scalar()
            logger.info(f"Total de registros: {total_count:,}")
            assert total_count > 0, "No hay registros en job_posting"
            
            # Test: Debe haber registros con skills
            result = conn.execute(text("""
                SELECT COUNT(*) 
                FROM job_posting 
                WHERE job_skills IS NOT NULL AND array_length(job_skills, 1) > 0
            """))
            skills_count = result.scalar()
            skills_percentage = (skills_count / total_count * 100) if total_count > 0 else 0
            logger.info(f"Registros con job_skills: {skills_count:,} ({skills_percentage:.1f}%)")
            assert skills_count > 0, "No hay registros con job_skills"
            
            # Test: Debe haber registros con type_skills
            result = conn.execute(text("""
                SELECT COUNT(*) 
                FROM job_posting 
                WHERE job_type_skills IS NOT NULL
            """))
            type_skills_count = result.scalar()
            type_skills_percentage = (type_skills_count / total_count * 100) if total_count > 0 else 0
            logger.info(f"Registros con job_type_skills: {type_skills_count:,} ({type_skills_percentage:.1f}%)")
            assert type_skills_count > 0, "No hay registros con job_type_skills"
            
            logger.info("✓ Checks de calidad de datos pasaron exitosamente")
    
    def test_posted_date_range_2023(self):
        """Test que valida que las fechas en posted_date NO sean anteriores a 2023"""
        logger.info("TEST: Verificando rango de fechas posted_date")
        
        with engine.connect() as conn:
            # Definir que las fechas no deben ser anteriores a 2023
            min_date = datetime(2023, 1, 1)
            max_date = datetime.now()  # Hasta la fecha actual
            
            # Contar total de registros con fechas válidas (no nulas)
            result = conn.execute(text("""
                SELECT COUNT(*) 
                FROM job_posting 
                WHERE job_posted_date IS NOT NULL
            """))
            total_with_dates = result.scalar()
            logger.info(f"Registros con fechas: {total_with_dates:,}")
            assert total_with_dates > 0, "No hay registros con fechas válidas"
            
            # Contar registros con fechas válidas (>= 2023)
            result = conn.execute(text("""
                SELECT COUNT(*) 
                FROM job_posting 
                WHERE job_posted_date >= :min_date
            """), {"min_date": min_date})
            dates_valid = result.scalar()
            
            # Contar registros con fechas anteriores a 2023 (inválidas)
            result = conn.execute(text("""
                SELECT COUNT(*) 
                FROM job_posting 
                WHERE job_posted_date IS NOT NULL 
                AND job_posted_date < :min_date
            """), {"min_date": min_date})
            dates_before_2023 = result.scalar()
            
            # Obtener fechas mínima y máxima para diagnóstico
            result = conn.execute(text("""
                SELECT MIN(job_posted_date) as min_date, MAX(job_posted_date) as max_date
                FROM job_posting 
                WHERE job_posted_date IS NOT NULL
            """))
            row = result.fetchone()
            db_min_date, db_max_date = row[0], row[1]
            
            logger.info(f"Análisis de fechas posted_date:")
            logger.info(f"  • Fechas válidas (>= 2023): {dates_valid:,}")
            logger.info(f"  • Fechas anteriores a 2023: {dates_before_2023:,}")
            logger.info(f"  • Rango: {db_min_date} a {db_max_date}")
            
            # Validar que NO haya fechas anteriores a 2023
            if dates_before_2023 == 0:
                logger.info("✓ Todas las fechas posted_date son >= 2023")
            else:
                logger.error(f"✗ Se encontraron {dates_before_2023} fechas anteriores a 2023")
            
            assert dates_before_2023 == 0, f"Se encontraron {dates_before_2023} fechas anteriores a 2023. Todas las fechas deben ser >= {min_date.date()}"

if __name__ == "__main__":
    pytest.main([__file__, "-v"])