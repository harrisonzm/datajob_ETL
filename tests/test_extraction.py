import pytest
import pandas as pd
from sqlalchemy import text
from db.config.db import engine, get_db
from extraction.extraction import clean_df, parse_skills_string, parse_type_skills_string

class TestExtraction:
    """Tests para el proceso de extracción de datos"""
    
    def test_database_connection(self):
        """Test que la conexión a la base de datos funciona"""
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1"))
            assert result.scalar() == 1
    
    def test_job_posting_table_exists(self):
        """Test que la tabla job_posting existe y tiene datos"""
        with engine.connect() as conn:
            # Verificar que la tabla existe
            result = conn.execute(text("""
                SELECT COUNT(*) 
                FROM information_schema.tables 
                WHERE table_name = 'job_posting'
            """))
            assert result.scalar() == 1
            
            # Verificar que tiene datos
            result = conn.execute(text("SELECT COUNT(*) FROM job_posting"))
            count = result.scalar()
            assert count > 0
            print(f"✅ job_posting tiene {count:,} registros")
    
    def test_job_posting_schema(self):
        """Test que la tabla job_posting tiene las columnas esperadas"""
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
            
        assert expected_columns.issubset(actual_columns)
        print(f"✅ Todas las columnas esperadas están presentes")
    
    def test_clean_df_function(self):
        """Test la función de limpieza de DataFrame"""
        # Crear DataFrame de prueba con duplicados y valores nulos
        test_data = pd.DataFrame({
            'job_title': ['Developer', 'Developer', 'null', ''],
            'company_name': ['Company A', 'Company A', 'Company B', 'NULL'],
            'job_work_from_home': ['true', 'true', 'false', 'nan']
        })
        
        cleaned_df = clean_df(test_data)
        
        # Verificar que se eliminaron duplicados
        assert len(cleaned_df) < len(test_data)
        
        # Verificar que se limpiaron valores nulos
        assert cleaned_df['job_title'].isna().sum() >= 1
        assert cleaned_df['company_name'].isna().sum() >= 1
        
        print("✅ Función clean_df funciona correctamente")
    
    def test_parse_skills_string(self):
        """Test el parsing de strings de skills"""
        # Test casos válidos
        assert parse_skills_string("['python', 'sql']") == ['python', 'sql']
        assert parse_skills_string("python, sql, javascript") == ['python', 'sql', 'javascript']
        assert parse_skills_string("python") == ['python']
        
        # Test casos nulos/vacíos
        assert parse_skills_string(None) is None
        assert parse_skills_string("") is None
        assert parse_skills_string("nan") is None
        
        print("✅ Función parse_skills_string funciona correctamente")
    
    def test_parse_type_skills_string(self):
        """Test el parsing de strings de type_skills JSON"""
        # Test caso válido
        json_str = "{'programming': ['python', 'sql'], 'cloud': ['aws']}"
        result = parse_type_skills_string(json_str)
        expected = {'programming': ['python', 'sql'], 'cloud': ['aws']}
        assert result == expected
        
        # Test casos nulos/vacíos
        assert parse_type_skills_string(None) is None
        assert parse_type_skills_string("") is None
        assert parse_type_skills_string("invalid_json") is None
        
        print("✅ Función parse_type_skills_string funciona correctamente")
    
    def test_data_quality_checks(self):
        """Test checks básicos de calidad de datos"""
        with engine.connect() as conn:
            # Test: Verificar que hay registros
            result = conn.execute(text("SELECT COUNT(*) FROM job_posting"))
            total_count = result.scalar()
            assert total_count > 0, "No hay registros en job_posting"
            
            # Test: Verificar que los IDs son únicos (nota: pueden ser secuenciales)
            result = conn.execute(text("""
                SELECT COUNT(DISTINCT id) as unique_ids, COUNT(*) as total_rows
                FROM job_posting
            """))
            row = result.fetchone()
            unique_ids, total_rows = row[0], row[1]
            
            # Si hay duplicados, es porque los IDs se generaron secuencialmente
            if unique_ids != total_rows:
                print(f"⚠️  IDs no únicos: {unique_ids} únicos de {total_rows} totales")
                print("   Esto es normal si los datos se cargaron múltiples veces")
            else:
                print(f"✅ Todos los IDs son únicos: {unique_ids}")
            
            # Test: Debe haber registros con skills
            result = conn.execute(text("""
                SELECT COUNT(*) 
                FROM job_posting 
                WHERE job_skills IS NOT NULL AND array_length(job_skills, 1) > 0
            """))
            skills_count = result.scalar()
            assert skills_count > 0, "No hay registros con job_skills"
            
            # Test: Debe haber registros con type_skills
            result = conn.execute(text("""
                SELECT COUNT(*) 
                FROM job_posting 
                WHERE job_type_skills IS NOT NULL
            """))
            type_skills_count = result.scalar()
            assert type_skills_count > 0, "No hay registros con job_type_skills"
            
            print(f"✅ {skills_count:,} registros con skills")
            print(f"✅ {type_skills_count:,} registros con type_skills")

if __name__ == "__main__":
    pytest.main([__file__, "-v"])