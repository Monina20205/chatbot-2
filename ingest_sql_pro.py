import json
import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine, text
from langchain_ollama import OllamaEmbeddings
import uuid
import os

# --- CONFIGURACIÓN DINÁMICA DE RED ---
# Como este script lo corres desde tu Mac, usará localhost:5432
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "first_service"
DB_USER = "postgres"
DB_PASS = "admin"

# Detectar si estamos dentro de Docker (por si acaso)
if os.path.exists("/.dockerenv"):
    DB_HOST = "db"

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# --- INICIALIZACIÓN DE COMPONENTES ---
engine = create_engine(DATABASE_URL)
# El base_url apunta a tu Mac desde afuera
embeddings_model = OllamaEmbeddings(model="llama3", base_url="http://localhost:11434")

def setup_infrastructure():
    """Prepara el esquema de tablas en Postgres (Vector + Auditoría)"""
    print("🏗️  Preparando infraestructura de tablas en Postgres...")
    with engine.connect() as conn:
        # Activar extensión de vectores
        conn.execute(text("CREATE EXTENSION IF NOT EXISTS vector;"))
        
        # Tabla de Almacenamiento Vectorial (RAG)
        conn.execute(text("DROP TABLE IF EXISTS vector_store;"))
        conn.execute(text("""
            CREATE TABLE vector_store (
                id UUID PRIMARY KEY,
                content TEXT,
                metadata JSONB,
                embedding VECTOR(4096)
            );
        """))
        
        # Tabla de Auditoría (Compliance Bancario)
        conn.execute(text("DROP TABLE IF EXISTS audit_logs;"))
        conn.execute(text("""
            CREATE TABLE audit_logs (
                id SERIAL PRIMARY KEY,
                user_id INT,
                user_query TEXT,
                ai_response TEXT,
                latency_ms FLOAT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """))
        conn.commit()
    print("✅ Tablas 'vector_store' y 'audit_logs' listas.")

def run_bank_ingestion():
    """Extrae del ODS (CSV), transforma y carga con vectores"""
    setup_infrastructure()
    
    # 1. Extracción del ODS
    try:
        df = pd.read_csv('pagos.csv')
    except FileNotFoundError:
        print("❌ Error: No se encontró el archivo 'pagos.csv'.")
        return

    print(f"🚀 Procesando {len(df)} registros para First Service...")
    
    for _, row in df.iterrows():
        # 2. Transformación a Lenguaje Natural (Chunking Semántico)
        # Esto optimiza la lectura del LLM evitando tablas crudas
        natural_language_chunk = (
            f"Registro Oficial First Service: El cliente {row['cliente']} (ID de cuenta: {row['id']}) "
            f"mantiene un saldo actual de {row['monto']} USD. Su cuenta es de categoría {row['tipo_cuenta']} "
            f"y el último movimiento registrado fue el {row['fecha']}."
        )
        
        # 3. Generación de Embeddings
        try:
            vector = embeddings_model.embed_query(natural_language_chunk)
        except Exception as e:
            print(f"❌ Error conectando con Ollama: {e}")
            break
            
        # 4. Carga Atómica a Postgres
        try:
            with engine.connect() as conn:
                conn.execute(
                    text("""
                        INSERT INTO vector_store (id, content, metadata, embedding) 
                        VALUES (:id, :content, :metadata, :embedding)
                    """),
                    {
                        "id": str(uuid.uuid4()),
                        "content": natural_language_chunk,
                        # CAMBIO AQUÍ: Usamos json.dumps en lugar de sqlalchemy.JSON.dumps
                        "metadata": json.dumps({"user_id": int(row['id'])}), 
                        "embedding": vector
                    }
                )
                conn.commit()
        except Exception as e:
            print(f"❌ Error al insertar registro en la base de datos: {e}")
            continue

    print(f"✅ Pipeline completado con éxito. Datos indexados en {DB_HOST}.")

if __name__ == "__main__":
    run_bank_ingestion()