from sqlalchemy import create_engine, text

# Conexión a tu instancia de Postgres
engine = create_engine("postgresql://postgres:admin@localhost:5432/postgres")

def init_audit_tables():
    with engine.connect() as conn:
        print("🔧 Creando tablas de auditoría...")
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS audit_logs (
                id SERIAL PRIMARY KEY,
                user_id INT,
                user_query TEXT,
                ai_response TEXT,
                latency_ms FLOAT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """))
        conn.commit()
        print("✅ Tabla 'audit_logs' creada exitosamente.")

if __name__ == "__main__":
    init_audit_tables()
