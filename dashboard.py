import streamlit as st
import pandas as pd
import plotly.express as px
from sqlalchemy import create_engine, text
import os

# --- CONFIGURACIÓN DE INFRAESTRUCTURA (ENVIRONMENT AGNOSTIC) ---
# Si estamos en Docker, el host de la DB es 'db'. Si es local, es 'localhost'.
DB_HOST = "db" if os.path.exists("/.dockerenv") else "localhost"
DATABASE_URL = f"postgresql://postgres:admin@{DB_HOST}:5432/first_service"

engine = create_engine(DATABASE_URL)

st.set_page_config(page_title="First Service - Admin Console", layout="wide")
st.title("🏛️ First Service: Consola de Auditoría y Control")
st.markdown("---")

# Inicializamos el DataFrame de logs vacío para evitar NameErrors
df_logs = pd.DataFrame()

# --- 1. MÉTRICAS DE NIVEL ARQUITECTO ---
st.subheader("🚀 Salud del Sistema")
col1, col2, col3 = st.columns(3)

try:
    with engine.connect() as conn:
        # Conteo de vectores (Datos de clientes)
        total_vectors = conn.execute(text("SELECT count(*) FROM vector_store")).scalar()
        # Conteo de interacciones (Auditoría)
        total_logs = conn.execute(text("SELECT count(*) FROM audit_logs")).scalar()
        # Latencia promedio del sistema
        avg_lat = conn.execute(text("SELECT AVG(latency_ms) FROM audit_logs")).scalar() or 0
    
    col1.metric("Vectores Indexados", total_vectors)
    col2.metric("Logs de Auditoría", total_logs)
    col3.metric("Latencia Promedio", f"{avg_lat:.2f} ms")
except Exception as e:
    st.error("Esperando conexión con la Base de Datos... Asegúrate de que el contenedor 'fs_database' esté activo.")

# --- 2. VISUALIZACIÓN DE AUDITORÍA (Compliance) ---
st.markdown("---")
st.subheader("🕵️ Análisis de Interacciones")

try:
    with engine.connect() as conn:
        df_logs = pd.read_sql(text("""
            SELECT id, user_id, user_query, ai_response, latency_ms, created_at 
            FROM audit_logs 
            ORDER BY created_at DESC 
            LIMIT 20
        """), conn)

    if not df_logs.empty:
        # Gráfico de estabilidad de latencia (Plotly)
        fig = px.line(df_logs, x="created_at", y="latency_ms", 
                      title="Performance del Pipeline RAG",
                      labels={"latency_ms": "Latencia (ms)", "created_at": "Fecha/Hora"})
        st.plotly_chart(fig, use_container_width=True)

        # Tabla detallada para auditoría forense
        st.write("**Detalle de Consultas Recientes:**")
        st.dataframe(df_logs, use_container_width=True)
    else:
        st.info("No hay interacciones registradas. El sistema está en espera de la primera consulta del cliente.")

except Exception as e:
    st.warning("Estructura de auditoría no detectada. La tabla 'audit_logs' se creará tras la primera interacción con el Chatbot.")

# --- 3. MONITOREO DE SEGURIDAD ---
if not df_logs.empty:
    st.markdown("---")
    st.subheader("🚨 Alertas de Seguridad")
    
    # Detección de uso inusual (más de 10 consultas por el mismo ID)
    spam_check = df_logs['user_id'].value_counts()
    anomalies = spam_check[spam_check > 10]
    
    if not anomalies.empty:
        for uid, count in anomalies.items():
            st.warning(f"Posible Anomalía: El Usuario {uid} ha realizado {count} consultas en un periodo corto.")
    else:
        st.success("Tráfico de red estable. No se detectan anomalías de acceso.")