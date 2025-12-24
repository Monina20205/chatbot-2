import streamlit as st
import time
import os
from sqlalchemy import create_engine, text
from langchain_ollama import ChatOllama, OllamaEmbeddings

# --- CONFIGURACIÓN DE RED ---
if os.path.exists("/.dockerenv"):
    DB_HOST = "db"
    OLLAMA_URL = "http://host.docker.internal:11434"
else:
    DB_HOST = "localhost"
    OLLAMA_URL = "http://localhost:11434"

DATABASE_URL = f"postgresql://postgres:admin@{DB_HOST}:5432/first_service"

@st.cache_resource
def get_resources():
    engine = create_engine(DATABASE_URL, pool_pre_ping=True)
    embeddings = OllamaEmbeddings(model="llama3", base_url=OLLAMA_URL)
    llm = ChatOllama(model="llama3", base_url=OLLAMA_URL, temperature=0)
    return engine, embeddings, llm

engine, embeddings_model, llm = get_resources()

st.set_page_config(page_title="First Service - Chatbot", layout="centered")
if "user_id" not in st.session_state: st.session_state.user_id = None
if "chat_history" not in st.session_state: st.session_state.chat_history = []

st.title("🤖 First Service Banking")

if not st.session_state.user_id:
    uid_input = st.text_input("Ingrese su ID de Cliente:")
    if st.button("Ingresar"):
        if uid_input.isdigit():
            st.session_state.user_id = int(uid_input)
            st.rerun()
else:
    st.sidebar.write(f"ID Cliente: {st.session_state.user_id}")
    if st.sidebar.button("Cerrar Sesión"):
        st.session_state.user_id = None
        st.rerun()

    for m in st.session_state.chat_history:
        with st.chat_message(m["role"]): st.markdown(m["content"])

    if prompt := st.chat_input("¿En qué puedo ayudarle?"):
        st.session_state.chat_history.append({"role": "user", "content": prompt})
        with st.chat_message("user"): st.markdown(prompt)

        with st.chat_message("assistant"):
            try:
                # 1. Vectorizar
                vec = embeddings_model.embed_query(prompt)

                # 2. SQL SIN DOS PUNTOS DOBLES (Evita el ProgrammingError)
                with engine.connect() as conn:
                    sql_query = text("""
                        SELECT content FROM vector_store 
                        WHERE CAST(metadata->>'user_id' AS INTEGER) = :uid 
                        ORDER BY embedding <=> CAST(:v AS VECTOR) 
                        LIMIT 1
                    """)
                    res = conn.execute(sql_query, {"uid": st.session_state.user_id, "v": vec}).fetchone()

                # 3. Respuesta
                context = res[0] if res else "No hay datos."
                ans = llm.invoke(f"Contexto: {context}. Pregunta: {prompt}").content
                st.markdown(ans)
                st.session_state.chat_history.append({"role": "assistant", "content": ans})

                # 4. Auditoría
                with engine.connect() as conn:
                    conn.execute(text("INSERT INTO audit_logs (user_id, user_query, ai_response, latency_ms) VALUES (:u, :q, :r, 0)"),
                                 {"u": st.session_state.user_id, "q": prompt, "r": ans})
                    conn.commit()
            except Exception as e:
                st.error(f"Error técnico: {e}")