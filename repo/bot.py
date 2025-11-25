import os
import re
import json
import duckdb
import pandas as pd
import streamlit as st
from pathlib import Path
from dotenv import load_dotenv
from openai import OpenAI

# =========================
# Configuración
# =========================
load_dotenv()
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4.1-mini")

st.set_page_config(page_title="Parquet (carpetas) Q&A con SQL", layout="wide")

if not OPENAI_API_KEY:
    st.error("Falta OPENAI_API_KEY en el entorno (.env).")
    st.stop()

client = OpenAI(api_key=OPENAI_API_KEY)

# =========================
# Seguridad SQL (solo lectura)
# =========================
READ_ONLY_SQL_PATTERN = re.compile(r"^\s*select\b", flags=re.IGNORECASE | re.DOTALL)
FORBIDDEN_SQL_TOKENS = re.compile(
    r"\b(insert|update|delete|create|drop|alter|attach|copy|pragma|call|execute|grant|revoke|replace)\b",
    flags=re.IGNORECASE
)

def is_read_only_sql(sql: str) -> bool:
    """Permite solo SELECT, sin tokens peligrosos ni múltiples sentencias."""
    if not sql:
        return False
    if not READ_ONLY_SQL_PATTERN.search(sql):
        return False
    if FORBIDDEN_SQL_TOKENS.search(sql):
        return False
    # Evitar múltiples sentencias (permitir ; final)
    s = sql.strip()
    if ";" in s[:-1]:
        return False
    return True

# =========================
# Funciones auxiliares
# =========================
def build_schema_summary_duckdb(con: duckdb.DuckDBPyConnection) -> str:
    """Resumen de columnas, dtypes y muestra de filas (para el LLM)."""
    cols = con.execute("DESCRIBE SELECT * FROM data").fetchall()
    dtypes = {c[0]: c[1] for c in cols}
    sample = con.execute("SELECT * FROM data LIMIT 6").df().to_dict(orient="records")
    summary = {
        "columns": list(dtypes.keys()),
        "dtypes": dtypes,
        "rows_example": sample,
    }
    return json.dumps(summary, ensure_ascii=False)

def llm_generate_sql(question: str, schema_json: str) -> str:
    """
    Convierte una pregunta en UNA única sentencia SELECT (DuckDB) sobre la tabla 'data'.
    Sin comentarios ni fences. Sin DDL/DML.
    """
    system = (
        "Eres un generador de SQL experto. Trabajas con DuckDB. "
        "La tabla disponible se llama data. Devuelves EXACTAMENTE una única sentencia SQL SELECT."
    )
    user = (
        "Convierte la siguiente pregunta en una única consulta SQL SELECT válida para DuckDB.\n"
        "Usa SOLO la tabla 'data'. Prohíbe subconsultas DML o DDL. "
        "Si necesitas agregaciones o filtros, inclúyelos. Si piden conteos o estadísticas, calcula con SQL.\n"
        "No incluyas comentarios, ni '```', ni texto adicional.\n\n"
        f"ESQUEMA Y EJEMPLOS (JSON): {schema_json}\n\n"
        f"PREGUNTA: {question}"
    )

    resp = client.chat.completions.create(
        model=OPENAI_MODEL,
        temperature=0.1,
        messages=[
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
    )
    sql = resp.choices[0].message.content.strip()

    # Limpieza si llegaran fences
    if sql.startswith("```"):
        sql = sql.strip("`").strip()
        sql = re.sub(r"^sql\s*", "", sql, flags=re.IGNORECASE).strip()

    return sql

def explain_answer(question: str, sql: str, df_result: pd.DataFrame) -> str:
    """Explica en lenguaje natural el resultado (usa una muestra para el prompt)."""
    preview = df_result.head(10).to_dict(orient="records")
    preview_json = json.dumps(preview, ensure_ascii=False)
    user = (
        "Con la consulta SQL ejecutada y el resultado (muestra), redacta una respuesta clara y breve para el usuario. "
        "Incluye insights clave y menciona columnas relevantes. No inventes datos.\n\n"
        f"PREGUNTA: {question}\n"
        f"SQL: {sql}\n"
        f"RESULTADO (muestra JSON): {preview_json}"
    )
    resp = client.chat.completions.create(
        model=OPENAI_MODEL,
        temperature=0.2,
        messages=[
            {"role": "system", "content": "Eres un analista de datos conciso y preciso."},
            {"role": "user", "content": user},
        ],
    )
    return resp.choices[0].message.content.strip()

def normalize_glob_path(path_str: str) -> str:
    """Normaliza ruta para glob recursivo de DuckDB."""
    p = Path(path_str).as_posix().rstrip("/")
    return f"{p}/**/*.parquet"

# =========================
# UI
# =========================
st.title("🧠💬 Q&A sobre carpeta Parquet particionada (DuckDB + ChatGPT)")

with st.sidebar:
    st.header("1) Selecciona la carpeta")
    st.caption("Ej.: ./players_score  (con subcarpetas tipo season_part=2024-2025)")
    default_dir = "./players_score"
    parquet_dir = st.text_input("Ruta a la carpeta raíz", value=default_dir)
    enforce_limit = st.toggle("Forzar LIMIT si falta", value=True)
    default_limit = st.number_input("Valor de LIMIT forzado", min_value=10, max_value=100000, value=1000, step=100)
    st.markdown("---")
    st.caption("Seguridad: solo se permite SQL de lectura (SELECT).")

# Validar carpeta
if not parquet_dir or not Path(parquet_dir).exists():
    st.info("Indica una ruta válida a la carpeta que contiene las particiones Parquet.")
    st.stop()

# Conexión DuckDB y vista
con = duckdb.connect(database=":memory:")

try:
    glob_pattern = normalize_glob_path(parquet_dir)
    # Creamos una vista lógica que lee todas las particiones
    con.execute("DROP VIEW IF EXISTS data")
    con.execute(f"CREATE VIEW data AS SELECT * FROM read_parquet('{glob_pattern}')")
except Exception as e:
    st.error(f"No se pudo crear la vista desde la carpeta: {e}")
    st.stop()

# Mostrar esquema y muestra
st.success(f"Dataset cargado desde: {Path(parquet_dir).resolve().as_posix()}")
with st.expander("Ver esquema (DESCRIBE) y muestra"):
    try:
        schema_df = con.execute("DESCRIBE SELECT * FROM data").df()
        st.subheader("Esquema")
        st.dataframe(schema_df, use_container_width=True)
    except Exception as e:
        st.error(f"No se pudo obtener el esquema: {e}")

    try:
        sample_df = con.execute("SELECT * FROM data LIMIT 50").df()
        st.subheader("Muestra (50 filas)")
        st.dataframe(sample_df, use_container_width=True)
    except Exception as e:
        st.error(f"No se pudo cargar una muestra: {e}")

# Entrada de pregunta
st.header("2) Haz tu pregunta")
q = st.text_input("Ej.: ¿cuántos registros hay por season_part y equipo?")
do_explain = st.toggle("Explicación en lenguaje natural", value=True)

# Botón
if st.button("Consultar", type="primary"):
    if not q.strip():
        st.warning("Escribe una pregunta primero.")
        st.stop()

    # Resumen de esquema para guiar al LLM
    try:
        schema_json = build_schema_summary_duckdb(con)
    except Exception as e:
        st.error(f"No se pudo construir el resumen de esquema: {e}")
        st.stop()

    # Generar SQL con LLM
    sql = llm_generate_sql(q.strip(), schema_json)

    # Forzar LIMIT si no hay y el usuario lo desea
    if enforce_limit and re.search(r"\blimit\b", sql, flags=re.IGNORECASE) is None:
        sql = f"{sql.rstrip(';')} LIMIT {int(default_limit)}"

    # Validación de seguridad
    if not is_read_only_sql(sql):
        st.error("La consulta generada no es de solo lectura o no es válida. Revisa o reformula la pregunta.")
        st.subheader("SQL generada (no válida)")
        st.code(sql or "-- vacío", language="sql")
        st.stop()

    st.subheader("Consulta SQL generada")
    st.code(sql, language="sql")

    # Ejecutar
    try:
        result_df = con.execute(sql).df()
    except Exception as e:
        st.error(f"Error ejecutando la consulta: {e}")
        st.stop()

    # Mostrar resultados
    st.subheader("Resultado")
    if len(result_df) == 0:
        st.info("La consulta no devolvió filas.")
    else:
        st.dataframe(result_df, use_container_width=True)

    # Explicación opcional
    if do_explain:
        try:
            explanation = explain_answer(q.strip(), sql, result_df)
            st.subheader("Explicación")
            st.write(explanation)
        except Exception as e:
            st.warning(f"No se pudo generar explicación automática: {e}")
