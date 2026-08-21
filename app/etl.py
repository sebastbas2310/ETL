"""
ETL: CSV (productos) -> PostgreSQL
------------------------------------------------------------
EXTRACT  : Lee el archivo CSV de productos.
TRANSFORM: Limpia el dataset (tiene datos "sucios" reales):
           - quita espacios sobrantes en texto
           - normaliza mayúsculas/minúsculas en 'categoria'
           - descarta filas sin producto_id, nombre_producto o
             precio_unitario (campos críticos)
           - descarta precios negativos (inválidos)
           - rellena stock_disponible nulo con 0
           - convierte fecha_ingreso a fecha real (o NULL si no es válida)
           - si un producto_id aparece repetido, se queda con el
             último registro del archivo (dato más reciente)
LOAD     : Crea la tabla (si no existe) y carga los datos en
           PostgreSQL con UPSERT (evita duplicados si el script
           se corre más de una vez).
------------------------------------------------------------
El archivo sample_data.csv se preparó deliberadamente con datos "sucios"
—valores nulos, categorías con mayúsculas y minúsculas inconsistentes,
precios negativos y productos con identificadores repetidos— para simular las condiciones
reales con las que suele llegar la información a un proceso ETL. La intención no es partir
de un dataset perfecto, sino demostrar que el pipeline es capaz de detectar y resolver
estos problemas de calidad de forma controlada: descartando registros con campos críticos ausentes,
normalizando texto, filtrando valores inválidos y resolviendo duplicados antes de cargar la
información en la base de datos. Así, el resultado final no es solo un script que mueve datos
de un lugar a otro, sino uno que garantiza que lo que llega a PostgreSQL es información confiable y consistente.
"""


import os
import sys
import time
import logging

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger("etl")

# --------------------------------------------------------------------------
# Configuración (se lee desde variables de entorno, definidas en docker-compose)
# --------------------------------------------------------------------------
DB_HOST = os.getenv("DB_HOST", "db")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "etl_db")
DB_USER = os.getenv("DB_USER", "etl_user")
DB_PASSWORD = os.getenv("DB_PASSWORD", "etl_password")
CSV_PATH = os.getenv("CSV_PATH", "/app/data/sample_data.csv")
TABLE_NAME = os.getenv("TABLE_NAME", "productos")

DB_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

DDL = f"""
CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
    producto_id       INTEGER PRIMARY KEY,
    nombre_producto    VARCHAR(150) NOT NULL,
    categoria          VARCHAR(50),
    precio_unitario    NUMERIC(12, 2) NOT NULL,
    stock_disponible   INTEGER NOT NULL DEFAULT 0,
    fecha_ingreso       DATE,
    fecha_carga         TIMESTAMP DEFAULT NOW()
);
"""

UPSERT = f"""
INSERT INTO {TABLE_NAME} (
    producto_id, nombre_producto, categoria, precio_unitario,
    stock_disponible, fecha_ingreso
) VALUES (
    :producto_id, :nombre_producto, :categoria, :precio_unitario,
    :stock_disponible, :fecha_ingreso
)
ON CONFLICT (producto_id) DO UPDATE SET
    nombre_producto  = EXCLUDED.nombre_producto,
    categoria        = EXCLUDED.categoria,
    precio_unitario  = EXCLUDED.precio_unitario,
    stock_disponible = EXCLUDED.stock_disponible,
    fecha_ingreso    = EXCLUDED.fecha_ingreso;
"""


def get_engine(retries: int = 10, delay: int = 3):
    """Reintenta la conexión mientras el contenedor de Postgres termina de iniciar."""
    for attempt in range(1, retries + 1):
        try:
            engine = create_engine(DB_URL)
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            log.info("Conexión a PostgreSQL exitosa.")
            return engine
        except OperationalError:
            log.warning(f"Postgres no disponible aún (intento {attempt}/{retries}). Reintentando...")
            time.sleep(delay)
    log.error("No fue posible conectar a PostgreSQL. Abortando.")
    sys.exit(1)


def extract(csv_path: str) -> pd.DataFrame:
    log.info(f"Extrayendo datos desde {csv_path}")
    if not os.path.exists(csv_path):
        log.error(f"No se encontró el archivo CSV en {csv_path}")
        sys.exit(1)
    # encoding='utf-8-sig' porque el CSV trae BOM al inicio
    df = pd.read_csv(csv_path, encoding="utf-8-sig")
    log.info(f"{len(df)} filas leídas.")
    return df


def transform(df: pd.DataFrame) -> pd.DataFrame:
    log.info("Transformando datos...")
    filas_iniciales = len(df)

    # 1. Limpiar espacios en columnas de texto
    for col in ["nombre_producto", "categoria"]:
        df[col] = df[col].astype(str).str.strip()
        df[col] = df[col].replace({"nan": None, "": None})

    # 2. Normalizar mayúsculas/minúsculas de categoria (ACCESORIOS / accesorios -> Accesorios)
    df["categoria"] = df["categoria"].apply(lambda x: x.title() if isinstance(x, str) else x)

    # 3. Descartar precios inválidos: negativos o en cero (dato inválido, no se puede "corregir" con certeza)
    invalidos = (df["precio_unitario"] <= 0).sum()
    if invalidos:
        log.warning(f"Se descartaron {invalidos} filas con precio_unitario negativo o en cero.")
        df.loc[df["precio_unitario"] <= 0, "precio_unitario"] = None

    # 4. Descartar filas sin campos críticos (no se puede cargar un producto sin ID, nombre o precio)
    campos_criticos = ["producto_id", "nombre_producto", "precio_unitario"]
    df = df.dropna(subset=campos_criticos)

    # 5. stock_disponible: nulo -> 0, y a entero
    df["stock_disponible"] = df["stock_disponible"].fillna(0).astype(int)

    # 6. fecha_ingreso: parsear; lo que no sea fecha válida (o esté vacío) queda NULL
    df["fecha_ingreso"] = pd.to_datetime(df["fecha_ingreso"], errors="coerce").dt.date
    df["fecha_ingreso"] = df["fecha_ingreso"].where(df["fecha_ingreso"].notna(), None)

    # 7. producto_id repetido -> nos quedamos con la última aparición en el archivo
    duplicados = df["producto_id"].duplicated(keep="last").sum()
    if duplicados:
        log.warning(f"Se encontraron {duplicados} producto_id duplicados; se conserva el último registro de cada uno.")
        df = df.drop_duplicates(subset="producto_id", keep="last")

    df["producto_id"] = df["producto_id"].astype(int)
    df["precio_unitario"] = df["precio_unitario"].astype(float)

    log.info(f"Transformación completa. {filas_iniciales} filas -> {len(df)} filas listas para cargar.")
    return df


def load(df: pd.DataFrame, engine) -> None:
    log.info(f"Creando tabla '{TABLE_NAME}' si no existe...")
    with engine.begin() as conn:
        conn.execute(text(DDL))

        registros = df.to_dict(orient="records")
        log.info(f"Cargando {len(registros)} registros en '{TABLE_NAME}' (upsert)...")
        for row in registros:
            conn.execute(text(UPSERT), row)

    log.info("Carga finalizada correctamente.")


def main():
    engine = get_engine()
    df = extract(CSV_PATH)
    df = transform(df)
    load(df, engine)
    log.info("ETL finalizado con éxito")


if __name__ == "__main__":
    main()