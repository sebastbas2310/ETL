# ETL: CSV → PostgreSQL con Docker Compose

Pipeline ETL que lee un archivo CSV de productos (con datos sucios reales:
nulos, mayúsculas inconsistentes, precios negativos, IDs duplicados),
lo limpia/transforma y lo carga en una base de datos PostgreSQL. Todo corre
en contenedores orquestados con Docker Compose.

## Estructura del proyecto

```
etl_project/
├── docker-compose.yml
├── data/
│   └── sample_data.csv      # CSV de muestra (440 filas, datos sucios reales)
├── app/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── etl.py                # Script ETL
└── README.md
```

## Calidad de datos: qué hace la transformación

El CSV de entrada trae problemas típicos de un dataset real:

| Problema encontrado | Cómo lo resuelve el ETL |
|---|---|
| Espacios sobrantes en texto | `.str.strip()` |
| Categorías con mayúsculas inconsistentes (`ACCESORIOS`, `accesorios`) | Normalización a `Title Case` |
| Precios negativos (2 filas) | Se descartan (dato inválido) |
| Filas sin `producto_id`, `nombre_producto` o `precio_unitario` | Se descartan (campos críticos) |
| `stock_disponible` vacío | Se rellena con `0` |
| `fecha_ingreso` vacía o inválida | Se guarda como `NULL` |
| `producto_id` duplicado (47 casos, con datos distintos entre sí) | Se conserva el último registro del archivo |

Resultado: de 440 filas de entrada quedan ~366 filas limpias cargadas en la tabla `productos`.

## Requisitos

- Docker
- Docker Compose (v2, incluido en Docker Desktop)

## Cómo ejecutarlo

1. Ubícate en la carpeta raíz del proyecto (`etl_project/`).
2. Levanta los contenedores:

   ```bash
   docker compose up --build
   ```

3. Lo que ocurre automáticamente:
   - Se levanta un contenedor de **PostgreSQL 16** (`etl_postgres`) con la base `etl_db`.
   - Cuando la base de datos está lista (`healthcheck`), se levanta el contenedor **etl_app**.
   - `etl_app` ejecuta `etl.py`, que:
     1. **Extract**: lee `data/sample_data.csv`.
     2. **Transform**: castea tipos, calcula `total_venta` (cantidad × precio_unitario) y descarta filas con campos críticos nulos.
     3. **Load**: crea la tabla `productos` si no existe y hace `UPSERT` de los registros (evita duplicados si vuelves a correr el proceso).
   - Al terminar, verás en el log: `ETL finalizado con éxito ✅`

4. Para correrlo en segundo plano:

   ```bash
   docker compose up --build -d
   ```

## Verificar los datos cargados

Con los contenedores corriendo:

```bash
docker exec -it etl_postgres psql -U etl_user -d etl_db -c "SELECT * FROM productos LIMIT 10;"
```

## Volver a correr el ETL (por ejemplo con un CSV nuevo)

Reemplaza `data/sample_data.csv` por tu propio archivo (respetando las columnas) y:

```bash
docker compose up --build etl
```

## Apagar y limpiar

```bash
docker compose down          # detiene los contenedores
docker compose down -v       # además borra el volumen de datos de Postgres
```

## Notas de diseño

- **Reintentos de conexión**: el script espera a que Postgres esté disponible
  (útil si el `healthcheck` no es suficiente en máquinas lentas).
- **Idempotencia**: se usa `ON CONFLICT DO UPDATE` sobre `id_venta`, así que
  correr el ETL varias veces con el mismo CSV no genera duplicados.
- **Configuración por variables de entorno**: host, credenciales, ruta del CSV
  y nombre de tabla se inyectan desde `docker-compose.yml`, no están *hardcodeados*
  en el script.