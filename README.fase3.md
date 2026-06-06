# Fase 3: PySpark — del CSV crudo a Postgres (Bronze → Silver)

## Objetivo de esta fase

Escribir los primeros **jobs de Spark** que implementan los dos primeros saltos de la arquitectura **Medallion**:

- **Job 1 (raw → bronze)**: lee los CSVs de `data/raw/`, **descubre dinámicamente** su estructura y vuelca todo al schema `raw_data` de Postgres con metadatos de auditoría.
- **Job 2 (bronze → silver)**: lee de `raw_data`, **valida contra un schema explícito** (el *contrato*), limpia los datos sucios, hace **joins** entre tablas y escribe al schema `staging`.

Al terminar la Fase 3 tendrás el flujo `raw → bronze → silver` funcionando end-to-end. Es **la fase más importante** del proyecto desde el punto de vista técnico: aquí tocas Spark de verdad.

---

## Prerrequisitos

- **Fase 1** y **Fase 2** completas.
- **Java 17** instalado (`java -version`).
- **Schemas Postgres** creados. Si el `init.sql` original solo creó `bronze`, `staging` y `analytics`, hay que añadir `raw_data`. Ya sea actualizando `init.sql` y recreando el volumen, o directamente:

  ```bash
  docker compose exec postgres psql -U fintech_user -d fintech \
    -c "CREATE SCHEMA IF NOT EXISTS raw_data;"
  ```

- **Driver `connectorx`** para que Polars pueda leer de Postgres en la validación:

  ```bash
  uv add connectorx
  ```

---

## Conceptos clave

Antes del código, cinco conceptos que aparecen en el job:

### 1. Schema explícito vs schema inferido

Cuando Spark lee un CSV, puede **inferir** los tipos de cada columna, pero esto tiene problemas:

- **Lento**: Spark lee el archivo dos veces (una para inferir, otra para procesar).
- **Frágil**: si un valor está mal, infiere el tipo equivocado (un monto con un solo nulo se vuelve `string` en vez de `double`).
- **No es explícito**: el código no documenta qué espera.

En **producción**, según la capa:

| Capa | Estrategia | Por qué |
|------|-----------|---------|
| **Bronze (raw_data)** | Schema **inferido** | "Absorbe todo lo que llega". Si la fuente cambia, no se rompe. |
| **Silver (staging)** | Schema **explícito** | Es el *contrato*. Si bronze cambió, **fallar ruidosamente aquí**. |
| **Gold (analytics)** | Schema **estrictamente explícito** | Cada KPI tiene tipos exactos. |

> **Principio guía**: **"fail loud, fail early"**.
> Es mejor que el pipeline truene a las 6 AM con un error claro que enviar datos corruptos a un dashboard que el CEO mire a las 9 AM.

### 2. Lazy evaluation y `.cache()`

Spark **no ejecuta transformaciones** hasta que se llama a una acción (`.show()`, `.count()`, `.write()`). Si se llama varias acciones sobre el mismo DataFrame, **Spark re-ejecuta el plan completo cada vez**. Para evitarlo:

```python
df_limpio = df.filter(...).join(...).withColumn(...)
df_limpio.cache()              # Guarda el resultado en memoria

print(df_limpio.count())       # Primera vez: ejecuta el plan
df_limpio.show()               # Reusa el caché
df_limpio.write.jdbc(...)      # Reusa el caché
```

### 3. Limpieza de datos en Spark

Spark tiene métodos específicos para los tipos de "suciedad" inyectados en Fase 1:

- **Nulos**: `.dropna()`, `.fillna()`, `.filter(col("x").isNotNull())`.
- **Duplicados**: `.dropDuplicates()` o `.dropDuplicates(["columna_id"])`.
- **Filtrado**: `.filter(col("x") > 0)` o `.where(...)`.

### 4. JDBC para escribir a Postgres

Spark **no tiene driver nativo** de Postgres. Necesita el **driver JDBC** (un `.jar` de Java). La forma cómoda es declararlo en la `SparkSession`:

```python
.config("spark.jars.packages", "org.postgresql:postgresql:42.7.3")
```

Spark se conecta a **Maven Central**, descarga el `.jar` y lo cachea. Solo tarda la primera vez.

### 5. Modos de escritura

Cuando se escribe a una tabla, Spark ofrece 4 modos:

- **`overwrite`**: borra la tabla y la reescribe.
- **`append`**: agrega filas (puede crear duplicados).
- **`ignore`**: si la tabla existe, no hace nada.
- **`error`** (default): falla si la tabla existe.

Aquí se usa **`overwrite`** porque los jobs son **idempotentes** (se pueden correr 10 veces y siempre dan el mismo resultado).

---

## Arquitectura: dos jobs en cascada

```text
┌──────────────┐     Job 1: dinámico        ┌──────────────────┐
│  data/raw/   │  ───────────────────────►  │  raw_data.*      │
│  *.csv       │  (inferSchema, absorbe)    │  + metadatos     │
└──────────────┘                            └──────────────────┘
                                                    │
                                                    │ Job 2: estricto
                                                    │ (valida contrato)
                                                    ▼
                                            ┌──────────────────┐
                                            │  staging.*       │
                                            │  (limpio + joins)│
                                            └──────────────────┘
```

---

## Paso 3.1: Builder centralizado de SparkSession

**Patrón profesional**: encapsular la creación de la `SparkSession` en una función reutilizable. Así no se repite configuración en cada job.

Crear `src/utils/spark_session.py`:

```python
"""Builder centralizado de SparkSession.

Encapsula toda la configuración para que cada job sea limpio y consistente.
"""
from __future__ import annotations

from pyspark.sql import SparkSession


def build_spark_session(app_name: str = "FintechAnalytics") -> SparkSession:
    """Construye una SparkSession configurada para correr en local.

    Configuración pensada para una laptop de desarrollo:
    - Modo local con todos los núcleos disponibles
    - JDBC de Postgres precargado
    - Adaptive Query Execution activado
    - Zona horaria local para evitar bugs de fechas
    - shuffle.partitions bajado a 8 (default 200 es excesivo en local)
    """
    return (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.session.timeZone", "America/Guatemala")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.driver.memory", "2g")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
```

### Repaso de cada `config`

- **`master("local[*]")`**: corre en local usando todos los núcleos.
- **`spark.jars.packages`**: descarga el driver JDBC de Postgres automáticamente.
- **`spark.sql.adaptive.enabled`**: **AQE** — Spark optimiza en tiempo de ejecución.
- **`spark.sql.shuffle.partitions=8`**: por defecto es **200**, demasiado para local.
- **`spark.sql.session.timeZone`**: **crítico** para fechas correctas.
- **`spark.serializer`**: **Kryo** es más rápido que el default Java.
- **`spark.driver.memory=2g`**: suficiente RAM para el driver.
- **`spark.ui.showConsoleProgress=false`**: silencia las barras de progreso ruidosas.

---

## Paso 3.2: Job 1 — Ingesta dinámica (raw → bronze)

Este job **no asume nada** sobre la estructura de los CSVs. Descubre las columnas, infiere los tipos, añade metadatos de auditoría y vuelca todo a Postgres.

> **Filosofía**: la capa **Bronze es absorbente**. Si la fuente cambia, no se falla aquí — se absorbe el cambio y la validación estricta vive en el siguiente salto (Silver).

Crear `src/processing/raw_to_bronze.py`:

```python
"""Job: raw -> bronze (ingesta dinámica).

Lee CSVs sin conocer su estructura previamente:
- Detecta automáticamente columnas e infiere tipos.
- Imprime el schema descubierto (observabilidad).
- Vuelca todo a Postgres tal cual, sin tocar nada.
- Añade metadatos de ingesta (fecha de carga, origen).

Filosofía: la capa bronze es ABSORBENTE. Si la fuente cambia, no fallamos aquí.
La validación estricta vive en el siguiente salto (silver).
"""
from __future__ import annotations

from datetime import datetime
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F  # noqa: N812

from src.utils.config import settings
from src.utils.spark_session import build_spark_session


def descubrir_archivos(carpeta: Path, extension: str = "*.csv") -> list[Path]:
    """Encuentra todos los archivos a ingerir, sin lista hardcoded."""
    archivos = sorted(carpeta.glob(extension))
    print(f">> {len(archivos)} archivo(s) encontrado(s) en {carpeta}:")
    for f in archivos:
        print(f"   - {f.name}")
    return archivos


def leer_csv_dinamico(spark: SparkSession, path: Path) -> DataFrame:
    """Lee un CSV sin conocer su schema.

    - header=true le dice que la primera fila son nombres de columna.
    - inferSchema=true hace que Spark lea el archivo dos veces:
      una para inferir tipos, otra para procesar.
    - Es lento en archivos grandes, pero perfecto para bronze.
    """
    return (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .option("multiLine", "true")        # tolera strings con saltos de línea
        .option("escape", '"')               # tolera comillas escapadas
        .csv(str(path))
    )


def anadir_metadatos(df: DataFrame, archivo_origen: str) -> DataFrame:
    """Añade columnas de auditoría que SIEMPRE deben acompañar datos crudos."""
    return (
        df
        .withColumn("_archivo_origen", F.lit(archivo_origen))
        .withColumn("_ingesta_timestamp", F.current_timestamp())
        .withColumn("_ingesta_fecha", F.current_date())
    )


def derivar_nombre_tabla(archivo: Path) -> str:
    """De 'clientes.csv' obtiene 'clientes'."""
    return archivo.stem.lower().replace("-", "_").replace(" ", "_")


def escribir_a_bronze(df: DataFrame, tabla: str) -> None:
    """Escribe al schema raw_data de Postgres."""
    print(f">> Escribiendo a Postgres: raw_data.{tabla}...")
    (
        df.write
        .format("jdbc")
        .option("url", settings.pg_jdbc_url)
        .option("dbtable", f"raw_data.{tabla}")
        .option("user", settings.pg_user)
        .option("password", settings.pg_password)
        .option("driver", "org.postgresql.Driver")
        .mode("overwrite")
        .save()
    )


def procesar_archivo(spark: SparkSession, archivo: Path) -> dict:
    """Pipeline completo para un archivo: lee, inspecciona, añade meta, escribe."""
    tabla = derivar_nombre_tabla(archivo)
    print(f"\n--- Procesando {archivo.name} -> raw_data.{tabla} ---")

    df = leer_csv_dinamico(spark, archivo)

    # OBSERVABILIDAD: imprime lo que descubrió
    print(f">> Schema descubierto ({len(df.columns)} columnas):")
    for campo in df.schema.fields:
        nullable = "NULL" if campo.nullable else "NOT NULL"
        print(f"   - {campo.name}: {campo.dataType.simpleString()} ({nullable})")

    n_filas = df.count()
    print(f">> {n_filas:,} filas leídas")

    print(">> Primeras filas:")
    df.show(3, truncate=40)

    df_con_meta = anadir_metadatos(df, archivo.name)
    escribir_a_bronze(df_con_meta, tabla)

    return {
        "archivo": archivo.name,
        "tabla": tabla,
        "filas": n_filas,
        "columnas": len(df.columns),
    }


def main() -> None:
    print("\n" + "=" * 60)
    print("  Job: raw -> bronze (ingesta dinámica)")
    print(f"  Inicio: {datetime.now().isoformat(timespec='seconds')}")
    print("=" * 60)

    spark = build_spark_session(app_name="RawToBronze")
    spark.sparkContext.setLogLevel("WARN")

    archivos = descubrir_archivos(settings.data_raw, extension="*.csv")
    if not archivos:
        print("!! No hay archivos para procesar. Abortando.")
        spark.stop()
        return

    resumen = []
    for archivo in archivos:
        try:
            info = procesar_archivo(spark, archivo)
            resumen.append(info)
        except Exception as e:
            print(f"!! ERROR procesando {archivo.name}: {e}")

    print("\n" + "=" * 60)
    print("  RESUMEN DE INGESTA")
    print("=" * 60)
    for r in resumen:
        print(f"  {r['tabla']:20s} {r['filas']:>10,} filas  {r['columnas']:>3} cols")
    print("=" * 60 + "\n")

    spark.stop()


if __name__ == "__main__":
    main()
```

### Puntos clave de este código

- **`descubrir_archivos`** usa `Path.glob("*.csv")` para encontrar **cualquier CSV** en la carpeta, sin lista hardcoded. Si mañana se agrega `productos.csv`, lo procesa solo.
- **`inferSchema=true`** es el corazón de la "dinámica". Es 2x más lento que con schema explícito (lee el archivo dos veces), pero da la flexibilidad necesaria para Bronze.
- **Las columnas `_archivo_origen`, `_ingesta_timestamp`, `_ingesta_fecha`** son **metadatos de auditoría**. Son la firma de un buen pipeline: en producción siempre se sabe de qué archivo vino cada fila y cuándo se cargó. Cuando algo falla en Gold tres semanas después, esto es lo que salva.
- **`derivar_nombre_tabla`**: el nombre del archivo es el nombre de la tabla. **Convención sobre configuración**.
- **El `try/except`** en el loop evita que un archivo malo tumbe la ingesta de los demás.

---

## Paso 3.3: Schemas como módulo Python (contrato Silver)

Aquí va la **decisión de diseño** más importante de la fase. Los schemas de Silver deben vivir **separados** del job que los consume — así son reutilizables, testables y editables sin tocar la lógica.

> **¿Por qué Python y no YAML?** Ver la sección de [Decisión de diseño](#decisión-de-diseño-por-qué-python-y-no-yaml-para-los-schemas) más abajo. Spoiler: YAML es excelente cuando crece, pero **over-engineering** para 3 tablas mantenidas por una sola persona.

Crear `src/schemas/__init__.py` (vacío) y `src/schemas/silver.py`:

```python
"""Schemas explícitos para la capa silver.

Definidos en Python (no YAML) por simplicidad: pocas tablas, equipo pequeño,
solo Spark los consume. Si el proyecto crece a 20+ tablas o llegan editores
no-developers (analistas, PMs), considerar migrar a YAML.
"""
from pyspark.sql.types import (
    DateType,
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


CLIENTES = StructType([
    StructField("cliente_id",       StringType(),    nullable=False),
    StructField("nombre_completo",  StringType(),    nullable=True),
    StructField("email",            StringType(),    nullable=True),
    StructField("telefono",         StringType(),    nullable=True),
    StructField("pais",             StringType(),    nullable=True),
    StructField("ciudad",           StringType(),    nullable=True),
    StructField("fecha_registro",   DateType(),      nullable=True),
    StructField("segmento",         StringType(),    nullable=True),
    StructField("fecha_nacimiento", DateType(),      nullable=True),
])

CUENTAS = StructType([
    StructField("cuenta_id",      StringType(), nullable=False),
    StructField("cliente_id",     StringType(), nullable=False),
    StructField("tipo_cuenta",    StringType(), nullable=True),
    StructField("moneda",         StringType(), nullable=True),
    StructField("fecha_apertura", DateType(),   nullable=True),
    StructField("estado",         StringType(), nullable=True),
    StructField("saldo_inicial",  DoubleType(), nullable=True),
])

TRANSACCIONES = StructType([
    StructField("transaccion_id",     StringType(),    nullable=False),
    StructField("cuenta_id",          StringType(),    nullable=False),
    StructField("timestamp_tx",       TimestampType(), nullable=True),
    StructField("monto",              DoubleType(),    nullable=True),
    StructField("tipo_tx",            StringType(),    nullable=True),
    StructField("comercio",           StringType(),    nullable=True),
    StructField("categoria_comercio", StringType(),    nullable=True),
    StructField("pais_tx",            StringType(),    nullable=True),
    StructField("ciudad_tx",          StringType(),    nullable=True),
    StructField("canal",              StringType(),    nullable=True),
])


# Índice por nombre de tabla, permite iterar dinámicamente sin hardcodear nombres.
SCHEMAS = {
    "clientes":      CLIENTES,
    "cuentas":       CUENTAS,
    "transacciones": TRANSACCIONES,
}
```

El diccionario **`SCHEMAS`** es la clave: permite que el job itere sobre todas las tablas **sin conocer sus nombres en código** (al estilo dinámico), pero manteniendo el contrato fuerte.

---

## Paso 3.4: Job 2 — Validación y limpieza (bronze → silver)

Este job lee de `raw_data`, **valida** que cada tabla cumpla el contrato declarado en `src/schemas/silver.py`, **castea** los tipos, limpia las transacciones, hace **joins** y escribe a `staging`.

Crear `src/processing/bronze_to_silver.py`:

```python
"""Job: bronze -> silver.

Lee tablas de raw_data, aplica los schemas declarados en src/schemas/silver.py,
limpia, enriquece con joins y escribe a staging.

Filosofía: aquí hacemos cumplir el CONTRATO. Si bronze cambió, fallamos ruidosamente.
"""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F  # noqa: N812
from pyspark.sql.types import StructType

from src.schemas.silver import SCHEMAS
from src.utils.config import settings
from src.utils.spark_session import build_spark_session


# ============ Lectura desde bronze con validación ============
def leer_de_bronze(spark: SparkSession, tabla: str, schema: StructType) -> DataFrame:
    """Lee tabla de raw_data, valida columnas y castea al schema de silver."""
    print(f">> Leyendo raw_data.{tabla}...")

    df_bronze = (
        spark.read
        .format("jdbc")
        .option("url", settings.pg_jdbc_url)
        .option("dbtable", f"raw_data.{tabla}")
        .option("user", settings.pg_user)
        .option("password", settings.pg_password)
        .option("driver", "org.postgresql.Driver")
        .load()
    )

    # Quitar columnas de metadatos de auditoría
    columnas_meta = ["_archivo_origen", "_ingesta_timestamp", "_ingesta_fecha"]
    df_sin_meta = df_bronze.drop(*[c for c in columnas_meta if c in df_bronze.columns])

    # Validación del contrato: ¿faltan columnas requeridas?
    esperadas = {c.name for c in schema.fields}
    presentes = set(df_sin_meta.columns)
    faltantes = esperadas - presentes
    if faltantes:
        raise ValueError(
            f"raw_data.{tabla} no cumple el contrato silver. Faltan: {faltantes}. "
            f"Encontradas: {sorted(presentes)}"
        )

    # Castear cada columna al tipo del contrato
    df_casteado = df_sin_meta
    for campo in schema.fields:
        df_casteado = df_casteado.withColumn(
            campo.name, F.col(campo.name).cast(campo.dataType)
        )

    return df_casteado.select(*[c.name for c in schema.fields])


# ============ Limpieza específica de transacciones ============
def limpiar_transacciones(transacciones: DataFrame) -> DataFrame:
    """Reglas de calidad para transacciones."""
    print(">> Limpiando transacciones...")
    n_inicial = transacciones.count()

    limpio = (
        transacciones
        .dropDuplicates(["transaccion_id"])
        .filter(F.col("monto").isNotNull())
        .filter(F.col("monto") > 0)
        .withColumn("anio_tx", F.year("timestamp_tx"))
        .withColumn("mes_tx", F.month("timestamp_tx"))
        .withColumn("dia_tx", F.dayofmonth("timestamp_tx"))
        .withColumn("hora_tx", F.hour("timestamp_tx"))
        .withColumn("es_finde", F.dayofweek("timestamp_tx").isin([1, 7]))
    )

    n_final = limpio.count()
    print(f"   {n_inicial:,} -> {n_final:,} filas ({n_inicial - n_final:,} descartadas)")
    return limpio


# ============ Joins ============
def enriquecer_con_joins(
    transacciones: DataFrame,
    cuentas: DataFrame,
    clientes: DataFrame,
) -> DataFrame:
    """Une transacciones con cuenta y cliente.

    Join interno: descarta transacciones de cuentas/clientes inexistentes.
    """
    print(">> Enriqueciendo con joins...")

    # Renombrar 'pais' y 'ciudad' del cliente para no chocar con los de la tx
    clientes_r = (
        clientes
        .withColumnRenamed("pais", "pais_cliente")
        .withColumnRenamed("ciudad", "ciudad_cliente")
    )

    return (
        transacciones
        .join(cuentas, on="cuenta_id", how="inner")
        .join(clientes_r, on="cliente_id", how="inner")
        # Bandera de fraude potencial: tx en país distinto al del cliente
        .withColumn(
            "pais_distinto_cliente",
            F.col("pais_tx") != F.col("pais_cliente"),
        )
    )


# ============ Escritura ============
def escribir_a_staging(df: DataFrame, tabla: str) -> None:
    """Escribe al schema staging de Postgres vía JDBC."""
    print(f">> Escribiendo staging.{tabla}...")
    (
        df.write
        .format("jdbc")
        .option("url", settings.pg_jdbc_url)
        .option("dbtable", f"staging.{tabla}")
        .option("user", settings.pg_user)
        .option("password", settings.pg_password)
        .option("driver", "org.postgresql.Driver")
        .mode("overwrite")
        .save()
    )


# ============ Main ============
def main() -> None:
    print("\n" + "=" * 60)
    print("  Job: bronze -> silver")
    print("=" * 60 + "\n")

    spark = build_spark_session(app_name="BronzeToSilver")
    spark.sparkContext.setLogLevel("WARN")

    print(f">> Schemas cargados: {sorted(SCHEMAS.keys())}\n")

    # Lectura tabla por tabla, aplicando su contrato
    dataframes = {
        tabla: leer_de_bronze(spark, tabla, schema)
        for tabla, schema in SCHEMAS.items()
    }

    # Procesamiento específico de transacciones
    if "transacciones" in dataframes:
        dataframes["transacciones"] = limpiar_transacciones(dataframes["transacciones"])

    # Si tenemos las 3, hacemos joins
    if all(t in dataframes for t in ("transacciones", "cuentas", "clientes")):
        dataframes["transacciones"] = enriquecer_con_joins(
            dataframes["transacciones"],
            dataframes["cuentas"],
            dataframes["clientes"],
        )
        dataframes["transacciones"].cache()

        print("\n>> Muestra de transacciones silver:")
        dataframes["transacciones"].select(
            "transaccion_id", "monto", "tipo_tx",
            "pais_cliente", "pais_tx", "pais_distinto_cliente",
        ).show(5, truncate=False)

    # Escribir TODAS las tablas a staging
    for tabla, df in dataframes.items():
        escribir_a_staging(df, tabla)

    print("\n" + "=" * 60)
    print("  Job completado.")
    print("=" * 60 + "\n")

    spark.stop()


if __name__ == "__main__":
    main()
```

### Lo elegante de este diseño

- El job **no menciona** los nombres de las tablas en su lógica principal: itera sobre `SCHEMAS.items()`.
- Si mañana se agrega `productos` al diccionario `SCHEMAS`, el job lo procesa **sin tocar este archivo**.
- La **validación del contrato** detecta inmediatamente si bronze cambió de forma incompatible.
- El **casteo explícito** garantiza que silver tenga los tipos exactos que necesita Gold/dbt.

---

## Paso 3.5: Conectividad de red

> **Importante**: `.env` tiene `PG_HOST=localhost`. Eso funciona porque Postgres expone el puerto al host. **Spark también corre fuera del contenedor**, así que `localhost:5432` también le sirve. **No hay que cambiar nada.**
>
> Cuando se meta **Airflow dentro de Docker** en una fase posterior, ahí sí habrá que cambiar `localhost` por `postgres` — el nombre del servicio en la red Docker.

---

## Paso 3.6: Ejecutar el flujo completo

```bash
# Job 1: ingesta dinámica (raw -> bronze)
uv run python -m src.processing.raw_to_bronze

# Job 2: limpieza con schema explícito (bronze -> silver)
uv run python -m src.processing.bronze_to_silver
```

> **Nota sobre `-m`**: ejecutar el archivo **como módulo** del paquete `src`, no como script suelto. Esto permite que los imports relativos (`from src.utils...`) funcionen sin problemas.

La primera vez tarda un poco más porque Spark descarga el **driver JDBC de Postgres** de Maven Central (~1 MB). Se ven mensajes del estilo:

```text
:: loading settings :: url = jar:file:...
org.postgresql#postgresql added as a dependency
```

Es normal. Los warnings sobre `incubator modules` y `NativeCodeLoader` también son **ruido normal en local** — ignorarlos.

---

## Paso 3.7: Validar el resultado

```bash
uv run python -c "
import polars as pl
from src.utils.config import settings

print('>> Conteos en raw_data:')
for tabla in ['clientes', 'cuentas', 'transacciones']:
    df = pl.read_database_uri(
        f'SELECT COUNT(*) AS n FROM raw_data.{tabla}',
        settings.pg_sqlalchemy_url,
    )
    print(f'   raw_data.{tabla}: {df[\"n\"][0]:,}')

print()
print('>> Conteos en staging:')
for tabla in ['clientes', 'cuentas', 'transacciones']:
    df = pl.read_database_uri(
        f'SELECT COUNT(*) AS n FROM staging.{tabla}',
        settings.pg_sqlalchemy_url,
    )
    print(f'   staging.{tabla}: {df[\"n\"][0]:,}')

print()
print('>> Transacciones fuera del país del cliente:')
df = pl.read_database_uri(
    'SELECT COUNT(*) AS n FROM staging.transacciones WHERE pais_distinto_cliente = true',
    settings.pg_sqlalchemy_url,
)
print(f'   {df[\"n\"][0]:,} (esperado ~5%, alrededor de 2400)')
"
```

### Resultados esperados

| Tabla | Bronze (`raw_data`) | Silver (`staging`) | Explicación |
|-------|--------------------:|-------------------:|-------------|
| `clientes` | 1.000 | 1.000 | Sin cambios. |
| `cuentas` | 2.037 | 2.037 | Sin cambios. |
| `transacciones` | 50.500 | ~49.000 | Descarta **~500 duplicados** + **~1.000 nulos en monto** = ~1.500 filas. |

**Transacciones con `pais_distinto_cliente = true`**: ~2.487 (≈5%). Coincide con el 5% inyectado en el generador de Fase 1.

> Si los conteos coinciden, **la detección de fraude potencial funciona end-to-end**. Es un hito grande.

---

## Paso 3.8: Commit

```bash
git add .
git status   # verificar que .env NO aparece
git commit -m "feat(fase3): pipeline raw->bronze->silver con spark y postgres"
```

---

## Lo que aprendiste en esta fase

- **Arquitectura Medallion real** con dos saltos: ingesta dinámica (bronze) + validación estricta (silver).
- **SparkSession** configurada profesionalmente (no la versión "hello world" de tutoriales).
- **Lectura dinámica de archivos** con `Path.glob` y descubrimiento automático.
- **Schemas explícitos como contrato** en Silver, separados en un módulo reutilizable.
- **Limpieza distribuida**: `dropDuplicates`, filtrado de nulos, validación de rangos.
- **Joins en Spark** con renombrado de columnas para evitar conflictos.
- **Escritura a Postgres vía JDBC** con manejo automático del driver.
- **Metadatos de auditoría** (`_archivo_origen`, `_ingesta_timestamp`) que en producción salvan la vida.
- **Decisión de diseño**: cuándo abstraer y cuándo no (lección de fondo más importante que el código).

---

## Decisión de diseño: ¿por qué Python y no YAML para los schemas?

Durante el desarrollo se exploró un refactor donde los schemas vivían en archivos YAML (`schemas/silver/clientes.yaml`, etc.) cargados dinámicamente por un loader. Se descartó. Esta es la razón.

### Tabla comparativa

| Aspecto | StructType en código | YAML + loader |
|---------|---------------------|---------------|
| **Quién puede editarlo** | Solo developers Python | Cualquiera que entienda YAML |
| **Type safety / autocomplete** | ✅ Sí | ❌ No |
| **Dependencias extras** | Ninguna | `pyyaml` + loader custom |
| **Errores se detectan en** | Import time | Runtime |
| **Reutilizable en dbt / GE** | ❌ Solo Python | ✅ Sí |
| **Curva de aprendizaje** | Baja | Media |
| **Cambios requieren** | Editar código | Editar config |

### Cuándo gana cada uno

**StructType en código gana cuando:**

- Pocas tablas (**< 10**) y son estables.
- Equipo pequeño, todos developers Python.
- Se quiere autocomplete y refactor seguro del IDE.
- Solo Spark consume los schemas.

**YAML gana cuando:**

- Muchas tablas (**20+**) o crecen rápido.
- Analistas/PMs no-developers sugieren cambios al schema.
- Los schemas se reutilizan en **dbt**, **Great Expectations**, generadores de docs.
- Múltiples equipos contribuyen.

### La trampa del "siempre es mejor"

Mucha gente aplica YAML porque suena "más profesional", sin necesitarlo. Termina con loaders complejos para gestionar 3 schemas, errores raros por typos detectados solo en runtime, code reviews donde hay que abrir 4 archivos en lugar de 1.

> **Regla profesional**: **agregar abstracciones cuando duela no tenerlas, no antes**.

Para este proyecto (3 tablas, una persona, solo Spark las consume), **Python módulo gana**. Cuando el proyecto crezca (10+ tablas, dbt + GE consumiendo schemas, otros equipos editándolos), YAML será la respuesta correcta. **Reconocerlo es señal de madurez profesional.**

---

## Troubleshooting: errores comunes encontrados

### Error 1: `relation "staging.clientes" does not exist`

**Síntoma:**

```text
RuntimeError: db error: ERROR: relation "staging.clientes" does not exist
```

**Causa:** se ejecutó la consulta de validación contra `staging.*` después de correr **solo el Job 1** (`raw_to_bronze`). El Job 1 escribe a `raw_data.*`, no a `staging.*`. La tabla `staging.clientes` solo existe **después de ejecutar el Job 2** (`bronze_to_silver`).

**Solución:** correr ambos jobs en orden:

```bash
uv run python -m src.processing.raw_to_bronze
uv run python -m src.processing.bronze_to_silver
```

**Lección:** el pipeline tiene **dos saltos**. Validar la capa correcta según qué job se haya ejecutado. Si solo se quiere ver bronze, consultar `raw_data.*`. Si se quiere ver silver, consultar `staging.*`.

---

### Error 2: schema `raw_data` no existe en Postgres

**Síntoma:**

```text
org.postgresql.util.PSQLException: ERROR: schema "raw_data" does not exist
```

**Causa:** el `init.sql` original de Fase 2 solo crea los schemas `bronze`, `staging` y `analytics`. El job de Fase 3 escribe a `raw_data`, que no fue creado.

**Solución:** crear el schema manualmente o actualizar `init.sql`:

```bash
docker compose exec postgres psql -U fintech_user -d fintech \
  -c "CREATE SCHEMA IF NOT EXISTS raw_data;"
```

**Lección:** el `init.sql` solo corre **la primera vez** que se levanta el contenedor (cuando el volumen está vacío). Para forzar que vuelva a correr, hay que destruir el volumen: `docker compose down -v` y volver a `up -d` (cuidado: borra los datos).

---

### Error 3: `fecha_registro` aparece como `timestamp` en bronze, no como `date`

**Síntoma:** al inspeccionar el schema descubierto por el Job 1:

```text
- fecha_registro: timestamp (NULL)
```

Cuando se esperaba `date`.

**Causa:** el generador de datos escribió `fecha_registro` con formato `2025-06-03 00:00:00` (con componente de hora). Spark, al inferir el tipo, vio el `00:00:00` y lo clasificó como **timestamp**. En cambio, `fecha_apertura` se escribió sin hora y sí salió como `date`.

**Esto NO es un bug**, es **exactamente el punto de la capa bronze**: absorbe lo que llega. En Silver se castea explícitamente a `DateType()` vía el contrato:

```python
StructField("fecha_registro", DateType(), nullable=True),
```

**Lección:** la inferencia de tipos depende del **formato exacto** del CSV. Por eso Silver tiene **schema explícito**: para garantizar tipos correctos sin importar cómo vino el dato.

---

### Error 4: refactor a YAML — `cuentas no cumple el contrato silver. Faltan: {'fecha_nacimiento', 'email', ...}`

**Síntoma:** al intentar el refactor con schemas en YAML, el loader cargó el schema de `clientes` para la tabla `cuentas`, causando un error misterioso donde las "columnas faltantes" eran en realidad las de otra tabla.

**Causa:** complejidad innecesaria. Un loader dinámico que parsea YAML añade **superficies de error** que no existen con un módulo Python plano (typos en nombres de archivo, caches mal invalidados, mappings de tipos incompletos).

**Solución:** se revirtió a `src/schemas/silver.py` (módulo Python con `StructType` directos). El error desapareció instantáneamente.

**Lección:** **over-engineering** es real y caro. La complejidad solo se justifica cuando el problema lo exige. Ver la sección [Decisión de diseño](#decisión-de-diseño-por-qué-python-y-no-yaml-para-los-schemas) arriba.

---

### Warnings que NO son errores

Estos aparecen casi siempre en local y son **ruido normal**. Ignorar:

```text
WARNING: Using incubator modules: jdk.incubator.vector
WARN Utils: Your hostname resolves to a loopback address: 127.0.1.1
WARN NativeCodeLoader: Unable to load native-hadoop library for your platform
```

Mientras el job termine exitosamente y los conteos sean razonables, **todo está bien**.

---

## Antes de la Fase 4

- [x] Ambos jobs corren sin errores.
- [x] Conteos en `raw_data` y `staging` son los esperados.
- [x] La bandera `pais_distinto_cliente` muestra ~5% de transacciones (≈2.400-2.500).
- [x] Commit hecho.

> Próximo paso: **Fase 4 — dbt** para modelar la capa **Gold** (`analytics`) con SQL puro, tests automáticos y documentación generada. Es la fase más divertida y la que más impacto visual tiene en el portafolio.