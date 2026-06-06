"""Job: bronze -> silver.

Lee las tablas crudas del schema raw_data, aplica schema explicito (castea tipos),
limpia, valida, enriquece con joins, y escribe al schema staging.

Filosofia: aqui hacemos cumplir el CONTRATO. Si bronze cambio, fallamos ruidosamente.
"""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F  # noqa: N812
from pyspark.sql.types import (
    DateType,
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from src.utils.config import settings
from src.utils.spark_session import build_spark_session

# ============ Schemas explicitos (CONTRATO de silver) ============
SCHEMA_CLIENTES = StructType([
    StructField("cliente_id", StringType(), nullable=False),
    StructField("nombre_completo", StringType(), nullable=True),
    StructField("email", StringType(), nullable=True),
    StructField("telefono", StringType(), nullable=True),
    StructField("pais", StringType(), nullable=True),
    StructField("ciudad", StringType(), nullable=True),
    StructField("fecha_registro", DateType(), nullable=True),
    StructField("segmento", StringType(), nullable=True),
    StructField("fecha_nacimiento", DateType(), nullable=True),
])

SCHEMA_CUENTAS = StructType([
    StructField("cuenta_id", StringType(), nullable=False),
    StructField("cliente_id", StringType(), nullable=False),
    StructField("tipo_cuenta", StringType(), nullable=True),
    StructField("moneda", StringType(), nullable=True),
    StructField("fecha_apertura", DateType(), nullable=True),
    StructField("estado", StringType(), nullable=True),
    StructField("saldo_inicial", DoubleType(), nullable=True),
])

SCHEMA_TRANSACCIONES = StructType([
    StructField("transaccion_id", StringType(), nullable=False),
    StructField("cuenta_id", StringType(), nullable=False),
    StructField("timestamp_tx", TimestampType(), nullable=True),
    StructField("monto", DoubleType(), nullable=True),
    StructField("tipo_tx", StringType(), nullable=True),
    StructField("comercio", StringType(), nullable=True),
    StructField("categoria_comercio", StringType(), nullable=True),
    StructField("pais_tx", StringType(), nullable=True),
    StructField("ciudad_tx", StringType(), nullable=True),
    StructField("canal", StringType(), nullable=True),
])


# ============ Lectura desde bronze con validacion ============
def leer_de_bronze(spark: SparkSession, tabla: str, schema_esperado: StructType) -> DataFrame:
    """Lee tabla de raw_data, valida columnas y castea al schema de silver.

    Si una columna esperada no existe en bronze, falla con error claro.
    """
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

    # Descartar columnas de metadatos de auditoria
    columnas_meta = ["_archivo_origen", "_ingesta_timestamp", "_ingesta_fecha"]
    df_sin_meta = df_bronze.drop(*[c for c in columnas_meta if c in df_bronze.columns])

    # Validacion: faltan columnas requeridas?
    columnas_esperadas = {c.name for c in schema_esperado.fields}
    columnas_presentes = set(df_sin_meta.columns)
    faltantes = columnas_esperadas - columnas_presentes
    if faltantes:
        raise ValueError(
            f"raw_data.{tabla} no tiene columnas requeridas por silver: {faltantes}. "
            f"Encontradas: {sorted(columnas_presentes)}"
        )

    # Castear cada columna al tipo del contrato silver
    df_casteado = df_sin_meta
    for campo in schema_esperado.fields:
        df_casteado = df_casteado.withColumn(
            campo.name, F.col(campo.name).cast(campo.dataType)
        )

    return df_casteado.select(*[c.name for c in schema_esperado.fields])


# ============ Limpieza ============
def limpiar_transacciones(transacciones: DataFrame) -> DataFrame:
    """Aplica reglas de calidad a transacciones."""
    print(">> Limpiando transacciones...")
    n_inicial = transacciones.count()

    sin_duplicados = transacciones.dropDuplicates(["transaccion_id"])
    sin_nulos = sin_duplicados.filter(F.col("monto").isNotNull())
    valido = sin_nulos.filter(F.col("monto") > 0)

    enriquecido = (
        valido
        .withColumn("anio_tx", F.year("timestamp_tx"))
        .withColumn("mes_tx", F.month("timestamp_tx"))
        .withColumn("dia_tx", F.dayofmonth("timestamp_tx"))
        .withColumn("hora_tx", F.hour("timestamp_tx"))
        .withColumn("es_finde", F.dayofweek("timestamp_tx").isin([1, 7]))
    )

    n_final = enriquecido.count()
    print(f"   {n_inicial:,} -> {n_final:,} filas ({n_inicial - n_final:,} descartadas)")
    return enriquecido


# ============ Enriquecimiento con joins ============
def enriquecer_con_joins(
    transacciones: DataFrame,
    cuentas: DataFrame,
    clientes: DataFrame,
) -> DataFrame:
    """Combina transacciones con datos de cuenta y cliente."""
    print(">> Enriqueciendo con joins...")

    clientes_renombrados = (
        clientes
        .withColumnRenamed("pais", "pais_cliente")
        .withColumnRenamed("ciudad", "ciudad_cliente")
    )

    enriquecido = (
        transacciones
        .join(cuentas, on="cuenta_id", how="inner")
        .join(clientes_renombrados, on="cliente_id", how="inner")
        .withColumn(
            "pais_distinto_cliente",
            F.col("pais_tx") != F.col("pais_cliente"),
        )
    )

    print(f"   {enriquecido.count():,} filas tras joins")
    return enriquecido


# ============ Escritura a staging ============
def escribir_a_staging(df: DataFrame, tabla: str) -> None:
    """Escribe al schema staging de Postgres via JDBC."""
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


def main() -> None:
    print("\n" + "=" * 60)
    print("  Job: bronze -> silver")
    print("=" * 60 + "\n")

    spark = build_spark_session(app_name="BronzeToSilver")
    spark.sparkContext.setLogLevel("WARN")

    # Leer las 3 tablas de bronze aplicando el contrato silver
    clientes = leer_de_bronze(spark, "clientes", SCHEMA_CLIENTES)
    cuentas = leer_de_bronze(spark, "cuentas", SCHEMA_CUENTAS)
    transacciones = leer_de_bronze(spark, "transacciones", SCHEMA_TRANSACCIONES)

    # Limpieza
    transacciones_limpias = limpiar_transacciones(transacciones)

    # Enriquecimiento con joins
    transacciones_silver = enriquecer_con_joins(
        transacciones_limpias, cuentas, clientes
    )
    transacciones_silver.cache()

    # Vista previa
    print("\n>> Muestra de transacciones silver:")
    transacciones_silver.select(
        "transaccion_id", "monto", "tipo_tx",
        "pais_cliente", "pais_tx", "pais_distinto_cliente",
    ).show(5, truncate=False)

    # Escribir a staging
    escribir_a_staging(clientes, "clientes")
    escribir_a_staging(cuentas, "cuentas")
    escribir_a_staging(transacciones_silver, "transacciones")

    print("\n" + "=" * 60)
    print("  Job completado.")
    print("=" * 60 + "\n")

    spark.stop()


if __name__ == "__main__":
    main()