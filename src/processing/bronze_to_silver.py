"""Job: bronze -> silver.

Lee tablas de raw_data, aplica los schemas declarados en schemas/silver/*.yaml,
limpia, enriquece y escribe a staging.

Refactor: los schemas viven en YAML, no en codigo Python. Agregar una tabla
nueva es agregar un archivo schemas/silver/nueva.yaml, sin tocar este archivo.
"""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F  # noqa: N812
from pyspark.sql.types import StructType

from src.utils.config import settings
from src.utils.schema_loader import cargar_schemas_de_capa
from src.utils.spark_session import build_spark_session


# ============ Lectura desde bronze ============
def leer_de_bronze(spark: SparkSession, tabla: str, schema: StructType) -> DataFrame:
    """Lee tabla de raw_data, valida que existan las columnas y castea al schema."""
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

    # Quitar columnas de auditoria
    columnas_meta = ["_archivo_origen", "_ingesta_timestamp", "_ingesta_fecha"]
    df_sin_meta = df_bronze.drop(*[c for c in columnas_meta if c in df_bronze.columns])

    # Validar contrato
    esperadas = {c.name for c in schema.fields}
    presentes = set(df_sin_meta.columns)
    faltantes = esperadas - presentes
    if faltantes:
        raise ValueError(
            f"raw_data.{tabla} no cumple el contrato silver. Faltan: {faltantes}"
        )

    # Castear al tipo del contrato
    df_casteado = df_sin_meta
    for campo in schema.fields:
        df_casteado = df_casteado.withColumn(
            campo.name, F.col(campo.name).cast(campo.dataType)
        )

    return df_casteado.select(*[c.name for c in schema.fields])


# ============ Limpieza especifica de transacciones ============
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
    """Une transacciones con cuenta y cliente."""
    print(">> Enriqueciendo con joins...")

    clientes_r = (
        clientes
        .withColumnRenamed("pais", "pais_cliente")
        .withColumnRenamed("ciudad", "ciudad_cliente")
    )

    return (
        transacciones
        .join(cuentas, on="cuenta_id", how="inner")
        .join(clientes_r, on="cliente_id", how="inner")
        .withColumn(
            "pais_distinto_cliente",
            F.col("pais_tx") != F.col("pais_cliente"),
        )
    )


# ============ Escritura ============
def escribir_a_staging(df: DataFrame, tabla: str) -> None:
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
    print("  Job: bronze -> silver (schemas desde YAML)")
    print("=" * 60 + "\n")

    spark = build_spark_session(app_name="BronzeToSilver")
    spark.sparkContext.setLogLevel("WARN")

    # Carga TODOS los schemas declarados en schemas/silver/
    schemas = cargar_schemas_de_capa("silver")
    print(f">> Schemas cargados: {sorted(schemas.keys())}\n")

    # Lectura tabla por tabla, aplicando su contrato
    dataframes = {
        tabla: leer_de_bronze(spark, tabla, schema)
        for tabla, schema in schemas.items()
    }

    # Procesamiento especifico de transacciones
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