""" Builder centralizado de SparkSession.

Encapsula toda la configuracion para que cada job sea limpio y consistente.
"""
from __future__ import annotations

from pyspark.sql import SparkSession


def build_spark_session(app_name: str = "FintechAnalytics") -> SparkSession:
    """Construye un SparkSession configurada para correr en local.

    Configuracion pensada para una laptop de desarrollo:
    - Modo local con todos los nucleos disponibles.
    - JDBC de Postgres precargado.
    - Adaptative Query Execution activado.
    - Zona horaria local para evitar bugs de fechas.
    - shuffle.partitions bajado a 8 (default 200 es excesivo en local).
    """
    return (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]") #  corre en local usando todos los núcleos
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3") # descarga el driver JDBC de Postgres automáticamente
        .config("spark.sql.adaptive.enabled", "true")  # AQE: Spark optimiza en tiempo de ejecución
        .config("spark.sql.adaptive.coalescePartitions.enableds", "true")
        .config("spark.sql.shuffle.partitions", "8") # por defecto es 200, demasiado para local
        .config("spark.sql.session.timeZone", "America/Guatemala") # crítico para fechas correctas
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") #  Kryo es más rápido que el default Java
        .config("spark.driver.memory", "2g") # suficiente RAM para el driver
        .config("spark.ui.showConsoleProgress", "false") # silencia las barras de progreso ruidosas
        .getOrCreate()
    )