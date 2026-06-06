"""Carga schemas desde archivos YAML y los convierte a StructType de Spark.

Patron 'configuration as data': el schema deja de estar quemado en codigo Python
y vive como configuracion versionada en Git, editable sin tocar codigo.
"""
from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml
from pyspark.sql.types import (
    BooleanType,
    DataType,
    DateType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from src.utils.config import settings

# Mapeo de strings YAML -> tipos Spark.
# Si necesitas un tipo nuevo (decimal, array, struct), lo agregas aqui.
_TYPE_MAP: dict[str, DataType] = {
    "string":    StringType(),
    "int":       IntegerType(),
    "integer":   IntegerType(),
    "long":      LongType(),
    "double":    DoubleType(),
    "float":     DoubleType(),
    "boolean":   BooleanType(),
    "bool":      BooleanType(),
    "date":      DateType(),
    "timestamp": TimestampType(),
}


SCHEMAS_DIR = settings.project_root / "schemas"


class SchemaError(Exception):
    """Error al cargar o parsear un schema YAML."""


def _yaml_a_struct(config: dict[str, Any], archivo: Path) -> StructType:
    """Convierte un dict de YAML a StructType, validando que los tipos existan."""
    if "fields" not in config:
        raise SchemaError(f"{archivo}: falta la clave 'fields'")

    campos = []
    for f in config["fields"]:
        nombre = f.get("name")
        tipo_str = f.get("type")
        nullable = f.get("nullable", True)

        if not nombre or not tipo_str:
            raise SchemaError(
                f"{archivo}: cada field requiere 'name' y 'type'. Recibido: {f}"
            )

        tipo = _TYPE_MAP.get(tipo_str.lower())
        if tipo is None:
            raise SchemaError(
                f"{archivo}: tipo desconocido '{tipo_str}' en campo '{nombre}'. "
                f"Tipos validos: {sorted(_TYPE_MAP.keys())}"
            )

        campos.append(StructField(nombre, tipo, nullable=nullable))

    return StructType(campos)


@lru_cache(maxsize=32)
def cargar_schema(capa: str, tabla: str) -> StructType:
    """Carga un schema individual: cargar_schema('silver', 'clientes')."""
    ruta = SCHEMAS_DIR / capa / f"{tabla}.yaml"
    if not ruta.exists():
        raise SchemaError(f"Schema no encontrado: {ruta}")

    config = yaml.safe_load(ruta.read_text(encoding="utf-8"))
    return _yaml_a_struct(config, ruta)


def cargar_schemas_de_capa(capa: str) -> dict[str, StructType]:
    """Carga TODOS los schemas de una capa: cargar_schemas_de_capa('silver').

    Returns:
        Diccionario {nombre_tabla: StructType}, indexado por el nombre del archivo.
    """
    carpeta = SCHEMAS_DIR / capa
    if not carpeta.exists():
        raise SchemaError(f"Carpeta de schemas no existe: {carpeta}")

    schemas = {}
    for archivo in sorted(carpeta.glob("*.yaml")):
        tabla = archivo.stem
        schemas[tabla] = cargar_schema(capa, tabla)

    if not schemas:
        raise SchemaError(f"No se encontraron archivos .yaml en {carpeta}")

    return schemas
