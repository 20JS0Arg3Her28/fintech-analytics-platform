"""Configuración centralizada del proyecto.

Lee variables de entorno desde .env y las valida con Pydantic.
Patrón usado en producción para evitar hardcoding y permitir
diferentes configs por entorno (dev, staging, prod).
"""

from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


def _find_project_root() -> Path:
    """Sube directorios hasta encontrar pyproject.toml (la raíz del proyecto).

    Más robusto que parents[N]: no depende de dónde esté este archivo.
    """
    current = Path(__file__).resolve()
    for parent in current.parents:
        if (parent / "pyproject.toml").exists():
            return parent
    raise RuntimeError("No se encontró pyproject.toml; ¿estás dentro del proyecto?")


PROJECT_ROOT = _find_project_root()

class Settings(BaseSettings):
    """Configuración global del proyecto, cargada desde variables de entorno."""

    model_config = SettingsConfigDict(
        env_file=PROJECT_ROOT / ".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # ============ Paths ============
    project_root: Path = PROJECT_ROOT
    data_raw: Path = PROJECT_ROOT / "data" / "raw"
    data_bronze: Path = PROJECT_ROOT / "data" / "bronze"

    # ============ Configuración de datos ============
    num_clientes: int = Field(default=1_000, description="Cantidad de clientes a generar")
    num_cuentas_por_cliente: int = Field(default=3)
    num_transacciones: int = Field(default=50_000)
    random_seed: int = Field(default=42, description="Semilla para reproducibilidad")

    # ============ Postgres (para fase 2) ============
    pg_host: str = Field(default="localhost")
    pg_port: int = Field(default=5432)
    pg_database: str = Field(default="fintech")
    pg_user: str = Field(default="fintech_user")
    pg_password: str = Field(default="fintech_pass")

    @property
    def pg_jdbc_url(self) -> str:
        """URL JDBC para que spark se conecte a Postgres."""
        return f"jdbc:postgresql://{self.pg_host}:{self.pg_port}/{self.pg_database}"

    @property
    def pg_sqlalchemy_url(self) -> str:
        """URL SQLAlchemy para conexiones Python directa."""
        return (
            f"postgresql+psycopg2://{self.pg_user}:{self.pg_password}"
            f"@{self.pg_host}:{self.pg_port}/{self.pg_database}"
        )

# Instancia global (Singleton).
settings = Settings()
