# Fase 1: Estructura del proyecto y generador de datos sintéticos

## Objetivos de esta fase

Al terminar la Fase 1 tendrás:

- Una **estructura de carpetas profesional** que cualquier ingeniero senior reconocería.
- **`pyproject.toml`** con dependencias agrupadas por propósito.
- Un módulo de **configuración centralizada** (patrón que se usa en producción).
- Un **generador de datos realistas** de fintech usando **Faker** + **Polars**.
- **Datos crudos** en `data/raw/` listos para que Spark los procese en la Fase 3.

> **Sin Docker. Sin Spark. Sin Airflow. Una sola cosa a la vez.**
> Si se intenta todo de golpe y algo falla, no se sabe dónde mirar. Construir incremental es el método profesional.

---

## Paso 1.1: Crear la estructura de carpetas

Desde la raíz del proyecto:

```bash
# Carpetas principales
mkdir -p data/{raw,bronze,generators}
mkdir -p src/{ingestion,processing,utils}
mkdir -p dbt_fintech/models
mkdir -p dags
mkdir -p docker/{airflow,spark,postgres}
mkdir -p tests
mkdir -p notebooks

# Archivos vacíos importantes
touch src/__init__.py
touch src/ingestion/__init__.py
touch src/processing/__init__.py
touch src/utils/__init__.py
touch tests/__init__.py
touch README.md
touch .env.example
touch .gitignore
```

### ¿Por qué los archivos `__init__.py` vacíos?

En Python, una carpeta se vuelve un **paquete importable** solo si contiene un `__init__.py` (puede estar vacío). Sin esto, no se puede escribir `from src.utils.config import settings` en otro archivo.

### ¿Por qué `.env.example` y no `.env` directamente?

El archivo **`.env`** contiene credenciales sensibles (passwords, API keys) y **nunca debe subirse a Git**. El **`.env.example`** es la plantilla pública que muestra qué variables existen, pero con valores falsos. Cuando alguien clone el repo, copia el `.example` y le pone sus propios valores.

> Es un patrón estándar de la industria. Aprenderlo aquí ahorra meter passwords accidentalmente en GitHub (uno de los errores más comunes y vergonzosos).

---

## Paso 1.2: Configurar `.gitignore`

Crear `.gitignore` en la raíz:

```gitignore
# Python
__pycache__/
*.py[cod]
*$py.class
*.so
.Python
.venv/
venv/
env/
*.egg-info/
dist/
build/

# uv
.uv/

# Entorno (NUNCA commitear)
.env
.env.local
*.pem
*.key

# Datos (NO subir datos al repo)
data/raw/*.csv
data/raw/*.json
data/raw/*.parquet
data/bronze/
data/silver/
data/gold/
!data/raw/.gitkeep
!data/bronze/.gitkeep

# IDEs
.vscode/
.idea/
*.swp
.DS_Store

# Jupyter
.ipynb_checkpoints/
*.ipynb_checkpoints

# dbt
dbt_fintech/target/
dbt_fintech/dbt_packages/
dbt_fintech/logs/

# Airflow
dags/__pycache__/
airflow.db
airflow.cfg
airflow-webserver.pid
logs/

# Spark
spark-warehouse/
metastore_db/
derby.log

# Tests
.pytest_cache/
.coverage
htmlcov/
```

### ¿Por qué no commitear datos?

Tres razones:

1. **Tamaño**: archivos de datos pueden ser GBs. Git no está diseñado para eso (existe **Git LFS** para casos especiales).
2. **Privacidad**: datos reales pueden contener información sensible.
3. **Reproducibilidad**: si los datos se generan con un script, **el script** es lo que debe estar versionado, no su salida.

### El truco del `.gitkeep`

```bash
# Crea archivos vacíos para que Git rastree las carpetas (Git ignora carpetas vacías)
touch data/raw/.gitkeep
touch data/bronze/.gitkeep
```

`.gitkeep` no es una convención oficial de Git, es un nombre acordado por la comunidad. Como Git no rastrea carpetas vacías (solo archivos), poner un archivo vacío dentro hace que la carpeta exista cuando alguien clone el repo.

---

## Paso 1.3: Configurar `pyproject.toml`

```toml
[project]
name = "fintech-analytics-platform"
version = "0.1.0"
description = "Plataforma end-to-end de analytics fintech con Spark, dbt y Airflow"
authors = [
    {name = "Tu Nombre", email = "tu@email.com"}
]
readme = "README.md"
requires-python = ">=3.11,<3.13"

dependencies = [
    "polars>=1.0.0",
    "faker>=25.0.0",
    "python-dotenv>=1.0.0",
    "pydantic>=2.7.0",
    "pydantic-settings>=2.3.0",
    "pyarrow>=15.0.0",
]

[dependency-groups]
spark = [
    "pyspark==4.1.1",
    "psycopg2-binary>=2.9.9",
]

dbt = [
    "dbt-core>=1.8.0",
    "dbt-postgres>=1.8.0",
]

dev = [
    "ruff>=0.5.0",
    "pytest>=8.0.0",
    "pytest-cov>=5.0.0",
    "ipython>=8.20.0",
    "jupyterlab>=4.0.0",
]

[tool.ruff]
line-length = 100
target-version = "py311"

[tool.ruff.lint]
select = ["E", "F", "I", "N", "W", "UP", "B", "C4", "RET", "SIM"]
ignore = ["E501"]

[tool.pytest.ini_options]
testpaths = ["tests"]
python_files = ["test_*.py"]

[tool.hatch.build.targets.wheel]
packages = ["src"]

[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"
```

### Explicación de cada sección

- **`[project]`**: metadata estándar. `requires-python = ">=3.11,<3.13"` porque **Spark 4** no soporta Python 3.13 todavía.
- **`dependencies`**: las que se instalan siempre. Solo cosas que necesita el código base.
- **`[dependency-groups]`**: dependencias opcionales agrupadas por propósito (mucho mejor que un solo `requirements.txt` gigante):
  - **`spark`**: instalar solo si se va a correr Spark localmente.
  - **`dbt`**: solo si se van a desarrollar modelos.
  - **`dev`**: herramientas de desarrollo (linter, tests, jupyter).
- **`[tool.ruff]`**: **Ruff** es el linter/formatter moderno de Python (reemplaza Black, isort, flake8 todos juntos, escrito en **Rust**). Más rápido y más estricto.
- **`[tool.pytest.ini_options]`**: dice a pytest dónde buscar tests.
- **`[tool.hatch.build...]`** + **`[build-system]`**: declara `src/` como paquete instalable. Necesario para que `from src.utils.config import settings` funcione en cualquier script.

### ¿Por qué Pydantic?

**Pydantic** es una librería para validar configuración y datos usando type hints. Es el **estándar de facto** en Python moderno; lo usan **FastAPI**, **LangChain**, **OpenAI SDK**, prácticamente todo.

### Instalación

```bash
uv sync --all-groups
uv pip install -e .
```

- **`uv sync --all-groups`** crea un `.venv` con todas las dependencias y genera `uv.lock` (lockfile con versiones exactas, **sí se commitea**).
- **`uv pip install -e .`** instala el proyecto en modo **editable**: cualquier cambio en `src/` se refleja inmediatamente sin reinstalar.

---

## Paso 1.4: Configuración centralizada

**Nunca pongas credenciales o configuración hardcodeada en los scripts.** Centralizar la config tiene varios beneficios:

- **Un solo lugar** para cambiar valores (puerto del DB, nombre de schema, etc.).
- **Validación automática** con Pydantic.
- **Carga desde variables de entorno** (compatible con Docker, AWS, etc.).
- **Type hints** que el IDE entiende.

Crear `src/utils/config.py`:

```python
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

    Más robusto que parents[N]: no depende de dónde esté ubicado este archivo.
    """
    current = Path(__file__).resolve()
    for parent in current.parents:
        if (parent / "pyproject.toml").exists():
            return parent
    raise RuntimeError("No se encontró pyproject.toml; ¿estás dentro del proyecto?")


PROJECT_ROOT = _find_project_root()


class Settings(BaseSettings):
    """Configuración global del proyecto."""

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

    # ============ Generación de datos ============
    num_clientes: int = Field(default=1_000, description="Cantidad de clientes a generar")
    num_cuentas_por_cliente_max: int = Field(default=3)
    num_transacciones: int = Field(default=50_000)
    random_seed: int = Field(default=42, description="Semilla para reproducibilidad")

    # ============ Postgres (para Fase 2) ============
    pg_host: str = Field(default="localhost")
    pg_port: int = Field(default=5432)
    pg_database: str = Field(default="fintech")
    pg_user: str = Field(default="fintech_user")
    pg_password: str = Field(default="fintech_pass")

    @property
    def pg_jdbc_url(self) -> str:
        """URL JDBC para que Spark se conecte a Postgres."""
        return f"jdbc:postgresql://{self.pg_host}:{self.pg_port}/{self.pg_database}"

    @property
    def pg_sqlalchemy_url(self) -> str:
        """URL SQLAlchemy para conexiones Python directas."""
        return (
            f"postgresql+psycopg2://{self.pg_user}:{self.pg_password}"
            f"@{self.pg_host}:{self.pg_port}/{self.pg_database}"
        )


# Instancia global (singleton). Importar esto en cualquier script.
settings = Settings()
```

### Cosas clave de este código

- **`_find_project_root()`**: en lugar de contar niveles con `parents[N]` (frágil), busca hacia arriba hasta encontrar `pyproject.toml`. Sin importar dónde se ejecute el script, **siempre** encuentra la raíz correcta. Es el patrón que usan **pytest** y **dbt** para localizar la raíz del proyecto.
- **`BaseSettings`**: clase de Pydantic que lee variables de entorno automáticamente. Si hay `PG_PASSWORD=secreto` en `.env`, se carga automáticamente en `settings.pg_password`.
- **`Field(default=...)`**: define valores por defecto y documentación.
- **`@property`**: las URLs JDBC y SQLAlchemy se calculan a partir de los otros campos. **Derivar, no duplicar.**
- **`settings = Settings()`**: instancia global. Otros scripts solo hacen `from src.utils.config import settings`.

### `.env.example`

```bash
# Postgres
PG_HOST=localhost
PG_PORT=5432
PG_DATABASE=fintech
PG_USER=fintech_user
PG_PASSWORD=fintech_pass

# Generación de datos
NUM_CLIENTES=1000
NUM_TRANSACCIONES=50000
RANDOM_SEED=42
```

Copiar para uso local:

```bash
cp .env.example .env
```

---

## Paso 1.5: El modelo de datos

Antes de generar datos, **diseñar el modelo**. Esto es lo que un ingeniero de datos hace **antes de tocar código**.

### Las 3 entidades

**`clientes`** — quién hace las transacciones:

- `cliente_id`: identificador único.
- `nombre_completo`, `email`, `telefono`.
- `pais`, `ciudad`.
- `fecha_registro`: cuándo se registró en el banco.
- `segmento`: `PERSONAL`, `PREMIUM`, `BUSINESS`.
- `fecha_nacimiento`.

**`cuentas`** — un cliente puede tener varias cuentas:

- `cuenta_id`: identificador único.
- `cliente_id`: **FK** a `clientes`.
- `tipo_cuenta`: `AHORROS`, `CORRIENTE`, `CREDITO`.
- `moneda`: `GTQ`, `USD`, `EUR`.
- `fecha_apertura`.
- `estado`: `ACTIVA`, `CERRADA`, `CONGELADA`.
- `saldo_inicial`.

**`transacciones`** — los movimientos (la tabla "grande" del proyecto):

- `transaccion_id`: identificador único.
- `cuenta_id`: **FK** a `cuentas`.
- `timestamp_tx`: cuándo ocurrió.
- `monto`: positivo siempre; el tipo dice si es entrada o salida.
- `tipo_tx`: `DEPOSITO`, `RETIRO`, `TRANSFERENCIA`, `PAGO_TARJETA`, `COMPRA_ONLINE`.
- `comercio`: nombre del comercio (para compras).
- `categoria_comercio`: `SUPERMERCADO`, `RESTAURANTE`, `GASOLINERA`, etc.
- `pais_tx`: dónde se hizo (puede diferir del país del cliente → **posible fraude**).
- `ciudad_tx`.
- `canal`: `APP`, `WEB`, `CAJERO`, `SUCURSAL`, `POS`.

### ¿Por qué este modelo?

- Permite **joins realistas** (`clientes` ↔ `cuentas` ↔ `transacciones`).
- Tiene **dimensiones** para agrupar (segmento, país, categoría, canal).
- Tiene **señales de fraude** posibles (transacciones fuera del país, horarios atípicos, montos grandes).
- Es **escalable**: aumentar `NUM_TRANSACCIONES` a 5M para estresar Spark.

---

## Paso 1.6: El generador de datos

Crear `data/generators/generate_data.py`:

```python
"""Generador de datos sintéticos para la plataforma fintech.

Usa Faker para datos realistas (nombres, emails, ciudades) y
Polars para construir los DataFrames y escribir a CSV.

Diseñado para ser reproducible: misma semilla = mismos datos.
Esto es CRÍTICO para tests y para debugging de pipelines.
"""
from __future__ import annotations

import random
from datetime import datetime, timedelta

import polars as pl
from faker import Faker

from src.utils.config import settings


# ============ Setup ============
# Faker no tiene locale es_GT; es_MX es el más cercano para nombres latinos.
# Los datos de dominio (países, ciudades, comercios) son constantes propias más abajo.
fake = Faker("es_MX")
Faker.seed(settings.random_seed)
random.seed(settings.random_seed)


# ============ Constantes del dominio ============
PAISES = ["Guatemala", "México", "El Salvador", "Honduras", "Costa Rica", "Estados Unidos"]
PAISES_PESOS = [0.55, 0.15, 0.10, 0.08, 0.07, 0.05]  # 55% de clientes de Guate

CIUDADES_POR_PAIS = {
    "Guatemala": ["Ciudad de Guatemala", "Quetzaltenango", "Antigua", "Cobán", "Escuintla"],
    "México": ["Ciudad de México", "Guadalajara", "Monterrey"],
    "El Salvador": ["San Salvador", "Santa Ana"],
    "Honduras": ["Tegucigalpa", "San Pedro Sula"],
    "Costa Rica": ["San José", "Cartago"],
    "Estados Unidos": ["Los Angeles", "Houston", "Miami"],
}

SEGMENTOS = ["PERSONAL", "PREMIUM", "BUSINESS"]
SEGMENTOS_PESOS = [0.75, 0.15, 0.10]

TIPOS_CUENTA = ["AHORROS", "CORRIENTE", "CREDITO"]
TIPOS_CUENTA_PESOS = [0.60, 0.30, 0.10]

MONEDAS = ["GTQ", "USD", "EUR"]
MONEDAS_PESOS = [0.75, 0.20, 0.05]

ESTADOS_CUENTA = ["ACTIVA", "CERRADA", "CONGELADA"]
ESTADOS_CUENTA_PESOS = [0.90, 0.07, 0.03]

TIPOS_TX = ["DEPOSITO", "RETIRO", "TRANSFERENCIA", "PAGO_TARJETA", "COMPRA_ONLINE"]
TIPOS_TX_PESOS = [0.15, 0.20, 0.25, 0.25, 0.15]

CANALES = ["APP", "WEB", "CAJERO", "SUCURSAL", "POS"]
CANALES_PESOS = [0.40, 0.20, 0.15, 0.05, 0.20]

CATEGORIAS_COMERCIO = {
    "SUPERMERCADO": ["La Torre", "Walmart", "Paiz", "Despensa Familiar"],
    "RESTAURANTE": ["Pollo Campero", "McDonalds", "Sarita", "Domino's"],
    "GASOLINERA": ["Puma", "Texaco", "Shell"],
    "FARMACIA": ["Farmacia Galeno", "Cruz Verde", "Batres"],
    "TECNOLOGIA": ["Amazon", "Apple Store", "MercadoLibre"],
    "TRANSPORTE": ["Uber", "InDriver", "Aerolínea Avianca"],
    "ENTRETENIMIENTO": ["Netflix", "Spotify", "Cinemark"],
}


# ============ Generadores ============
def generar_clientes(n: int) -> pl.DataFrame:
    """Genera un DataFrame de clientes con datos realistas."""
    print(f"  Generando {n:,} clientes...")

    paises = random.choices(PAISES, weights=PAISES_PESOS, k=n)
    ciudades = [random.choice(CIUDADES_POR_PAIS[p]) for p in paises]

    # Fechas de registro distribuidas en los últimos 3 años
    fecha_min = datetime(2022, 1, 1)
    fecha_max = datetime(2025, 12, 31)
    rango_dias = (fecha_max - fecha_min).days

    return pl.DataFrame({
        "cliente_id": [f"CLI{i:06d}" for i in range(1, n + 1)],
        "nombre_completo": [fake.name() for _ in range(n)],
        "email": [fake.email() for _ in range(n)],
        "telefono": [fake.phone_number() for _ in range(n)],
        "pais": paises,
        "ciudad": ciudades,
        "fecha_registro": [
            fecha_min + timedelta(days=random.randint(0, rango_dias))
            for _ in range(n)
        ],
        "segmento": random.choices(SEGMENTOS, weights=SEGMENTOS_PESOS, k=n),
        "fecha_nacimiento": [
            fake.date_of_birth(minimum_age=18, maximum_age=80)
            for _ in range(n)
        ],
    })


def generar_cuentas(clientes: pl.DataFrame) -> pl.DataFrame:
    """Cada cliente tiene entre 1 y N cuentas."""
    cliente_ids = clientes["cliente_id"].to_list()
    print(f"  Generando cuentas para {len(cliente_ids):,} clientes...")

    rows = []
    cuenta_counter = 1

    for cliente_id in cliente_ids:
        num_cuentas = random.randint(1, settings.num_cuentas_por_cliente_max)
        for _ in range(num_cuentas):
            rows.append({
                "cuenta_id": f"ACC{cuenta_counter:08d}",
                "cliente_id": cliente_id,
                "tipo_cuenta": random.choices(TIPOS_CUENTA, weights=TIPOS_CUENTA_PESOS, k=1)[0],
                "moneda": random.choices(MONEDAS, weights=MONEDAS_PESOS, k=1)[0],
                "fecha_apertura": fake.date_between(start_date="-3y", end_date="today"),
                "estado": random.choices(ESTADOS_CUENTA, weights=ESTADOS_CUENTA_PESOS, k=1)[0],
                "saldo_inicial": round(random.uniform(100, 50_000), 2),
            })
            cuenta_counter += 1

    return pl.DataFrame(rows)


def generar_transacciones(
    cuentas: pl.DataFrame,
    clientes: pl.DataFrame,
    n: int,
) -> pl.DataFrame:
    """Genera transacciones. Inyecta datos 'sucios' a propósito."""
    print(f"  Generando {n:,} transacciones...")

    cuentas_activas = cuentas.filter(pl.col("estado") == "ACTIVA")
    cuenta_ids = cuentas_activas["cuenta_id"].to_list()

    # Mapa cuenta -> país del cliente (para detectar fraude después)
    cuenta_a_pais = dict(
        cuentas_activas
        .join(clientes, on="cliente_id")
        .select("cuenta_id", "pais")
        .iter_rows()
    )

    fecha_min = datetime(2025, 1, 1)
    fecha_max = datetime(2025, 12, 31, 23, 59, 59)
    rango_segundos = int((fecha_max - fecha_min).total_seconds())

    rows = []
    for i in range(1, n + 1):
        cuenta_id = random.choice(cuenta_ids)
        tipo_tx = random.choices(TIPOS_TX, weights=TIPOS_TX_PESOS, k=1)[0]
        pais_cliente = cuenta_a_pais[cuenta_id]

        # 95% de transacciones en el país del cliente, 5% fuera (fraude potencial)
        if random.random() < 0.95:
            pais_tx = pais_cliente
        else:
            paises_extranjeros = [p for p in PAISES if p != pais_cliente]
            pais_tx = random.choice(paises_extranjeros)

        ciudad_tx = random.choice(CIUDADES_POR_PAIS[pais_tx])

        # Monto depende del tipo
        if tipo_tx == "DEPOSITO":
            monto = round(random.uniform(50, 10_000), 2)
        elif tipo_tx == "RETIRO":
            monto = round(random.uniform(20, 2_000), 2)
        elif tipo_tx == "TRANSFERENCIA":
            monto = round(random.uniform(10, 50_000), 2)
        else:  # PAGO_TARJETA, COMPRA_ONLINE
            monto = round(random.uniform(5, 1_500), 2)

        # Comercio: solo para compras y pagos
        if tipo_tx in ("PAGO_TARJETA", "COMPRA_ONLINE"):
            categoria = random.choice(list(CATEGORIAS_COMERCIO.keys()))
            comercio = random.choice(CATEGORIAS_COMERCIO[categoria])
        else:
            categoria = None
            comercio = None

        rows.append({
            "transaccion_id": f"TX{i:010d}",
            "cuenta_id": cuenta_id,
            "timestamp_tx": fecha_min + timedelta(seconds=random.randint(0, rango_segundos)),
            "monto": monto,
            "tipo_tx": tipo_tx,
            "comercio": comercio,
            "categoria_comercio": categoria,
            "pais_tx": pais_tx,
            "ciudad_tx": ciudad_tx,
            "canal": random.choices(CANALES, weights=CANALES_PESOS, k=1)[0],
        })

    df = pl.DataFrame(rows)
    n_filas = df.height

    # Inyección de suciedad: 2% de nulos en monto
    indices_nulos = random.sample(range(n_filas), k=int(0.02 * n_filas))
    df = df.with_columns(
        pl.when(pl.int_range(n_filas).is_in(indices_nulos))
        .then(None)
        .otherwise(pl.col("monto"))
        .alias("monto")
    )

    # 1% de duplicados (mismo transaccion_id repetido)
    duplicados = df.sample(n=int(0.01 * n_filas), seed=settings.random_seed)
    return pl.concat([df, duplicados])


# ============ Main ============
def main() -> None:
    """Orquesta la generación y escribe a data/raw/."""
    print(f"\n{'=' * 60}")
    print(f"  Generador de datos fintech")
    print(f"  Seed: {settings.random_seed}")
    print(f"{'=' * 60}\n")

    settings.data_raw.mkdir(parents=True, exist_ok=True)

    clientes = generar_clientes(settings.num_clientes)
    cuentas = generar_cuentas(clientes)
    transacciones = generar_transacciones(
        cuentas, clientes, settings.num_transacciones
    )

    # Escribir CSVs
    clientes.write_csv(settings.data_raw / "clientes.csv")
    cuentas.write_csv(settings.data_raw / "cuentas.csv")
    transacciones.write_csv(settings.data_raw / "transacciones.csv")

    print(f"\n  Resumen:")
    print(f"    Clientes:      {clientes.height:,} filas")
    print(f"    Cuentas:       {cuentas.height:,} filas")
    print(f"    Transacciones: {transacciones.height:,} filas (incluye 1% duplicados)")
    print(f"\n  Archivos en: {settings.data_raw}")
    print(f"{'=' * 60}\n")


if __name__ == "__main__":
    main()
```

### Conceptos importantes en este código

- **`from __future__ import annotations`**: permite usar type hints modernos en cualquier versión de Python sin problemas.
- **`Faker.seed(42)` + `random.seed(42)`**: **reproducibilidad**. Es uno de los pilares de la ingeniería de datos. Cuando un colega ejecute el script, debe obtener **exactamente los mismos datos**. Sin seed, cada ejecución sería diferente y los tests serían frágiles.
- **Suciedad intencional**: el 2% de nulos y 1% de duplicados son **a propósito**. La realidad de los datos no es perfecta, y Spark tiene que aprender a manejarlo. Si se generan datos "perfectos", el pipeline será inútil en producción.
- **Pesos en `random.choices()`**: refleja **distribuciones reales**. El 55% de clientes en Guatemala, el 5% de transacciones fuera del país, etc. Esto hace los datos interesantes para análisis.
- **El mapa `cuenta_a_pais`**: se construye previamente para que las transacciones tengan **lógica de fraude detectable**. Sin este enlace, las transacciones fuera del país serían aleatorias y no significarían nada para análisis.

---

## Paso 1.7: Ejecutar el generador

```bash
uv run python data/generators/generate_data.py
```

Salida esperada:

```text
============================================================
  Generador de datos fintech
  Seed: 42
============================================================

  Generando 1,000 clientes...
  Generando cuentas para 1,000 clientes...
  Generando 50,000 transacciones...

  Resumen:
    Clientes:      1,000 filas
    Cuentas:       ~2,000 filas
    Transacciones: 50,500 filas (incluye 1% duplicados)

  Archivos en: /.../fintech-analytics-platform/data/raw
============================================================
```

---

## Paso 1.8: Validar los datos generados

> **Nunca confíes en que el código hizo lo correcto sin verificar.**

Ejecutar en `uv run ipython`:

```python
import polars as pl

clientes = pl.read_csv("data/raw/clientes.csv")
cuentas = pl.read_csv("data/raw/cuentas.csv")
transacciones = pl.read_csv("data/raw/transacciones.csv")

# Forma
print("Formas:")
print(f"  clientes:      {clientes.shape}")
print(f"  cuentas:       {cuentas.shape}")
print(f"  transacciones: {transacciones.shape}")

# Suciedad intencional
print(f"\nNulos en monto: {transacciones['monto'].null_count()} (esperado ~1000)")
dup = transacciones.height - transacciones['transaccion_id'].n_unique()
print(f"Duplicados de transaccion_id: {dup} (esperado ~500)")

# Distribuciones
print("\nSegmentos de clientes:")
print(clientes.group_by("segmento").len().sort("len", descending=True))

print("\nTipos de transacción:")
print(transacciones.group_by("tipo_tx").len().sort("len", descending=True))

# Transacciones fuera del país del cliente
fuera_pais = (
    transacciones
    .join(cuentas.select("cuenta_id", "cliente_id"), on="cuenta_id")
    .join(clientes.select("cliente_id", "pais"), on="cliente_id")
    .filter(pl.col("pais_tx") != pl.col("pais"))
)
print(
    f"\nTransacciones fuera del país del cliente: "
    f"{fuera_pais.height} ({fuera_pais.height / transacciones.height * 100:.1f}%)"
)
```

### Resultados esperados

- **Forma transacciones**: `(50500, 10)`.
- **Nulos en monto**: ~1000 (el 2% de 50.000).
- **Duplicados**: ~500 (el 1%).
- **Segmentos**: PERSONAL ~75%, PREMIUM ~15%, BUSINESS ~10%.
- **Transacciones fuera del país**: ~5%.

> Si los números no cuadran, hay un bug. Esto es **validación básica de calidad de datos**, y se formaliza con **Great Expectations** en fases posteriores.

---

## Paso 1.9: Commit inicial

```bash
git init
git add .
git status   # Verificar que .env NO aparece, pero .env.example sí
git commit -m "feat: estructura inicial del proyecto y generador de datos"
```

> Si **`.env`** aparece en `git status`, revisar `.gitignore`. **No hacer commit** si está apareciendo.

---

## Lo que aprendiste en esta fase

- **Estructura profesional** de proyecto Python (paquetes, `src/` layout).
- **Gestión moderna de dependencias** con `uv` y grupos.
- **Configuración centralizada** con Pydantic Settings y variables de entorno.
- **Búsqueda robusta de la raíz** del proyecto vía marker file (`pyproject.toml`).
- **Reproducibilidad** con seeds.
- **Generación de datos sintéticos realistas** con Faker y Polars.
- **Inyección intencional de suciedad** para simular datos reales.
- **Validación básica de outputs**.

---

## Troubleshooting: errores comunes encontrados

Esta sección documenta los **bugs reales** que aparecieron durante el desarrollo de esta fase. Son trampas clásicas: vale la pena conocerlas para no caer en ellas (o reconocerlas rápido cuando ocurran).

### Error 1: Python 3.13 detectado, incompatible con el proyecto

**Síntoma:**

```text
error: The Python request from .python-version resolved to Python 3.13.11,
which is incompatible with the project's Python requirement: >=3.11, <3.13
```

**Causa:** `uv init` crea automáticamente un archivo `.python-version` con la versión del Python del sistema. Si el sistema tiene 3.13 pero `pyproject.toml` requiere `<3.13` (porque **Spark 4** no soporta 3.13), choca.

**Solución:**

```bash
uv python pin 3.12
uv sync --all-groups
```

**Lección:** alinear `.python-version` con el `requires-python` de `pyproject.toml`. Las dependencias del proyecto mandan, no la versión del sistema.

---

### Error 2: `Invalid configuration for faker locale es_GT`

**Síntoma:**

```text
AttributeError: Invalid configuration for faker locale es_GT
```

**Causa:** Faker solo trae los locales que su comunidad ha construido. Los disponibles para español son: `es`, `es_ES`, `es_MX`, `es_AR`, `es_CL`, `es_CO`, `es_CA`. **No existe `es_GT`** (ni `es_SV`, ni `es_HN`).

**Solución:**

```python
# Faker no tiene locale es_GT; es_MX es el más cercano para nombres latinos.
# Los datos de dominio (países, ciudades, comercios) son constantes propias.
fake = Faker("es_MX")
```

**Lección:** el locale de Faker solo afecta **nombres, emails, teléfonos y direcciones genéricas**. No afecta la lógica de dominio (países, ciudades, comercios), porque esos están definidos como constantes propias.

---

### Error 3: `TypeError: unhashable type: 'list'` al indexar `CIUDADES_POR_PAIS`

**Síntoma:**

```text
DEBUG pais_tx = ['El Salvador'] (tipo: list)
TypeError: unhashable type: 'list'
```

**Causa:** la trampa más común de Python con `random`:

- **`random.choice(lista)`** → devuelve **un elemento** (string).
- **`random.choices(lista, k=1)`** → devuelve **una lista de un elemento** (`['string']`).

La `s` final cambia todo. Cuando se usa `random.choices(..., k=1)` esperando un string, se obtiene `['Guatemala']` en lugar de `'Guatemala'`, y al usarlo como clave de diccionario explota.

**Solución:** para un solo valor sin pesos, **usar siempre `random.choice` (singular)**:

```python
# ❌ Mal: devuelve lista
pais_tx = random.choices(paises_extranjeros)

# ✅ Bien: devuelve string
pais_tx = random.choice(paises_extranjeros)

# ✅ Bien también: si necesitas pesos, extrae el elemento [0]
pais_tx = random.choices(paises_extranjeros, weights=..., k=1)[0]
```

**Lección:** auditar el archivo completo buscando cada `random.choices` que **no** termine en `[0]` y que **no** esté generando `k=n` elementos a propósito. Esos son los sospechosos.

---

### Error 4: solo se generó 1 transacción en lugar de 50,500

**Síntoma:** el resumen mostró `Transacciones: 1 filas` después de "generar" 50.000.

**Causa:** **error de indentación silencioso**. El cuerpo del bucle estaba a 4 espacios (nivel de la función) en lugar de 8 (nivel del bucle):

```python
for i in range(1, n + 1):
    cuenta_id = random.choice(cuenta_ids)
    # ... cálculos ...
    ciudad_tx = random.choice(CIUDADES_POR_PAIS[pais_tx])

# ❌ Esto está FUERA del for (4 espacios)
if tipo_tx == "DEPOSITO":
    monto = ...
rows.append({...})
```

Python ejecutó el `for` 50.000 veces, pero **solo el `append` final corrió una vez** (con los valores de la última iteración). Resultado: `rows` con un único elemento.

**Solución:** indentar todo el cuerpo del bucle correctamente (8 espacios, dentro del `for`).

**Lección:** este tipo de bug **no genera error**, simplemente hace algo distinto a lo esperado. Dos defensas:

1. Correr el linter: `uv run ruff check data/generators/generate_data.py`. Ruff a veces detecta variables sospechosamente no usadas que delatan estos errores.
2. Configurar el editor para mostrar la indentación y auto-formatear con Ruff al guardar.

---

### Error 5: los CSVs aparecen en una carpeta equivocada

**Síntoma:** los archivos se escribieron en `/home/usuario/Documents/personal/data/raw` en lugar de `/.../fintech-analytics-platform/data/raw`. Faltaban dos niveles del path.

**Causa:** el cálculo original usaba `parents[N]` para encontrar la raíz:

```python
# ❌ Frágil: depende de la ubicación exacta del archivo
PROJECT_ROOT = Path(__file__).resolve().parents[2]
```

Esto es **frágil**: si el archivo se mueve, si se importa desde otro directorio, o si el cálculo de niveles está mal, todo el sistema de paths se rompe silenciosamente.

**Solución: buscar un marker file** (`pyproject.toml`):

```python
def _find_project_root() -> Path:
    """Sube directorios hasta encontrar pyproject.toml (la raíz del proyecto)."""
    current = Path(__file__).resolve()
    for parent in current.parents:
        if (parent / "pyproject.toml").exists():
            return parent
    raise RuntimeError("No se encontró pyproject.toml; ¿estás dentro del proyecto?")

PROJECT_ROOT = _find_project_root()
```

**Lección:** no contar niveles a mano. **Usar un marker file** (`pyproject.toml`, `.git/`, `setup.py`) es el patrón profesional, el mismo que usan **pytest** y **dbt** para localizar la raíz del proyecto.

---

## Antes de la Fase 2

Asegurarse de:

- [x] El script corre sin errores y genera los 3 CSVs.
- [x] Los números de validación son razonables (50.500 transacciones, ~1000 nulos, ~500 duplicados).
- [x] Los archivos están en `data/raw/` **dentro del proyecto**.
- [x] **Docker Desktop** instalado y corriendo (lo necesitamos en Fase 2).
- [x] Commit inicial hecho, con `.env` correctamente ignorado.