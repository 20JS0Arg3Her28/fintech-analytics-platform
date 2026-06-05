# Fase 2: Docker Compose y PostgreSQL

## Objetivo de esta fase

Levantar **PostgreSQL** dentro de un contenedor **Docker**, crear los **schemas** de la arquitectura **Medallion**, y verificar que se puede conectar desde **Python**. Solo Postgres por ahora — **un servicio a la vez**. Spark y Airflow vienen después.

> **¿Por qué empezar solo con Postgres?**
> Porque si se levantan los 5 servicios de golpe y algo falla, no se sabe cuál es. Construir de forma incremental es el método profesional.

---

## Paso 2.1: Verificar Docker

```bash
docker --version
docker compose version
docker run hello-world
```

Si `hello-world` corre y se cierra solo, **Docker está listo**. Si da un error de permisos (`permission denied`), se necesita agregar el usuario al grupo `docker`:

```bash
sudo usermod -aG docker $USER
# luego cerrar sesión y volver a entrar, o reiniciar
```

---

## Paso 2.2: Entender qué se va a crear

Antes del código, el concepto. Se van a crear **dos archivos**:

- **`docker/postgres/init.sql`** — un script SQL que Postgres ejecuta automáticamente la primera vez que el contenedor arranca. Aquí se crean los schemas.
- **`docker-compose.yml`** — el archivo que define el servicio de Postgres: qué imagen usar, qué puerto exponer, qué credenciales y dónde guardar los datos.

### El concepto clave: persistencia con volúmenes

Un contenedor es **efímero**: si se borra, todo lo que tenía adentro desaparece. Eso es un problema para una base de datos — no se quieren perder los datos cada vez que se reinicia.

La solución es un **volumen**: una carpeta gestionada por Docker que vive **fuera del contenedor**. La base de datos escribe ahí, y aunque se destruya y recree el contenedor, los datos siguen intactos. Esto se verá reflejado en el `docker-compose.yml`.

---

## Paso 2.3: Crear el script de inicialización

Crear el archivo `docker/postgres/init.sql`:

```sql
-- Script de inicialización de la base de datos fintech.
-- Postgres ejecuta automáticamente los .sql de /docker-entrypoint-initdb.d/
-- la PRIMERA vez que el contenedor arranca (cuando el volumen está vacío).

-- Schemas de la arquitectura Medallion
CREATE SCHEMA IF NOT EXISTS bronze;    -- datos crudos (referencia)
CREATE SCHEMA IF NOT EXISTS staging;   -- capa Silver: datos limpios de Spark
CREATE SCHEMA IF NOT EXISTS analytics; -- capa Gold: modelos de dbt

-- Mensaje de confirmación en los logs del contenedor
DO $$
BEGIN
    RAISE NOTICE 'Schemas bronze, staging y analytics creados correctamente.';
END $$;
```

### ¿Por qué tres schemas?

Un **schema** en Postgres es como una carpeta dentro de la base de datos que agrupa tablas. Separar por capa Medallion da orden:

- `staging.transacciones_silver` → limpio, lo escribe **Spark**.
- `analytics.fraude_por_cliente` → analítico, lo crea **dbt**.

Cualquiera que abra la base entiende la arquitectura de inmediato.

---

## Paso 2.4: Crear el `docker-compose.yml`

Crear `docker-compose.yml` en la **raíz del proyecto** (no dentro de `docker/`):

```yaml
# Docker Compose: define los servicios de la plataforma fintech.
# Fase 2: solo PostgreSQL. Iremos agregando servicios fase por fase.

services:
  postgres:
    image: postgres:16
    container_name: fintech_postgres
    restart: unless-stopped
    environment:
      POSTGRES_USER: fintech_user
      POSTGRES_PASSWORD: fintech_pass
      POSTGRES_DB: fintech
    ports:
      - "5432:5432"
    volumes:
      # Persistencia: los datos sobreviven aunque se borre el contenedor
      - postgres_data:/var/lib/postgresql/data
      # Script de init: se ejecuta solo la primera vez
      - ./docker/postgres/init.sql:/docker-entrypoint-initdb.d/init.sql
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U fintech_user -d fintech"]
      interval: 10s
      timeout: 5s
      retries: 5

volumes:
  postgres_data:
```

### Explicación línea por línea

- **`image: postgres:16`** — descarga la imagen oficial de Postgres 16 desde **Docker Hub**. No se instala nada manualmente.
- **`container_name`** — nombre fijo para referirse al contenedor en los comandos.
- **`restart: unless-stopped`** — si el contenedor se cae, Docker lo reinicia solo. **No** se reinicia si se paró a propósito.
- **`environment`** — variables que la imagen de Postgres lee para crear el **usuario**, **contraseña** y **base de datos** iniciales.
- **`ports: "5432:5432"`** — mapea el puerto. El formato es `HOST:CONTENEDOR`. El `5432` de la izquierda es el de la laptop; el de la derecha es el del interior del contenedor. Así se puede conectar desde la máquina a `localhost:5432`.
- **`volumes`** — dos cosas distintas:
  - La primera línea es el **volumen de persistencia** (datos).
  - La segunda **monta** `init.sql` dentro de la carpeta especial que Postgres revisa al arrancar.
- **`healthcheck`** — Docker comprueba periódicamente si Postgres está **realmente listo** (no solo "arrancando"). Esto será crítico cuando se agregue Airflow, porque Airflow debe esperar a que Postgres esté sano antes de arrancar.
- **`volumes:` al final (sin indentar)** — declara el volumen nombrado. Las dos secciones `volumes` son distintas: la de adentro del servicio **usa** el volumen, la de abajo lo **define**.

---

## Paso 2.5: Levantar Postgres

```bash
docker compose up -d
```

El flag **`-d`** significa **"detached"**: corre en segundo plano y devuelve la terminal. La primera vez descarga la imagen de Postgres (~150 MB), por lo que tarda un poco.

Verificar que está corriendo:

```bash
docker compose ps
```

Debe aparecer `fintech_postgres` con estado **`Up`** y **`(healthy)`** después de unos segundos.

Revisar los logs para confirmar que `init.sql` corrió:

```bash
docker compose logs postgres | grep -i "schemas"
```

Debe aparecer el mensaje: `Schemas bronze, staging y analytics creados correctamente.`

---

## Paso 2.6: Conectarse y verificar

Entrar al Postgres del contenedor:

```bash
docker compose exec postgres psql -U fintech_user -d fintech
```

Ya dentro de **`psql`**, listar los schemas:

```sql
\dn
```

Deben aparecer `bronze`, `staging`, `analytics` (más `public`, que viene por defecto). Salir con `\q`.

---

## Paso 2.7: Verificar conexión desde Python

Esto confirma que el código Python podrá hablar con la base. Como `config.py` ya tiene `pg_sqlalchemy_url`, probar con **Polars**:

```bash
uv run python -c "
import polars as pl
from src.utils.config import settings
df = pl.read_database_uri(
    'SELECT schema_name FROM information_schema.schemata',
    settings.pg_sqlalchemy_url
)
print(df)
"
```

Si esto imprime una tabla con los nombres de los schemas, Python se conecta correctamente a Postgres.

### Posible error de driver

Puede que pida un driver. Si da error de conexión, instalar:

```bash
uv add connectorx
```

> **`connectorx`** es el motor que Polars usa para leer de bases de datos; es **muy rápido**, escrito en **Rust**.