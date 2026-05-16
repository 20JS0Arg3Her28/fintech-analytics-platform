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

# =========== Set up ===========
# Faker en español para nombres y direcciones latinos
fake = Faker("es_MX")
Faker.seed(settings.random_seed)
random.seed(settings.random_seed)


# =========== Constantes del dominio ===========
PAISES = ["México", "Guatemala", "El Salvador", "Honduras", "Costa Rica", "Estados Unidos"]
PAISES_PESOS = [0.55, 0.15, 0.1, 0.08, 0.07, 0.05]  # Distribución de clientes por país

CIUDADES_POR_PAIS = {
    "México": ["Ciudad de México", "Guadalajara", "Monterrey"],
    "Guatemala": ["Ciudad de Guatemala", "Quetzaltenango", "Antigua", "Cobán", "Escuintla"],
    "El Salvador": ["San Salvador", "Santa Ana"],
    "Honduras": ["Tegucigalpa", "San Pedro Sula"],
    "Costa Rica": ["San José", "Cartago"],
    "Estados Unidos": ["Los Angeles", "Houston", "Miami"],
}

SEGMENTOS = ["PERSONAL", "PREMIUM", "BUSINESS"]
SEGMENTOS_PESOS = [0.75, 0.15, 0.1]

TIPOS_CUENTA = ["AHORROS", "CORRIENTE", "CREDITO"]
TIPOS_CUENTA_PESOS = [0.6, 0.3, 0.1]

MONEDAS = ["GTQ", "USD", "EUR"]
MONEDAS_PESOS = [0.75, 0.2, 0.05]

ESTADOS_CUENTA = ["ACTIVA", "CERRADA", "CONGELADA"]
ESTADOS_CUENTA_PESOS = [0.9, 0.07, 0.03]

TIPOS_TX = ["DEPOSITO", "RETIRO", "TRANSFERENCIA", "PAGO_TARJETA", "COMPRA_ONLINE"]
TIPOS_TX_PESOS = [0.15, 0.2, 0.25, 0.25, 0.15]

CANALES = ["APP", "WEB", "CAJERO", "SUCURSAL", "POS"]
CANALES_PESOS = [0.4, 0.2, 0.15, 0.05, 0.2]

CATEGORIAS_COMERCIO = {
    "SUPERMERCADO": ["La Torre", "Walmart", "Paiz", "Despensa Familiar"],
    "RESTAURANTE": ["Pollo Campero", "McDonalds", "Sarita", "Domino's"],
    "GASOLINERA": ["Puma", "Texaco", "Shell"],
    "FARMACIA": ["Farmacia Galeno", "Cruz Verde", "Batres"],
    "TECNOLOGIA": ["Amazon", "Apple Store", "MercadoLibre"],
    "TRANSPORTE": ["Uber", "InDriver", "Aerolínea Avianca"],
    "ENTRETENIMIENTO": ["Netflix", "Spotify", "Cinemark"],
}

# =========== Funciones de generación ===========
def generar_clientes(num_clientes: int) -> pl.DataFrame:
    """Genera un DataFrame de clientes sintéticos con datos realistas."""
    print(f"Generando {num_clientes} clientes sintéticos...")

    paises = random.choices(PAISES, weights=PAISES_PESOS, k=num_clientes)
    ciudades = [random.choice(CIUDADES_POR_PAIS[p]) for p in paises]

    # Fechas de registro distribuidas en los últimos 3 años
    fecha_min = datetime(2022, 1, 1)
    fecha_max = datetime(2025, 12, 31)
    rango_dias = (fecha_max - fecha_min).days

    return pl.DataFrame({
        "cliente_id": [f"CLI{i:06d}" for i in range(1, num_clientes + 1)],
        "nombre_completo": [fake.name() for _ in range(num_clientes)],
        "email": [fake.email() for _ in range(num_clientes)],
        "telefono": [fake.phone_number() for _ in range(num_clientes)],
        "pais": paises,
        "ciudad": ciudades,
        "fecha_registro": [
            fecha_min + timedelta(days=random.randint(0, rango_dias))
            for _ in range(num_clientes)
        ],
        "segmento": random.choices(SEGMENTOS, weights=SEGMENTOS_PESOS, k=num_clientes),
        "fecha_nacimiento": [
            fake.date_of_birth(minimum_age=18, maximum_age=80)
            for _ in range(num_clientes)
        ],
    })

def generar_cuentas(clientes: pl.DataFrame) -> pl.DataFrame:
    """Cada cliente tiene entre 1 y N cuentas."""
    clientes_ids = clientes["cliente_id"].to_list()
    print(f"Generando cuentas para {len(clientes_ids)} clientes...")

    rows = []
    cuenta_counter = 1

    for cliente_id in clientes_ids:
        num_cuentas = random.randint(1, settings.num_cuentas_por_cliente)
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
    """Genera transacciones. Se hace inyección de datos 'sucios' a proposito."""
    print(f"Generando {n:,} transacciones...")

    cuentas_activas = cuentas.filter(pl.col("estado") == "ACTIVA")
    cuentas_ids = cuentas_activas["cuenta_id"].to_list()

    # Mapa cuentas -> país del cliente (para detectar fraude después)
    cuenta_a_pais = dict(
        cuentas_activas
        .join(clientes, on="cliente_id")
        .select("cuenta_id", "pais")
        .iter_rows()
    )

    fecha_min = datetime(2022, 1, 1)
    fecha_max = datetime(2025, 12, 31, 23, 59, 59)
    rango_segundos = int((fecha_max - fecha_min).total_seconds())

    rows = []
    for i in range(1, n + 1):
        cuenta_id = random.choice(cuentas_ids)
        tipo_tx = random.choices(TIPOS_TX, weights=TIPOS_TX_PESOS, k=1)[0]
        pais_cliente = cuenta_a_pais[cuenta_id]

        # 95% de transacciones en el pais del cliente, 5% fuera (fraude potencial)
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
        else:
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
    duplicados = df.sample(int(0.01 * n_filas), seed=settings.random_seed)
    return pl.concat([df, duplicados])

def main() -> None:
    """Orquesta la generacion y escribe a data/raw/."""
    print(f"\n{'=' * 60}")
    print("Generador de datos fintech")
    print(f"Seed: {settings.random_seed}")
    print(f"{'=' * 60}\n")

    settings.data_raw.mkdir(parents=True, exist_ok=True)

    clientes = generar_clientes(settings.num_clientes)
    cuentas = generar_cuentas(clientes)
    transacciones = generar_transacciones(
        cuentas,
        clientes,
        settings.num_transacciones
    )

    # Escribir a CSV
    clientes.write_csv(settings.data_raw / "clientes.csv")
    cuentas.write_csv(settings.data_raw / "cuentas.csv")
    transacciones.write_csv(settings.data_raw / "transacciones.csv")

    print("\nResumen:")
    print(f"Clientes:      {clientes.height:,} filas")
    print(f"Cuentas:       {cuentas.height:,} filas")
    print(f"Transacciones: {transacciones.height:,} filas (incluye 1% duplicados)")
    print(f"\nArchivos en: {settings.data_raw}")
    print(f"{'=' * 60}\n")

if __name__ == "__main__":
    main()
