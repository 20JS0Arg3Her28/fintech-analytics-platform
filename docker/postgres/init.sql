-- Script de inicialización de la base de datos fintech.
-- Postgres ejecuta automaticamente los .sql de /docker-entrypoint-initdb.d/.
-- la PRIMERA vez que el contenedor arranca (cuando el volumen de datos está vacío). Si el volumen ya tiene datos, este script NO se ejecutará.
CREATE SCHEMA IF NOT EXISTS raw_data; -- BRONZE, datos crudos (referencia)
CREATE SCHEMA IF NOT EXISTS staging; -- capa SILVER: datos limpios de spark
CREATE SCHEMA IF NOT  EXISTS analytics; -- capa GOLD: modelo dbt

-- Mensaje de confirmación en los logs del contenedor
DO $$
BEGIN
    RAISE NOTICE 'Esquemas raw_data, staging y analytics creados (si no existían) correctamente.';
END;