-- Database: Indicadores de desempenho SISAB
-- Author: Marco Cesar da Silva

-- DROP DATABASE indicadores_de_desempenho_sisab;

CREATE DATABASE desempenho_sisab
    WITH OWNER = postgres
        ENCODING = 'UTF8'
        TABLESPACE = pg_default
        LC_COLLATE = 'undefined'
        LC_CTYPE = 'undefined'
        CONNECTION LIMIT = -1;

COMMENT ON DATABASE desempenho_sisab
    IS 'Banco de dados criado para ingestão de dados de indicadores quadrimestrais do SISAB (Sistema de Informação em Saúde para a Atenção Básica)';