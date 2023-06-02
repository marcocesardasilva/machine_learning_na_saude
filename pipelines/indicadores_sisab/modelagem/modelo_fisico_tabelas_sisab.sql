------------------------------------------------
-- Dropar tabelas
------------------------------------------------

-- DROP TABLE public.fato_desempenho CASCADE;
-- DROP TABLE public.dim_indicador CASCADE;
-- DROP TABLE public.dim_equipe CASCADE;
-- DROP TABLE public.dim_periodo CASCADE;
-- DROP TABLE public.dim_localidade CASCADE;

------------------------------------------------
-- Criar tabelas
------------------------------------------------

CREATE TABLE public.fato_desempenho (
    sk_indicador integer NOT NULL,
    sk_equipe integer NOT NULL,
    sk_periodo integer NOT NULL,
    sk_localidade integer NOT NULL,
    numerador integer NOT NULL,
    denominador_utilizado integer NOT NULL,
    percentual_quadrimestre integer NOT NULL,
    denominador_identificado integer NOT NULL,
    denominador_estimado integer NOT NULL,
    cadastro integer NOT NULL,
    base_externa real NOT NULL,
    percentual integer NOT NULL,
    populacao integer NOT NULL,
    PRIMARY KEY (sk_indicador, sk_equipe, sk_periodo, quadrimestre, sk_localidade)
);


CREATE TABLE public.dim_indicador (
    sk_indicador integer NOT NULL,
    nm_indicador varchar(128) NOT NULL,
    de_indicador varchar(256) NOT NULL,
    PRIMARY KEY (sk_indicador)
);


CREATE TABLE public.dim_equipe (
    sk_equipe integer NOT NULL,
    nm_equipe varchar(128) NOT NULL,
    de_equipe varchar(256) NOT NULL,
    PRIMARY KEY (sk_equipe)
);


CREATE TABLE public.dim_periodo (
    sk_periodo integer NOT NULL,
    ano integer NOT NULL,
    quadrimestre integer NOT NULL,
    PRIMARY KEY (sk_periodo)
);


CREATE TABLE public.dim_localidade (
    sk_localidade integer NOT NULL,
    uf char(2) NOT NULL,
    municipio varchar(256) NOT NULL,
    PRIMARY KEY (sk_localidade)
);

------------------------------------------------
-- Relacionar tabelas
------------------------------------------------

ALTER TABLE public.fato_desempenho ADD CONSTRAINT FK_fato_indicadores__sk_indicador FOREIGN KEY (sk_indicador) REFERENCES public.dim_indicador(sk_indicador);
ALTER TABLE public.fato_desempenho ADD CONSTRAINT FK_fato_indicadores__sk_equipes FOREIGN KEY (sk_equipe) REFERENCES public.dim_equipe(sk_equipe);
ALTER TABLE public.fato_desempenho ADD CONSTRAINT FK_fato_indicadores__sk_periodo FOREIGN KEY (sk_periodo) REFERENCES public.dim_periodo(sk_periodo);
ALTER TABLE public.fato_desempenho ADD CONSTRAINT FK_fato_indicadores__sk_localidade FOREIGN KEY (sk_localidade) REFERENCES public.dim_localidade(sk_localidade);