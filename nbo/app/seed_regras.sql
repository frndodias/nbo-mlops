-- Schema de regras do motor de elegibilidade NBO (Lakebase)
CREATE SCHEMA IF NOT EXISTS nbo;

CREATE TABLE IF NOT EXISTS nbo.ofertas_catalogo (
  tipo_oferta      text PRIMARY KEY,
  ativo            boolean NOT NULL DEFAULT true,
  custo_base_vibes int NOT NULL
);

CREATE TABLE IF NOT EXISTS nbo.orcamento_segmento (
  segmento     text PRIMARY KEY,
  prioridade   int NOT NULL,
  budget_vibes bigint NOT NULL
);

CREATE TABLE IF NOT EXISTS nbo.regras_elegibilidade (
  id          serial PRIMARY KEY,
  prioridade  int NOT NULL,
  nome        text NOT NULL,
  ativo       boolean NOT NULL DEFAULT true,
  condicoes   jsonb NOT NULL DEFAULT '[]',
  acao_tipo   text NOT NULL,          -- melhor_ml | cashback | bonus_vibes | desconto_parceiro | missao | sem_oferta
  acao_nivel  int  NOT NULL DEFAULT 1 -- 1..3
);

-- log de mudanças (auditoria — pro pitch de governança)
CREATE TABLE IF NOT EXISTS nbo.regras_auditoria (
  id         serial PRIMARY KEY,
  ts         timestamptz NOT NULL DEFAULT now(),
  usuario    text,
  acao       text,
  detalhe    jsonb
);

-- ===== SEED (idempotente) =====
INSERT INTO nbo.ofertas_catalogo (tipo_oferta, ativo, custo_base_vibes) VALUES
 ('cashback', true, 100), ('bonus_vibes', true, 80),
 ('desconto_parceiro', true, 40), ('missao', true, 20)
ON CONFLICT (tipo_oferta) DO UPDATE SET ativo = EXCLUDED.ativo, custo_base_vibes = EXCLUDED.custo_base_vibes;

INSERT INTO nbo.orcamento_segmento (segmento, prioridade, budget_vibes) VALUES
 ('em_risco', 1, 300000), ('novo', 2, 200000), ('ativo', 3, 200000), ('campeao', 4, 60000)
ON CONFLICT (segmento) DO UPDATE SET prioridade = EXCLUDED.prioridade, budget_vibes = EXCLUDED.budget_vibes;

TRUNCATE nbo.regras_elegibilidade RESTART IDENTITY;
INSERT INTO nbo.regras_elegibilidade (prioridade, nome, ativo, condicoes, acao_tipo, acao_nivel) VALUES
 (1, 'Em risco quente → reativar agressivo', true,
    '[{"campo":"segmento","op":"=","valor":"em_risco"},{"campo":"score","op":">=","valor":0.5}]', 'melhor_ml', 3),
 (2, 'Em risco → reativar', true,
    '[{"campo":"segmento","op":"=","valor":"em_risco"}]', 'melhor_ml', 2),
 (3, 'Novo → missão de engajamento', true,
    '[{"campo":"segmento","op":"=","valor":"novo"}]', 'missao', 1),
 (4, 'Campeão engajado → bônus leve', true,
    '[{"campo":"segmento","op":"=","valor":"campeao"},{"campo":"score","op":">=","valor":0.6}]', 'bonus_vibes', 1),
 (5, 'Campeão → não gastar (já engajado)', true,
    '[{"campo":"segmento","op":"=","valor":"campeao"}]', 'sem_oferta', 1),
 (6, 'Ativo propenso → melhor oferta ML', true,
    '[{"campo":"segmento","op":"=","valor":"ativo"},{"campo":"score","op":">=","valor":0.4}]', 'melhor_ml', 2),
 (7, 'Padrão → desconto de parceiro', true,
    '[]', 'desconto_parceiro', 1);

SELECT prioridade, nome, acao_tipo, acao_nivel FROM nbo.regras_elegibilidade ORDER BY prioridade;
