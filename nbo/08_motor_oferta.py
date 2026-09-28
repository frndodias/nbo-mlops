# Databricks notebook source
# MAGIC %md
# MAGIC # 06 · Motor de Oferta (BATCH) — lê regras do LAKEBASE
# MAGIC
# MAGIC O pipeline do dia-a-dia. Lê a **lista de decisão** que o **Databricks App** grava no Lakebase,
# MAGIC avalia sobre a base de scores (`base_scored`/`base_clientes_score`) e aplica **budget por balde**.
# MAGIC
# MAGIC Regras (Lakebase) + score do modelo + features → `gold_ofertas_final`.

# COMMAND ----------

import json, requests, ssl
import pandas as pd
import pg8000.dbapi  # driver Postgres PURO PYTHON (psycopg nativo dá SIGABRT no serverless)

dbutils.widgets.text("catalog", "fernando_custodio")
dbutils.widgets.text("schema", "nbo")
CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
# Lakebase — ajuste para o SEU projeto/instância (veja README)
LB_ENDPOINT = "projects/nbo-app/branches/production/endpoints/primary"
LB_HOST = "SEU-LAKEBASE-HOST.database.cloud.databricks.com"  # <-- TROQUE

ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
WS_HOST = ctx.apiUrl().get()
WS_TOKEN = ctx.apiToken().get()
USER = ctx.userName().get()

# COMMAND ----------

# MAGIC %md ## 1. Ler as regras do Lakebase

# COMMAND ----------

cred = requests.post(f"{WS_HOST}/api/2.0/postgres/credentials",
                     headers={"Authorization": f"Bearer {WS_TOKEN}"},
                     json={"endpoint": LB_ENDPOINT}).json()
PG_TOKEN = cred["token"]

def _read(cur, sql, cols):
    cur.execute(sql)
    return pd.DataFrame(cur.fetchall(), columns=cols)

conn = pg8000.dbapi.connect(host=LB_HOST, port=5432, database="databricks_postgres",
                            user=USER, password=PG_TOKEN, ssl_context=ssl.create_default_context())
cur = conn.cursor()
regras = _read(cur, "SELECT id, prioridade, nome, ativo, condicoes, acao_tipo, acao_nivel "
                    "FROM nbo.regras_elegibilidade WHERE ativo ORDER BY prioridade",
               ["id", "prioridade", "nome", "ativo", "condicoes", "acao_tipo", "acao_nivel"])
orcamento = _read(cur, "SELECT segmento, prioridade, budget_vibes FROM nbo.orcamento_segmento",
                  ["segmento", "prioridade", "budget_vibes"])
catalogo = _read(cur, "SELECT tipo_oferta, ativo, custo_base_vibes FROM nbo.ofertas_catalogo",
                 ["tipo_oferta", "ativo", "custo_base_vibes"])
cur.close(); conn.close()
print("regras ativas:", len(regras))
budget = dict(zip(orcamento.segmento, orcamento.budget_vibes))
custo_base = dict(zip(catalogo.tipo_oferta, catalogo.custo_base_vibes))
ativos = set(catalogo[catalogo.ativo == True].tipo_oferta)

# COMMAND ----------

# MAGIC %md ## 2. Base de scores (UC) → pandas

# COMMAND ----------

cli = spark.table(f"{CATALOG}.{SCHEMA}.base_clientes_score").toPandas()
scored = spark.table(f"{CATALOG}.{SCHEMA}.base_scored").toPandas()

# índices rápidos
p_map = {(r.customer_id, r.tipo_oferta, r.nivel_agressividade): r.p_aceitar for r in scored.itertuples()}
# melhor tipo (ativo) por (cliente, nível)
sc_act = scored[scored.tipo_oferta.isin(ativos)]
best = (sc_act.sort_values("p_aceitar", ascending=False)
        .groupby(["customer_id", "nivel_agressividade"]).first().reset_index())
best_map = {(r.customer_id, r.nivel_agressividade): (r.tipo_oferta, r.p_aceitar) for r in best.itertuples()}

# COMMAND ----------

# MAGIC %md ## 3. Avaliar a lista de decisão (primeira regra que casa vence)

# COMMAND ----------

def val_campo(row, campo):
    return row["score_max"] if campo == "score" else row[campo]

def testa(row, cond):
    v = val_campo(row, cond["campo"]); alvo = cond["valor"]; op = cond["op"]
    if op == "=":  return v == alvo
    if op == "!=": return v != alvo
    if op == ">=": return v >= alvo
    if op == "<=": return v <= alvo
    if op == ">":  return v > alvo
    if op == "<":  return v < alvo
    return False

def aplica_regras(row):
    for r in regras.itertuples():
        conds = r.condicoes if isinstance(r.condicoes, list) else json.loads(r.condicoes)
        if all(testa(row, c) for c in conds):
            return r.nome, r.acao_tipo, int(r.acao_nivel)
    return "sem_regra", "sem_oferta", 1

linhas = []
for row in cli.itertuples():
    d = row._asdict()
    nome, acao, nivel = aplica_regras(d)
    if acao == "sem_oferta":
        tipo, p = "sem_oferta", 0.0
    elif acao == "melhor_ml":
        tipo, p = best_map.get((row.customer_id, nivel), ("sem_oferta", 0.0))
    else:
        tipo, p = acao, p_map.get((row.customer_id, acao, nivel), 0.0)
    custo = int(custo_base.get(tipo, 0) * nivel) if tipo != "sem_oferta" else 0
    linhas.append((row.customer_id, row.segmento, nome, tipo, nivel, float(p), custo))

dec = pd.DataFrame(linhas, columns=["customer_id", "segmento", "regra_aplicada",
                                    "tipo_oferta", "nivel_agressividade", "p_aceitar", "custo_vibes"])

# COMMAND ----------

# MAGIC %md ## 4. Budget por balde (segmento) → Gold Final

# COMMAND ----------

dec = dec.sort_values(["segmento", "p_aceitar"], ascending=[True, False])
dec["custo_acum"] = dec.groupby("segmento")["custo_vibes"].cumsum()
dec["budget_seg"] = dec["segmento"].map(budget).fillna(0)
# quem tem oferta e cabe no balde é atendido; sem_oferta não consome budget
dec["elegivel_budget"] = (dec["tipo_oferta"] != "sem_oferta") & (dec["custo_acum"] <= dec["budget_seg"])
dec["oferta_final"] = dec.apply(lambda r: r.tipo_oferta if r.elegivel_budget else "sem_oferta", axis=1)

gold = spark.createDataFrame(dec[["customer_id", "segmento", "regra_aplicada", "oferta_final",
                                  "tipo_oferta", "nivel_agressividade", "p_aceitar",
                                  "custo_vibes", "elegivel_budget"]])
(gold.write.mode("overwrite").option("overwriteSchema", "true")
 .saveAsTable(f"{CATALOG}.{SCHEMA}.gold_ofertas_final"))

# COMMAND ----------

display(spark.table(f"{CATALOG}.{SCHEMA}.gold_ofertas_final")
        .groupBy("segmento", "oferta_final").count().orderBy("segmento", "oferta_final"))

# COMMAND ----------

atend = int(dec.elegivel_budget.sum())
maria = dec[dec.customer_id == 1].iloc[0]
dbutils.notebook.exit(json.dumps({
    "total": int(len(dec)), "atendidos": atend,
    "maria_regra": str(maria.regra_aplicada), "maria_oferta": str(maria.oferta_final),
    "maria_nivel": int(maria.nivel_agressividade), "maria_p": round(float(maria.p_aceitar), 3),
}))
