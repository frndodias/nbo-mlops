# Databricks notebook source
# MAGIC %md
# MAGIC # 07a · Carregar base de scores no Lakebase (pro App)
# MAGIC O App lê tudo do Lakebase (baixa latência). Carrega `base_clientes` (12k) e `base_scored` (144k).

# COMMAND ----------

import ssl, requests
import pg8000.dbapi

dbutils.widgets.text("catalog", "fernando_custodio")
dbutils.widgets.text("schema", "nbo")
CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
# Lakebase — ajuste para o SEU projeto/instância (veja README)
LB_ENDPOINT = "projects/nbo-app/branches/production/endpoints/primary"
LB_HOST = "SEU-LAKEBASE-HOST.database.cloud.databricks.com"  # <-- TROQUE
ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
WS_HOST = ctx.apiUrl().get(); WS_TOKEN = ctx.apiToken().get(); USER = ctx.userName().get()
tok = requests.post(f"{WS_HOST}/api/2.0/postgres/credentials",
                    headers={"Authorization": f"Bearer {WS_TOKEN}"}, json={"endpoint": LB_ENDPOINT}).json()["token"]
conn = pg8000.dbapi.connect(host=LB_HOST, port=5432, database="databricks_postgres",
                            user=USER, password=tok, ssl_context=ssl.create_default_context())
cur = conn.cursor()

# COMMAND ----------

cli = spark.table(f"{CATALOG}.{SCHEMA}.base_clientes_score").toPandas()
scored = spark.table(f"{CATALOG}.{SCHEMA}.base_scored").select(
    "customer_id", "tipo_oferta", "nivel_agressividade", "p_aceitar").toPandas()
print("cli:", len(cli), "scored:", len(scored))

# COMMAND ----------

cur.execute("""
DROP TABLE IF EXISTS nbo.base_clientes;
CREATE TABLE nbo.base_clientes (
  customer_id bigint PRIMARY KEY, segmento text, score_max double precision,
  freq_60d int, recencia_hoje int, freq_total int, vibes_acum bigint);
DROP TABLE IF EXISTS nbo.base_scored;
CREATE TABLE nbo.base_scored (
  customer_id bigint, tipo_oferta text, nivel_agressividade int, p_aceitar double precision);
""")
conn.commit()

def bulk_insert(tabela, ncols, rows, batch=2000):
    ph = "(" + ",".join(["%s"] * ncols) + ")"
    for i in range(0, len(rows), batch):
        chunk = rows[i:i+batch]
        sql = f"INSERT INTO {tabela} VALUES " + ",".join([ph] * len(chunk))
        cur.execute(sql, [v for row in chunk for v in row])
    conn.commit()

bulk_insert("nbo.base_clientes", 7,
            [tuple(x) for x in cli[["customer_id","segmento","score_max","freq_60d",
                                    "recencia_hoje","freq_total","vibes_acum"]].itertuples(index=False, name=None)])
bulk_insert("nbo.base_scored", 4,
            [tuple(x) for x in scored.itertuples(index=False, name=None)])

cur.execute("CREATE INDEX IF NOT EXISTS ix_scored_cust ON nbo.base_scored(customer_id)")
conn.commit()

# COMMAND ----------

cur.execute("SELECT (SELECT count(*) FROM nbo.base_clientes), (SELECT count(*) FROM nbo.base_scored)")
res = cur.fetchone()
cur.close(); conn.close()
import json
dbutils.notebook.exit(json.dumps({"base_clientes": res[0], "base_scored": res[1]}))
