# Databricks notebook source
# MAGIC %md
# MAGIC # 05 · Online Store (Lakebase) — Vibe NBO
# MAGIC
# MAGIC Publica `customer_features` num **online store gerenciado (Lakebase)** em modo **CONTÍNUO**,
# MAGIC pro endpoint fazer o feature lookup em ms no tempo real (features sempre frescas).
# MAGIC
# MAGIC Feature Store (Delta) ──sync contínuo──► Online Store (Lakebase) ──ms──► Serving Endpoint (RT)

# COMMAND ----------

dbutils.widgets.text("capacity", "CU_1", "capacity")
dbutils.widgets.text("publish_mode", "CONTINUOUS", "publish_mode")
CAP = dbutils.widgets.get("capacity")
PUBLISH_MODE = dbutils.widgets.get("publish_mode")

import json, time
from databricks.feature_engineering import FeatureEngineeringClient
fe = FeatureEngineeringClient()

dbutils.widgets.text("catalog", "fernando_custodio")
dbutils.widgets.text("schema", "nbo")
CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
FT = f"{CATALOG}.{SCHEMA}.customer_features"
STORE = "nbo-online"  # DNS-compliant (sem underscore) — exigência do Lakebase

# CDF é pré-requisito pra publish contínuo
spark.sql(f"ALTER TABLE {FT} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")

# COMMAND ----------

# MAGIC %md ## 1. Criar (ou reusar) o online store e aguardar AVAILABLE

# COMMAND ----------

existing = {getattr(s, "name", None) for s in fe.list_online_stores()}
if STORE not in existing:
    print("criando online store...")
    fe.create_online_store(name=STORE, capacity=CAP)
else:
    print("online store já existe")

store = None
for i in range(80):  # ~20 min máx
    s = fe.get_online_store(name=STORE)
    st = str(getattr(s, "state", "")).upper()
    if i % 4 == 0:
        print("state:", st)
    if st.endswith("AVAILABLE"):
        store = s
        break
    if st.endswith("STOPPED") or "FAIL" in st:
        raise RuntimeError(f"online store em estado inesperado: {st}")
    time.sleep(15)
if store is None:
    raise TimeoutError("online store não ficou AVAILABLE a tempo")
print("online store AVAILABLE:", store)

# COMMAND ----------

# MAGIC %md ## 2. Publicar a feature table (contínuo)

# COMMAND ----------

ONLINE_TBL = f"{CATALOG}.{SCHEMA}.customer_features_online"
pub = fe.publish_table(
    online_store=store,
    source_table_name=FT,
    online_table_name=ONLINE_TBL,
    publish_mode=PUBLISH_MODE,
)
print("publish:", pub)

# COMMAND ----------

dbutils.notebook.exit(json.dumps({
    "online_store": STORE,
    "state": str(getattr(store, "state", None)),
    "publish_mode": PUBLISH_MODE,
    "source_table": FT,
}))
