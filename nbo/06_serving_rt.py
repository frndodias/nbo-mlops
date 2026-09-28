# Databricks notebook source
# MAGIC %md
# MAGIC # 05 · Serving RT — Vibe NBO (Pilar 4)
# MAGIC
# MAGIC O **balcão em tempo real**: o app manda `customer_id` + a oferta candidata, o endpoint
# MAGIC busca as features da cliente no **online store (Lakebase)** e devolve `P(aceitar)` em ms.
# MAGIC
# MAGIC Só o **modelo** vai pro RT (as regras/budget ficam no batch — notebook 06).

# COMMAND ----------

import mlflow, time
from mlflow.deployments import get_deploy_client

dbutils.widgets.text("catalog", "fernando_custodio")
dbutils.widgets.text("schema", "nbo")
CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
ENDPOINT = "nbo-score-oferta"
MODEL_NAME = f"{CATALOG}.{SCHEMA}.nbo_score_oferta"
client = get_deploy_client("databricks")

# COMMAND ----------

# MAGIC %md ## Criar / atualizar o endpoint a partir do champion
# MAGIC (idempotente — se já existir, faz update de config)

# COMMAND ----------

from mlflow.tracking import MlflowClient
mlflow.set_registry_uri("databricks-uc")
ver = MlflowClient(registry_uri="databricks-uc").get_model_version_by_alias(MODEL_NAME, "champion").version

config = {
    "served_entities": [{
        "entity_name": MODEL_NAME,
        "entity_version": ver,
        "workload_size": "Small",
        "scale_to_zero_enabled": True,
    }],
    "traffic_config": {"routes": [
        {"served_model_name": f"nbo_score_oferta-{ver}", "traffic_percentage": 100}
    ]},
}

try:
    client.get_endpoint(ENDPOINT)
    client.update_endpoint(endpoint=ENDPOINT, config=config)
    print("endpoint atualizado para champion v", ver)
except Exception:
    client.create_endpoint(name=ENDPOINT, config=config)
    print("endpoint criado com champion v", ver)

# COMMAND ----------

# MAGIC %md ## Testar em tempo real — a Maria (customer_id = 1)
# MAGIC Mandamos só o ID + a oferta candidata; as features vêm do Lakebase automaticamente.

# COMMAND ----------

candidatas = [
    {"customer_id": 1, "tipo_oferta": "cashback",          "nivel_agressividade": 3},
    {"customer_id": 1, "tipo_oferta": "desconto_parceiro", "nivel_agressividade": 1},
    {"customer_id": 1, "tipo_oferta": "bonus_vibes",       "nivel_agressividade": 2},
    {"customer_id": 1, "tipo_oferta": "missao",            "nivel_agressividade": 1},
]

t0 = time.time()
resp = client.predict(endpoint=ENDPOINT, inputs={"dataframe_records": candidatas})
dt = (time.time() - t0) * 1000

for c, p in zip(candidatas, resp["predictions"]):
    print(f"  {c['tipo_oferta']:18} nível {c['nivel_agressividade']} → P(aceitar) = {p:.3f}")
print(f"\nlatência: {dt:.0f} ms")
melhor = max(zip(candidatas, resp["predictions"]), key=lambda x: x[1])
print(f"melhor oferta p/ Maria em RT: {melhor[0]['tipo_oferta']} (P={melhor[1]:.3f})")

# COMMAND ----------

import json
dbutils.notebook.exit(json.dumps({
    "endpoint": ENDPOINT, "champion_version": ver,
    "maria_melhor": melhor[0]["tipo_oferta"], "maria_p": round(float(melhor[1]), 3),
}))
