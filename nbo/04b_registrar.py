# Databricks notebook source
# MAGIC %md
# MAGIC # 04b · Registrar & Promover (com portão de avaliação)
# MAGIC
# MAGIC Passo **deliberado** (separado do treino): pega um run escolhido, registra como **versão** no UC
# MAGIC e **só promove a `champion` se o AUC for ≥ ao champion atual** (senão fica como `challenger`).
# MAGIC
# MAGIC - `run_id` vazio = pega **o melhor run** (maior AUC) do experimento.
# MAGIC - Usa `create_model_version` (dribla a quota de "criar modelo" do metastore).

# COMMAND ----------

import mlflow, time, json
from mlflow.tracking import MlflowClient

mlflow.set_registry_uri("databricks-uc")
dbutils.widgets.text("catalog", "fernando_custodio")
dbutils.widgets.text("schema", "nbo")
CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
_user = spark.sql("SELECT current_user()").first()[0]
EXP = f"/Users/{_user}/nbo/exp_nbo_score_oferta"
MODEL_NAME = f"{CATALOG}.{SCHEMA}.nbo_score_oferta"

dbutils.widgets.text("run_id", "", "run_id (vazio = melhor do experimento)")
RUN_ID = dbutils.widgets.get("run_id").strip()

c = MlflowClient(registry_uri="databricks-uc")

# COMMAND ----------

# MAGIC %md ## 1. Resolver o run a promover + seu AUC

# COMMAND ----------

if not RUN_ID:
    exp = mlflow.get_experiment_by_name(EXP)
    best = mlflow.search_runs([exp.experiment_id], order_by=["metrics.auc DESC"], max_results=1)
    RUN_ID = best.iloc[0]["run_id"]; novo_auc = float(best.iloc[0]["metrics.auc"])
else:
    novo_auc = float(c.get_run(RUN_ID).data.metrics["auc"])
print(f"candidato: run {RUN_ID}  AUC={novo_auc:.3f}")

# AUC do champion atual (pelo run que o gerou)
champ_auc = None
try:
    champ = c.get_model_version_by_alias(MODEL_NAME, "champion")
    champ_auc = float(c.get_run(champ.run_id).data.metrics.get("auc"))
    print(f"champion atual: v{champ.version}  AUC={champ_auc:.3f}")
except Exception:
    print("sem champion atual (primeira promoção)")

# COMMAND ----------

# MAGIC %md ## 2. Registrar a versão (sempre vira ao menos challenger)

# COMMAND ----------

try:
    c.get_registered_model(MODEL_NAME)
except Exception:
    try: c.create_registered_model(MODEL_NAME)
    except Exception as e: print("aviso ao criar registered model (quota?):", str(e)[:150])

mv = c.create_model_version(name=MODEL_NAME, source=f"runs:/{RUN_ID}/model", run_id=RUN_ID)
for _ in range(40):
    if c.get_model_version(MODEL_NAME, mv.version).status == "READY": break
    time.sleep(3)
c.set_registered_model_alias(MODEL_NAME, "challenger", int(mv.version))
print(f"registrado v{mv.version} → alias 'challenger'")

# COMMAND ----------

# MAGIC %md ## 3. Portão de avaliação — promove a champion só se AUC ≥ champion

# COMMAND ----------

promovido = (champ_auc is None) or (novo_auc >= champ_auc)
if promovido:
    c.set_registered_model_alias(MODEL_NAME, "champion", int(mv.version))
    print(f"✅ PROMOVIDO a champion (v{mv.version}, AUC {novo_auc:.3f})")
else:
    print(f"⛔ NÃO promovido — AUC {novo_auc:.3f} < champion {champ_auc:.3f}. Fica como challenger.")

# COMMAND ----------

dbutils.notebook.exit(json.dumps({
    "version": int(mv.version), "run_id": RUN_ID, "novo_auc": round(novo_auc, 3),
    "champion_auc": (round(champ_auc, 3) if champ_auc is not None else None),
    "promovido_a_champion": promovido,
}))
