# Databricks notebook source
# MAGIC %md
# MAGIC # 06a · Base de scores pré-computada — Vibe NBO
# MAGIC
# MAGIC Pontua **todas as combinações** (cliente × tipo × nível) com o modelo champion e grava:
# MAGIC - `base_scored` — 1 linha por (cliente, tipo, nível) com `P(aceitar)` + custo
# MAGIC - `base_clientes_score` — 1 linha por cliente: segmento + features + `score_max` (melhor P(aceitar))
# MAGIC
# MAGIC Serve o **pipeline** (motor) e a **análise de impacto** do App (avaliação de regras instantânea, sem re-chamar o modelo).

# COMMAND ----------

dbutils.widgets.text("catalog", "fernando_custodio")
dbutils.widgets.text("schema", "nbo")
CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
dbutils.widgets.text("model_uri", f"models:/{CATALOG}.{SCHEMA}.nbo_score_oferta@champion", "Model URI")
MODEL_URI = dbutils.widgets.get("model_uri")

from pyspark.sql import functions as F, Window
from databricks.feature_engineering import FeatureEngineeringClient
import mlflow
mlflow.set_registry_uri("databricks-uc")
fe = FeatureEngineeringClient()

# CATALOG e SCHEMA já vêm dos widgets no topo
FT = f"{CATALOG}.{SCHEMA}.customer_features"
FEATURES = ["freq_60d", "freq_total", "vibes_acum", "tenure_dias", "dias_desde_evento_anterior"]
TIPOS = ["cashback", "bonus_vibes", "desconto_parceiro", "missao"]
CUSTO_BASE = {"cashback": 100, "bonus_vibes": 80, "desconto_parceiro": 40, "missao": 20}

# COMMAND ----------

# features mais recentes + recência de hoje + segmento
latest = (spark.table(FT)
    .withColumn("rn", F.row_number().over(Window.partitionBy("customer_id").orderBy(F.col("data_feature").desc())))
    .filter("rn = 1").drop("rn", "data_feature"))
recencia_hoje = (spark.table(f"{CATALOG}.{SCHEMA}.eventos_resgate")
    .groupBy("customer_id").agg(F.datediff(F.current_date(), F.max("data_evento")).alias("recencia_hoje")))

base = (spark.table(f"{CATALOG}.{SCHEMA}.clientes").select("customer_id")
        .join(latest, "customer_id", "left").join(recencia_hoje, "customer_id", "left")
        .fillna({"freq_60d": 0, "freq_total": 0, "vibes_acum": 0, "tenure_dias": 0,
                 "dias_desde_evento_anterior": 999, "recencia_hoje": 999}))
seg = (F.when((F.col("recencia_hoje") > 90) & (F.col("freq_total") >= 2), "em_risco")
        .when(F.col("freq_total") <= 1, "novo")
        .when((F.col("recencia_hoje") <= 30) & (F.col("freq_total") >= 5), "campeao")
        .otherwise("ativo"))
base = base.withColumn("segmento", seg)

# COMMAND ----------

# candidatas: cliente × tipo × nível (1..3)
cand = (base
        .withColumn("tipo_oferta", F.explode(F.array(*[F.lit(t) for t in TIPOS])))
        .withColumn("nivel_agressividade", F.explode(F.array(F.lit(1), F.lit(2), F.lit(3))))
        .withColumn("data_oferta", F.current_timestamp()))

scored = fe.score_batch(model_uri=MODEL_URI, df=cand, result_type="double")
scored = scored.withColumnRenamed("prediction", "p_aceitar")

custo_expr = F.create_map(*sum([[F.lit(k), F.lit(v)] for k, v in CUSTO_BASE.items()], []))
scored = scored.withColumn("custo_vibes", custo_expr[F.col("tipo_oferta")] * F.col("nivel_agressividade"))

base_scored = scored.select("customer_id", "segmento", "tipo_oferta", "nivel_agressividade",
                            "p_aceitar", "custo_vibes",
                            "freq_60d", "freq_total", "vibes_acum", "recencia_hoje")
(base_scored.write.mode("overwrite").option("overwriteSchema", "true")
 .saveAsTable(f"{CATALOG}.{SCHEMA}.base_scored"))

# COMMAND ----------

# score por cliente (melhor P(aceitar) entre todas as candidatas)
base_cli = (base_scored.groupBy("customer_id", "segmento",
                                "freq_60d", "freq_total", "vibes_acum", "recencia_hoje")
            .agg(F.max("p_aceitar").alias("score_max")))
(base_cli.write.mode("overwrite").option("overwriteSchema", "true")
 .saveAsTable(f"{CATALOG}.{SCHEMA}.base_clientes_score"))

# COMMAND ----------

import json
dbutils.notebook.exit(json.dumps({
    "base_scored": int(base_scored.count()),
    "base_clientes_score": int(base_cli.count()),
}))
