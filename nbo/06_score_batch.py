# Databricks notebook source
# MAGIC %pip install databricks-feature-engineering
# MAGIC %restart_python

# COMMAND ----------

# MAGIC %md
# MAGIC # 06 · Scoragem em Batch — Vibe NBO (Pilar 4 · balcão batch)
# MAGIC
# MAGIC O "balcão do dia-a-dia": pontua a **base inteira** com o modelo **champion** e grava numa tabela Gold.
# MAGIC O `score_batch` faz o **feature lookup point-in-time sozinho** (mesmo modelo/artefato do RT → sem skew).
# MAGIC `data_oferta = agora` → usa as features atuais.

# COMMAND ----------

dbutils.widgets.text("catalog", "fernando_custodio")
dbutils.widgets.text("schema", "nbo")
CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
dbutils.widgets.text("model_uri", f"models:/{CATALOG}.{SCHEMA}.nbo_score_oferta@champion", "Model URI")
MODEL_URI = dbutils.widgets.get("model_uri")

# data do score (carimba scored_at). vazio = agora. Ex.: 2026-09-22 (normal) / 2026-09-23 (Black Friday)
dbutils.widgets.text("data_score", "2026-09-22", "Data do score (YYYY-MM-DD, vazio=agora)")

from pyspark.sql import functions as F, Window
from databricks.feature_engineering import FeatureEngineeringClient, FeatureLookup
from mlflow.tracking import MlflowClient
from datetime import datetime, timezone
import mlflow, json
mlflow.set_registry_uri("databricks-uc")
fe = FeatureEngineeringClient()

# CATALOG e SCHEMA já vêm dos widgets no topo
TIPOS = ["cashback", "bonus_vibes", "desconto_parceiro", "missao"]
CUSTO_BASE = {"cashback": 100, "bonus_vibes": 80, "desconto_parceiro": 40, "missao": 20}

# --- para o snapshot de inferência (monitoramento de drift) ---
FT = f"{CATALOG}.{SCHEMA}.customer_features"
SEL = ["freq_resgates", "freq_resgates_90d", "vibes_acumulados", "vibes_vencer_30d",
       "vibes_vencidos", "dias_desde_ultimo_resgate", "dias_desde_ultimo_credito",
       "missoes_concluidas_30d"]
# timestamp único do run (senão current_timestamp() varia por avaliação → não casa no join)
# usa a data do widget (carimba o dia da janela p/ o monitor); vazio = agora
_ds = dbutils.widgets.get("data_score").strip()
RUN_TS = datetime.now(timezone.utc) if not _ds else datetime.fromisoformat(_ds).replace(tzinfo=timezone.utc)
# versão do champion — separa champion/challenger no monitor
MODEL_VERSION = MlflowClient(registry_uri="databricks-uc").get_model_version_by_alias(
    f"{CATALOG}.{SCHEMA}.nbo_score_oferta", "champion").version
print("RUN_TS:", RUN_TS, "| model_version:", MODEL_VERSION)

# COMMAND ----------

# MAGIC %md ## 1. Candidatas: cliente × tipo × nível (com data_oferta = hoje)

# COMMAND ----------

clientes = spark.table(f"{CATALOG}.{SCHEMA}.clientes").select("customer_id")
cand = (clientes
        .withColumn("tipo_oferta", F.explode(F.array(*[F.lit(t) for t in TIPOS])))
        .withColumn("nivel_agressividade", F.explode(F.array(F.lit(1), F.lit(2), F.lit(3))))
        .withColumn("data_oferta", F.lit(RUN_TS).cast("timestamp")))
print("candidatas:", cand.count())

# COMMAND ----------

# MAGIC %md ## 2. score_batch + snapshot de inferência (predição + features usadas → drift)
# MAGIC O `score_batch` faz o lookup point-in-time sozinho pra pontuar. Mas ele **não devolve** as
# MAGIC features usadas — então surfamos as **mesmas** features (mesmo lookup, mesmo `data_oferta`)
# MAGIC e gravamos junto com a predição. Isso congela o "retrato do mundo" no instante da inferência,
# MAGIC sem skew, e dá ao monitor a linha do tempo pra detectar drift.

# COMMAND ----------

# 1) score_batch: modelo busca features sozinho e devolve P(aceitar)
scored = (fe.score_batch(model_uri=MODEL_URI, df=cand, result_type="double")
          .withColumnRenamed("prediction", "p_aceitar"))

# 2) surfar as MESMAS features usadas (mesmo lookup point-in-time do treino) pra gravar o snapshot
lookups = [FeatureLookup(table_name=FT, lookup_key="customer_id",
                         timestamp_lookup_key="data_oferta", feature_names=SEL)]
cand_feat = fe.create_training_set(df=cand, feature_lookups=lookups, label=None).load_df()

# 3) juntar predição + features (mesma chave, mesmo data_oferta fixo → casa 1:1)
keys = ["customer_id", "tipo_oferta", "nivel_agressividade", "data_oferta"]
custo_expr = F.create_map(*sum([[F.lit(k), F.lit(v)] for k, v in CUSTO_BASE.items()], []))
out = (cand_feat.join(scored.select(*keys, "p_aceitar"), keys)
       .withColumn("custo_vibes", custo_expr[F.col("tipo_oferta")] * F.col("nivel_agressividade"))
       .withColumn("model_version", F.lit(MODEL_VERSION))
       .withColumnRenamed("data_oferta", "scored_at")
       .select("customer_id", "tipo_oferta", "nivel_agressividade", "p_aceitar", "custo_vibes",
               *SEL, "model_version", "scored_at"))

# 4) APPEND → acumula histórico pro monitor ter linha do tempo.
#    1ª vez após esta mudança: descomente a linha de overwriteSchema abaixo p/ recriar o schema,
#    depois volte para append.
# out.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{CATALOG}.{SCHEMA}.batch_scores")
out.write.mode("append").saveAsTable(f"{CATALOG}.{SCHEMA}.batch_scores")

# COMMAND ----------

# MAGIC %md ## 3. NBO — melhor oferta por cliente (maior P(aceitar))

# COMMAND ----------

# baseline: melhor = maior P(aceitar). (A otimização por valor esperado p_aceitar×valor − custo
# fica no notebook 08_motor_oferta.) Fonte = `out`, que já tem custo_vibes/scored_at/model_version.
w = Window.partitionBy("customer_id").orderBy(F.col("p_aceitar").desc())
nbo = (out.withColumn("rk", F.row_number().over(w)).filter("rk = 1")
       .select("customer_id", "tipo_oferta", "nivel_agressividade", "p_aceitar", "custo_vibes",
               "model_version", "scored_at"))
(nbo.write.mode("overwrite").option("overwriteSchema", "true")
 .saveAsTable(f"{CATALOG}.{SCHEMA}.gold_nbo_batch"))

# COMMAND ----------

# MAGIC %md ## Resultado

# COMMAND ----------

display(spark.table(f"{CATALOG}.{SCHEMA}.gold_nbo_batch")
        .groupBy("tipo_oferta").count().orderBy(F.col("count").desc()))
print("NBO da Maria:")
display(spark.table(f"{CATALOG}.{SCHEMA}.gold_nbo_batch").filter("customer_id = 1"))

# COMMAND ----------

dbutils.notebook.exit(json.dumps({
    "batch_scores": spark.table(f"{CATALOG}.{SCHEMA}.batch_scores").count(),
    "gold_nbo_batch": spark.table(f"{CATALOG}.{SCHEMA}.gold_nbo_batch").count(),
}))
