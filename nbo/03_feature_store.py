# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Feature Store — Vibe NBO (Pilar 3)
# MAGIC
# MAGIC Materializa as **8 features selecionadas no `02`** como uma **feature table de série temporal**
# MAGIC (`customer_features`), com snapshots point-in-time nas datas {cada resgate, cada oferta, hoje}.
# MAGIC Tudo em **Spark** (agregações as-of). O treino (`04`) e o serving usam via FeatureLookup.

# COMMAND ----------

from pyspark.sql import functions as F
from databricks.feature_engineering import FeatureEngineeringClient, FeatureLookup
fe = FeatureEngineeringClient()

dbutils.widgets.text("catalog", "fernando_custodio")
dbutils.widgets.text("schema", "nbo")
CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
FT = f"{CATALOG}.{SCHEMA}.customer_features"
SEL = ["freq_resgates", "freq_resgates_90d", "vibes_acumulados", "vibes_vencer_30d",
       "vibes_vencidos", "dias_desde_ultimo_resgate", "dias_desde_ultimo_credito",
       "missoes_concluidas_30d"]

rg = spark.table(f"{CATALOG}.{SCHEMA}.eventos_resgate")
cr = spark.table(f"{CATALOG}.{SCHEMA}.vibes_creditos")
ms = spark.table(f"{CATALOG}.{SCHEMA}.missoes")
of = spark.table(f"{CATALOG}.{SCHEMA}.ofertas_historico")
cli = spark.table(f"{CATALOG}.{SCHEMA}.clientes")

# COMMAND ----------

# MAGIC %md ## Pontos de snapshot por cliente: {resgates, ofertas, hoje}

# COMMAND ----------

pts = (rg.select("customer_id", F.col("data_evento").cast("timestamp").alias("data_feature"))
       .union(of.select("customer_id", F.col("data_oferta").cast("timestamp").alias("data_feature")))
       .union(cli.select("customer_id", F.current_timestamp().alias("data_feature")))
       .distinct())

def dsub(dias): return F.expr(f"data_feature - INTERVAL {dias} DAYS")

# COMMAND ----------

# MAGIC %md ## Calcular as 8 features as-of cada snapshot (Spark)

# COMMAND ----------

r = (pts.join(rg, "customer_id").where(F.col("data_evento") <= F.col("data_feature"))
     .groupBy("customer_id", "data_feature").agg(
        F.count("*").alias("freq_resgates"),
        F.sum((F.col("data_evento") > dsub(90)).cast("int")).alias("freq_resgates_90d"),
        F.max("data_evento").alias("_ult_resg")))

c = (pts.join(cr, "customer_id").where(F.col("data_credito") <= F.col("data_feature"))
     .groupBy("customer_id", "data_feature").agg(
        F.sum("vibes_creditados").alias("vibes_acumulados"),
        F.sum(F.when((F.col("data_expiracao") > F.col("data_feature")) & (F.col("data_expiracao") <= dsub(-30)), F.col("vibes_creditados")).otherwise(0)).alias("vibes_vencer_30d"),
        F.sum(F.when(F.col("data_expiracao") <= F.col("data_feature"), F.col("vibes_creditados")).otherwise(0)).alias("vibes_vencidos"),
        F.max("data_credito").alias("_ult_cred")))

mm = (pts.join(ms, "customer_id").where(F.col("data_missao") <= F.col("data_feature"))
      .groupBy("customer_id", "data_feature").agg(
        F.sum((F.col("data_missao") > dsub(30)).cast("int")).alias("missoes_concluidas_30d")))

feat = (pts.join(r, ["customer_id", "data_feature"], "left")
        .join(c, ["customer_id", "data_feature"], "left")
        .join(mm, ["customer_id", "data_feature"], "left")
        .withColumn("dias_desde_ultimo_resgate", F.coalesce(F.datediff("data_feature", "_ult_resg"), F.lit(999)))
        .withColumn("dias_desde_ultimo_credito", F.coalesce(F.datediff("data_feature", "_ult_cred"), F.lit(999)))
        .fillna({"freq_resgates": 0, "freq_resgates_90d": 0, "vibes_acumulados": 0,
                 "vibes_vencer_30d": 0, "vibes_vencidos": 0, "missoes_concluidas_30d": 0})
        .select("customer_id", "data_feature", *SEL))

print("snapshots:", feat.count())
feat.filter("customer_id = 1").orderBy(F.col("data_feature").desc()).show(3, truncate=False)

# COMMAND ----------

# MAGIC %md ## Registrar como feature table (série temporal)

# COMMAND ----------

spark.sql(f"DROP TABLE IF EXISTS {FT}")
fe.create_table(
    name=FT,
    primary_keys=["customer_id", "data_feature"],
    timeseries_column="data_feature",
    df=feat,
    description="Features de loyalty selecionadas (NBO Vibe): frequência, Vibes (acum/vencer/vencidos), recência, missões",
)

# COMMAND ----------

# MAGIC %md ## Point-in-time na prática — features da Maria as-of cada oferta

# COMMAND ----------

lookups = [FeatureLookup(table_name=FT, lookup_key="customer_id",
                         timestamp_lookup_key="data_oferta", feature_names=SEL)]
ts = fe.create_training_set(df=of, feature_lookups=lookups, label="aceitou", exclude_columns=["oferta_id"])
tdf = ts.load_df()
print("linhas de treino:", tdf.count())
tdf.filter("customer_id = 1").orderBy("data_oferta").show(truncate=False)

# COMMAND ----------

import json
dbutils.notebook.exit(json.dumps({"feature_table": FT, "features": SEL,
                                  "snapshots": int(feat.count()), "linhas_treino": int(tdf.count())}))
