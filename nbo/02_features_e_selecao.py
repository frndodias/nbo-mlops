# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Features & Seleção — Vibe NBO
# MAGIC
# MAGIC **Análise de features (rápida, em Spark).** Calcula candidatas *as-of a oferta* com agregações,
# MAGIC mede **sinal / redundância / importância** e **seleciona** o que vira feature store no `03`.
# MAGIC
# MAGIC > Aqui os Vibes (saldo/vencer/vencidos) são **aproximados** — suficiente pra medir sinal.
# MAGIC > A versão exata (FIFO point-in-time) é construída no `03`, só pras features **selecionadas**.

# COMMAND ----------

from pyspark.sql import functions as F
import pandas as pd
import matplotlib.pyplot as plt

VERDE, LARANJA, ROXO = "#2D8659", "#E67E3C", "#8E6FB0"
plt.rcParams.update({"figure.facecolor": "white", "axes.grid": True, "grid.alpha": 0.25,
                     "axes.spines.top": False, "axes.spines.right": False})
dbutils.widgets.text("catalog", "fernando_custodio")
dbutils.widgets.text("schema", "nbo")
CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")

oj = spark.table(f"{CATALOG}.{SCHEMA}.ofertas_historico").select(
    "oferta_id", "customer_id", "data_oferta", "aceitou")
rg = spark.table(f"{CATALOG}.{SCHEMA}.eventos_resgate")
cr = spark.table(f"{CATALOG}.{SCHEMA}.vibes_creditos")
ms = spark.table(f"{CATALOG}.{SCHEMA}.missoes")

def dsub(dias):  # data_oferta - N dias
    return F.expr(f"data_oferta - INTERVAL {dias} DAYS")

# COMMAND ----------

# MAGIC %md ## Candidatas as-of a oferta (agregações Spark)

# COMMAND ----------

# resgates ocorridos até a data da oferta
r = (oj.join(rg, "customer_id").where(F.col("data_evento") <= F.col("data_oferta"))
     .groupBy("oferta_id").agg(
        F.count("*").alias("freq_resgates"),
        F.sum((F.col("data_evento") > dsub(30)).cast("int")).alias("freq_resgates_30d"),
        F.sum((F.col("data_evento") > dsub(60)).cast("int")).alias("freq_resgates_60d"),
        F.sum((F.col("data_evento") > dsub(90)).cast("int")).alias("freq_resgates_90d"),
        F.datediff(F.first("data_oferta"), F.max("data_evento")).alias("dias_desde_ultimo_resgate"),
        F.sum("vibes_gastos").alias("_gastos"),
        F.round(F.avg("vibes_gastos"), 1).alias("ticket_medio_resgate"),
        F.countDistinct(F.when(F.col("data_evento") > dsub(90), F.col("categoria"))).alias("n_categorias_90d")))

# créditos até a data da oferta (Vibes aproximados)
c = (oj.join(cr, "customer_id").where(F.col("data_credito") <= F.col("data_oferta"))
     .groupBy("oferta_id").agg(
        F.sum("vibes_creditados").alias("vibes_acumulados"),
        F.sum(F.when(F.col("data_expiracao") <= F.col("data_oferta"), F.col("vibes_creditados")).otherwise(0)).alias("vibes_vencidos"),
        F.sum(F.when((F.col("data_expiracao") > F.col("data_oferta")) & (F.col("data_expiracao") <= dsub(-15)), F.col("vibes_creditados")).otherwise(0)).alias("vibes_vencer_15d"),
        F.sum(F.when((F.col("data_expiracao") > F.col("data_oferta")) & (F.col("data_expiracao") <= dsub(-30)), F.col("vibes_creditados")).otherwise(0)).alias("vibes_vencer_30d"),
        F.sum(F.when((F.col("data_expiracao") > F.col("data_oferta")) & (F.col("data_expiracao") <= dsub(-60)), F.col("vibes_creditados")).otherwise(0)).alias("vibes_vencer_60d"),
        F.datediff(F.first("data_oferta"), F.max("data_credito")).alias("dias_desde_ultimo_credito")))

# missões até a data da oferta
mm = (oj.join(ms, "customer_id").where(F.col("data_missao") <= F.col("data_oferta"))
      .groupBy("oferta_id").agg(
        F.sum((F.col("data_missao") > dsub(30)).cast("int")).alias("missoes_concluidas_30d"),
        F.datediff(F.first("data_oferta"), F.max("data_missao")).alias("dias_desde_ultima_missao")))

feat = (oj.join(r, "oferta_id", "left").join(c, "oferta_id", "left").join(mm, "oferta_id", "left")
        .fillna(0)
        .withColumn("vibes_saldo", F.greatest(F.lit(0), F.col("vibes_acumulados") - F.col("_gastos") - F.col("vibes_vencidos")))
        .withColumn("taxa_uso_vibes", F.when(F.col("vibes_acumulados") > 0, F.round(F.col("_gastos")/F.col("vibes_acumulados"), 3)).otherwise(0.0))
        .withColumn("dias_desde_ultimo_resgate", F.coalesce("dias_desde_ultimo_resgate", F.lit(999)))
        .withColumn("dias_desde_ultimo_credito", F.coalesce("dias_desde_ultimo_credito", F.lit(999)))
        .withColumn("dias_desde_ultima_missao", F.coalesce("dias_desde_ultima_missao", F.lit(999))))

CANDIDATAS = ["freq_resgates","freq_resgates_30d","freq_resgates_60d","freq_resgates_90d",
              "vibes_acumulados","vibes_saldo","vibes_vencer_15d","vibes_vencer_30d","vibes_vencer_60d",
              "vibes_vencidos","dias_desde_ultimo_resgate","dias_desde_ultimo_credito",
              "dias_desde_ultima_missao","missoes_concluidas_30d","ticket_medio_resgate",
              "n_categorias_90d","taxa_uso_vibes"]

df = feat.select("aceitou", *CANDIDATAS).toPandas()   # ~31k linhas, pequeno
TAXA = df.aceitou.mean(); print("ofertas:", len(df), "| taxa de aceite:", round(TAXA, 3))

# COMMAND ----------

# MAGIC %md ## 1. Sinal — correlação de cada candidata com o aceite

# COMMAND ----------

corrs = df[CANDIDATAS].corrwith(df["aceitou"]).sort_values()
fig, ax = plt.subplots(figsize=(9, 7))
ax.barh(corrs.index, corrs.values, color=[VERDE if v >= 0 else LARANJA for v in corrs.values], alpha=0.85)
ax.axvline(0, color="#888", lw=1); ax.set_title("Correlação de cada candidata com o aceite", fontweight="bold")
fig.tight_layout(); plt.show()

# COMMAND ----------

# MAGIC %md ## 2. Colinearidade (candidatas redundantes)

# COMMAND ----------

corr = df[CANDIDATAS].corr()
fig, ax = plt.subplots(figsize=(11, 10))
im = ax.imshow(corr, cmap="RdBu_r", vmin=-1, vmax=1)
ax.set_xticks(range(len(CANDIDATAS))); ax.set_xticklabels(CANDIDATAS, rotation=45, ha="right", fontsize=8)
ax.set_yticks(range(len(CANDIDATAS))); ax.set_yticklabels(CANDIDATAS, fontsize=8)
fig.colorbar(im, fraction=0.046, pad=0.04); ax.set_title("Matriz de correlação (candidatas)", fontweight="bold")
fig.tight_layout(); plt.show()

# COMMAND ----------

# MAGIC %md ## 3. Importância num modelo (permutation)

# COMMAND ----------

from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.inspection import permutation_importance
from sklearn.model_selection import train_test_split
from sklearn.metrics import roc_auc_score

X, y = df[CANDIDATAS].astype(float), df["aceitou"].astype(int)
Xtr, Xte, ytr, yte = train_test_split(X, y, test_size=0.25, random_state=42, stratify=y)
m = HistGradientBoostingClassifier(max_iter=200, random_state=42).fit(Xtr, ytr)
auc = roc_auc_score(yte, m.predict_proba(Xte)[:, 1])
imp = permutation_importance(m, Xte, yte, n_repeats=5, random_state=42, scoring="roc_auc")
imp_s = pd.Series(imp.importances_mean, index=CANDIDATAS).sort_values()
fig, ax = plt.subplots(figsize=(9, 7))
ax.barh(imp_s.index, imp_s.values, color=ROXO, alpha=0.85)
ax.set_title(f"Importância (permutation) — AUC = {auc:.3f}", fontweight="bold")
fig.tight_layout(); plt.show()

# COMMAND ----------

# MAGIC %md ## 4. Seleção automática (bom sinal + sem redundância)

# COMMAND ----------

PISO = 0.001
ranked = imp_s.sort_values(ascending=False)
selecionadas, descartadas = [], []
for f in ranked.index:
    if ranked[f] < PISO:
        descartadas.append((f, "sinal fraco")); continue
    red = next((s for s in selecionadas if abs(corr.loc[f, s]) > 0.9), None)
    descartadas.append((f, f"redundante com {red}")) if red else selecionadas.append(f)

print("✅ SELECIONADAS:", selecionadas)
for f, mot in descartadas: print("   ❌", f, "→", mot)

# COMMAND ----------

import json
dbutils.notebook.exit(json.dumps({"ofertas": int(len(df)), "auc": round(float(auc), 3),
                                  "selecionadas": selecionadas,
                                  "descartadas": {f: mot for f, mot in descartadas}}))
