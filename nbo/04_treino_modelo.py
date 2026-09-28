# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Treino do modelo + MLflow + Registry (Pilares 1 e 2)
# MAGIC
# MAGIC - Monta o **training set** com FeatureLookup **point-in-time** (as 8 features selecionadas)
# MAGIC - Treina o classificador de **propensão a aceitar** `P(aceitar)`
# MAGIC - Registra no **MLflow** + publica no **Model Registry (UC)** como **champion**
# MAGIC
# MAGIC O modelo é empacotado como **pyfunc que devolve `P(aceitar)`** → batch e RT dão a mesma probabilidade.

# COMMAND ----------

import mlflow, mlflow.pyfunc, time, json
from databricks.feature_engineering import FeatureEngineeringClient, FeatureLookup
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import OneHotEncoder
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import roc_auc_score, accuracy_score
from mlflow.tracking import MlflowClient

mlflow.set_registry_uri("databricks-uc")
dbutils.widgets.text("catalog", "fernando_custodio")
dbutils.widgets.text("schema", "nbo")
CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
_user = spark.sql("SELECT current_user()").first()[0]
EXP = f"/Users/{_user}/nbo/exp_nbo_score_oferta"
mlflow.set_experiment(EXP)   # todos os treinos caem no mesmo experimento (comparar runs)
FT = f"{CATALOG}.{SCHEMA}.customer_features"
MODEL_NAME = f"{CATALOG}.{SCHEMA}.nbo_score_oferta"
FEATURES = ["freq_resgates", "freq_resgates_90d", "vibes_acumulados", "vibes_vencer_30d",
            "vibes_vencidos", "dias_desde_ultimo_resgate", "dias_desde_ultimo_credito",
            "missoes_concluidas_30d"]
fe = FeatureEngineeringClient()

# hiperparâmetros como widgets — mude e rode de novo pra gerar runs comparáveis (Pilar 2)
dbutils.widgets.text("learning_rate", "0.08")
dbutils.widgets.text("max_depth", "6")
dbutils.widgets.text("max_iter", "200")
LR = float(dbutils.widgets.get("learning_rate"))
MD = int(dbutils.widgets.get("max_depth"))
MI = int(dbutils.widgets.get("max_iter"))

# COMMAND ----------

# MAGIC %md ## Training set com point-in-time (Feature Store)

# COMMAND ----------

ofertas = spark.table(f"{CATALOG}.{SCHEMA}.ofertas_historico")
lookups = [FeatureLookup(table_name=FT, lookup_key="customer_id",
                         timestamp_lookup_key="data_oferta", feature_names=FEATURES)]
training_set = fe.create_training_set(
    df=ofertas, feature_lookups=lookups, label="aceitou",
    exclude_columns=["oferta_id", "customer_id", "data_oferta"])  # keys fora das features
df = training_set.load_df().toPandas()
print("shape:", df.shape, "| colunas:", list(df.columns))

# COMMAND ----------

# MAGIC %md ## Treino
# MAGIC Entrada do modelo = as 8 features (da Feature Store) + atributos da oferta (`tipo_oferta`, `nivel_agressividade`).

# COMMAND ----------

X = df.drop(columns=["aceitou"]); y = df["aceitou"].astype(int)
X_tr, X_te, y_tr, y_te = train_test_split(X, y, test_size=0.25, random_state=42, stratify=y)

pre = ColumnTransformer([("tipo", OneHotEncoder(handle_unknown="ignore"), ["tipo_oferta"])], remainder="passthrough")
model = Pipeline([("pre", pre),
                  ("clf", HistGradientBoostingClassifier(max_iter=MI, learning_rate=LR, max_depth=MD, random_state=42))])

class NBOProbaModel(mlflow.pyfunc.PythonModel):
    def __init__(self, pipe): self.pipe = pipe
    def predict(self, context, model_input):
        d = model_input.copy()
        for col in d.columns:
            if col != "tipo_oferta":
                d[col] = d[col].astype("float64")
        return self.pipe.predict_proba(d)[:, 1]

with mlflow.start_run(run_name=f"histgb_lr{LR}_d{MD}_it{MI}") as run:
    model.fit(X_tr, y_tr)
    p = model.predict_proba(X_te)[:, 1]
    auc = roc_auc_score(y_te, p); acc = accuracy_score(y_te, (p >= 0.5).astype(int))
    mlflow.log_params({"algo": "HistGradientBoosting", "n_features": len(FEATURES),
                       "learning_rate": LR, "max_depth": MD, "max_iter": MI})
    mlflow.log_metrics({"auc": auc, "accuracy": acc})
    print(f"AUC={auc:.3f}  ACC={acc:.3f}")
    fe.log_model(model=NBOProbaModel(model), artifact_path="model", flavor=mlflow.pyfunc, training_set=training_set)
    run_id = run.info.run_id
model_uri = f"runs:/{run_id}/model"
print(f"run_id={run_id}  AUC={auc:.3f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Este notebook NÃO registra no UC — só loga o run (pra comparar).
# MAGIC A promoção (registrar versão + champion, com portão de AUC) é o **`04b_registrar`**.

# COMMAND ----------

dbutils.notebook.exit(json.dumps({
    "model": MODEL_NAME, "run_id": run_id, "model_uri": model_uri,
    "auc": round(float(auc), 3), "accuracy": round(float(acc), 3),
    "params": {"learning_rate": LR, "max_depth": MD, "max_iter": MI},
}))
