# Databricks notebook source
# MAGIC %md
# MAGIC # 05b · Chamada do modelo via REST API — Vibe NBO
# MAGIC
# MAGIC Pega o **host + token** do próprio contexto do notebook e chama o endpoint de serving
# MAGIC via **REST** (`requests`) — exatamente o que um app/WhatsApp faria em produção.
# MAGIC Também imprime o **curl equivalente** (pra colar no Postman/terminal).

# COMMAND ----------

import requests, json, time

ENDPOINT = "nbo-score-oferta"

# host + token do contexto do notebook (não precisa criar PAT)
ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
HOST = ctx.apiUrl().get()
TOKEN = ctx.apiToken().get()
URL = f"{HOST}/serving-endpoints/{ENDPOINT}/invocations"
print("URL:", URL)

# COMMAND ----------

# MAGIC %md ## Chamada — ofertas candidatas da Maria (customer_id = 1)

# COMMAND ----------

payload = {"dataframe_records": [
    {"customer_id": 1, "tipo_oferta": "cashback",          "nivel_agressividade": 3},
    {"customer_id": 1, "tipo_oferta": "desconto_parceiro", "nivel_agressividade": 1},
    {"customer_id": 1, "tipo_oferta": "bonus_vibes",       "nivel_agressividade": 2},
    {"customer_id": 1, "tipo_oferta": "missao",            "nivel_agressividade": 1},
]}
headers = {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}

t0 = time.time()
resp = requests.post(URL, headers=headers, data=json.dumps(payload))
dt = (time.time() - t0) * 1000
resp.raise_for_status()
preds = resp.json()["predictions"]

print(f"status {resp.status_code} · {dt:.0f} ms\n")
for c, p in zip(payload["dataframe_records"], preds):
    print(f"  {c['tipo_oferta']:18} nível {c['nivel_agressividade']} → P(aceitar) = {p:.3f}")
melhor = max(zip(payload["dataframe_records"], preds), key=lambda x: x[1])
print(f"\n🎯 melhor oferta p/ Maria em tempo real: {melhor[0]['tipo_oferta']} (P={melhor[1]:.3f})")

# COMMAND ----------

# MAGIC %md ## curl equivalente (Postman)

# COMMAND ----------

curl = (
    f"curl -X POST '{URL}' \\\n"
    f"  -H 'Authorization: Bearer <SEU_TOKEN>' \\\n"
    f"  -H 'Content-Type: application/json' \\\n"
    f"  -d '{json.dumps(payload)}'"
)
print(curl)

# COMMAND ----------

dbutils.notebook.exit(json.dumps({
    "url": URL, "status": resp.status_code,
    "maria_melhor": melhor[0]["tipo_oferta"], "maria_p": round(float(melhor[1]), 3),
    "latencia_ms": round(dt),
}))
