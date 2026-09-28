# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Gerar dados (cru) — Vibe NBO
# MAGIC
# MAGIC **Só dados crus.** Nada de feature aqui — engenharia de features é o `02`.
# MAGIC
# MAGIC | Tabela | O que é |
# MAGIC |---|---|
# MAGIC | `clientes` | dimensão (região, empresa_contratante, faixa_etaria, categoria_preferida) |
# MAGIC | `eventos_resgate` | resgates (data, Vibes gastos, categoria) |
# MAGIC | `vibes_creditos` | ledger: crédito de Vibes + data de expiração (12 meses) |
# MAGIC | `missoes` | conclusões de missão gamificada |
# MAGIC | `ofertas_historico` | ofertas + **label observado** `aceitou` |
# MAGIC
# MAGIC O `aceitou` é gerado a partir de **traços latentes** do cliente (engajamento, sensibilidade a urgência)
# MAGIC + atributos da oferta. As features (freq, saldo, Vibes a vencer…) serão **medições** desses latentes,
# MAGIC calculadas no `02` — por isso a descoberta de features lá não é circular.

# COMMAND ----------

import numpy as np, pandas as pd
from datetime import datetime

dbutils.widgets.text("catalog", "fernando_custodio")
dbutils.widgets.text("schema", "nbo")
CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
spark.sql(f"USE CATALOG {CATALOG}"); spark.sql(f"USE SCHEMA {SCHEMA}")
rng = np.random.default_rng(42)
HOJE = pd.Timestamp(datetime.now().date())
N_CLIENTES = 12000
VALIDADE_DIAS = 365

CATEGORIAS = ["mercado", "combustivel", "farmacia", "pet", "entretenimento", "moda"]
TIPOS_OFERTA = ["cashback", "bonus_vibes", "desconto_parceiro", "missao"]
REGIOES = ["Sudeste", "Nordeste", "Sul", "Centro-Oeste", "Norte"]; REGIAO_P = [0.42, 0.27, 0.14, 0.08, 0.09]
EMPRESAS = ["Banco Alfa", "Varejo Beta", "Telecom Gama", "Farmácia Delta", "Indústria Épsilon"]; EMPRESA_P = [0.30, 0.28, 0.18, 0.14, 0.10]
FAIXAS = ["18-24", "25-34", "35-44", "45-59", "60+"]; FAIXA_P = [0.16, 0.30, 0.26, 0.20, 0.08]
TIPO_MISSAO = ["nota_fiscal", "quiz", "indicacao"]

# COMMAND ----------

# MAGIC %md ## Clientes (+ traços latentes, que NÃO são salvos)

# COMMAND ----------

ids = np.arange(1, N_CLIENTES + 1)
dias_max = (HOJE - pd.Timestamp("2023-01-01")).days - 120
data_entrada = pd.Timestamp("2023-01-01") + pd.to_timedelta(rng.integers(0, dias_max, N_CLIENTES), unit="D")
engaj = np.clip(rng.beta(2, 3, N_CLIENTES), 0.02, 0.98)          # latente: engajamento
sens_urg = rng.uniform(0.3, 1.0, N_CLIENTES)                     # latente: sensibilidade a urgência
cat_pref = rng.choice(CATEGORIAS, N_CLIENTES, p=[0.30, 0.22, 0.18, 0.10, 0.12, 0.08])

clientes_pd = pd.DataFrame({
    "customer_id": ids, "nome": [f"Cliente {i}" for i in ids],
    "data_entrada_programa": data_entrada,
    "regiao": rng.choice(REGIOES, N_CLIENTES, p=REGIAO_P),
    "empresa_contratante": rng.choice(EMPRESAS, N_CLIENTES, p=EMPRESA_P),
    "faixa_etaria": rng.choice(FAIXAS, N_CLIENTES, p=FAIXA_P),
    "categoria_preferida": cat_pref,
    "canal_cadastro": rng.choice(["app", "whatsapp", "web"], N_CLIENTES, p=[0.7, 0.2, 0.1]),
    "_engaj": engaj, "_urg": sens_urg,
})
clientes_pd.loc[0, ["nome", "data_entrada_programa", "regiao", "empresa_contratante", "faixa_etaria",
                    "categoria_preferida", "canal_cadastro", "_engaj", "_urg"]] = \
    ["Maria", pd.Timestamp("2023-03-10"), "Sudeste", "Banco Alfa", "35-44", "mercado", "app", 0.55, 0.9]

# COMMAND ----------

# MAGIC %md ## Eventos crus: resgates, créditos (ledger), missões, ofertas

# COMMAND ----------

resg, cred, miss, ofer = {}, {}, {}, {}
r_rows, c_rows, m_rows = [], [], []
for i in range(N_CLIENTES):
    cid = int(ids[i]); ent = clientes_pd.loc[i, "data_entrada_programa"]; e = clientes_pd.loc[i, "_engaj"]
    tdias = max((HOJE - ent).days, 5); cpref = clientes_pd.loc[i, "categoria_preferida"]

    n = int(rng.poisson(max(e * tdias / 30 * 0.8, 0.1)))
    rs = []
    for off in np.sort(rng.integers(0, tdias, n)):
        d = ent + pd.Timedelta(days=int(off)); v = int(rng.integers(20, 400))
        cat = cpref if rng.random() < 0.65 else rng.choice(CATEGORIAS)
        rs.append((d, v)); r_rows.append((cid, d, v, cat))
    resg[cid] = rs

    nc = int(rng.poisson(max(e * tdias / 40, 0.3)))
    cs = []
    for off in np.sort(rng.integers(0, tdias, max(nc, 1))):
        d = ent + pd.Timedelta(days=int(off)); amt = int(rng.integers(50, 500))
        cs.append((d, amt, d + pd.Timedelta(days=VALIDADE_DIAS))); c_rows.append((cid, d, amt, d + pd.Timedelta(days=VALIDADE_DIAS)))
    cred[cid] = cs

    nm = int(rng.poisson(max(e * tdias / 55, 0.1)))
    ms = []
    for off in (np.sort(rng.integers(0, tdias, nm)) if nm else []):
        d = ent + pd.Timedelta(days=int(off)); ms.append(d); m_rows.append((cid, d, str(rng.choice(TIPO_MISSAO))))
    miss[cid] = ms

    no = max(int(rng.poisson(2.5)), 1)
    ofer[cid] = [(ent + pd.Timedelta(days=int(rng.integers(3, tdias))),
                  str(rng.choice(TIPOS_OFERTA, p=[0.30, 0.25, 0.30, 0.15])), int(rng.integers(1, 4))) for _ in range(no)]

# ---- Maria: resgates pequenos, lote grande vencendo em ~20 dias, missões recentes ----
maria_r = [(HOJE - pd.Timedelta(days=d), 40) for d in [520,410,300,210,150,90,40,3]]
resg[1] = maria_r
r_rows = [r for r in r_rows if r[0] != 1] + [(1, d, v, "mercado") for d, v in maria_r]
maria_c = [(HOJE - pd.Timedelta(days=345), 900, HOJE + pd.Timedelta(days=20)),
           (HOJE - pd.Timedelta(days=60), 300, HOJE + pd.Timedelta(days=305))]
cred[1] = maria_c
c_rows = [r for r in c_rows if r[0] != 1] + [(1, d, a, e) for d, a, e in maria_c]
maria_m = [HOJE - pd.Timedelta(days=25), HOJE - pd.Timedelta(days=10)]
miss[1] = maria_m
m_rows = [r for r in m_rows if r[0] != 1] + [(1, d, "nota_fiscal") for d in maria_m]
ofer[1] = [(HOJE - pd.Timedelta(days=200), "desconto_parceiro", 1),
           (HOJE - pd.Timedelta(days=35), "cashback", 3)]

# COMMAND ----------

# MAGIC %md ## Label observado (a partir de latentes + oferta)
# MAGIC Sem feature store: usamos só agregados triviais do cru pra tornar o comportamento realista
# MAGIC (recência, missões recentes, se há Vibes vencendo). Nada disso é salvo como feature.

# COMMAND ----------

def sigmoid(x): return 1.0 / (1.0 + np.exp(-x))
TIPO_BASE = {"cashback": 0.5, "bonus_vibes": 0.2, "desconto_parceiro": 0.05, "missao": -0.2}

of_rows = []; oid = 0
for i in range(N_CLIENTES):
    cid = int(ids[i]); rs, cs, ms = resg[cid], cred[cid], miss[cid]
    e = clientes_pd.loc[i, "_engaj"]; urg = clientes_pd.loc[i, "_urg"]
    for (d, tipo, nivel) in ofer[cid]:
        oid += 1
        rd = [dt for dt, _ in rs if dt <= d]
        recency = (d - max(rd)).days if rd else 365
        miss30 = sum(1 for dt in ms if d - pd.Timedelta(days=30) < dt <= d)
        # Vibes vencendo em 30d na data da oferta (filtro simples do ledger — aproximação p/ o label)
        exp_soon = sum(a for dc, a, ex in cs if dc <= d and d < ex <= d + pd.Timedelta(days=30))
        resgate_offer = tipo in ("cashback", "bonus_vibes", "desconto_parceiro")
        latent = (-2.6 + 0.45*nivel + TIPO_BASE[tipo]
                  + 2.2*e                                   # engajamento (latente) — driver principal
                  - 0.004*min(recency, 365)
                  + urg * 0.004*min(exp_soon, 500) * (1.6 if resgate_offer else 0.3)  # ⭐ urgência
                  + 0.15*miss30
                  + rng.normal(0, 0.35))
        of_rows.append((oid, cid, d, tipo, nivel, int(rng.random() < sigmoid(latent))))

# Maria: cashback aceito, desconto_parceiro ignorado
of_rows = [o for o in of_rows if o[1] != 1]
of_rows += [(oid+1, 1, HOJE - pd.Timedelta(days=200), "desconto_parceiro", 1, 0),
            (oid+2, 1, HOJE - pd.Timedelta(days=35), "cashback", 3, 1)]

# COMMAND ----------

# MAGIC %md ## Escrever tabelas (só dados crus)

# COMMAND ----------

def salvar(df, nome):
    (spark.createDataFrame(df).write.mode("overwrite").option("overwriteSchema", "true")
     .saveAsTable(f"{CATALOG}.{SCHEMA}.{nome}"))

salvar(clientes_pd.drop(columns=["_engaj", "_urg"]), "clientes")
salvar(pd.DataFrame(r_rows, columns=["customer_id","data_evento","vibes_gastos","categoria"]), "eventos_resgate")
salvar(pd.DataFrame(c_rows, columns=["customer_id","data_credito","vibes_creditados","data_expiracao"]), "vibes_creditos")
salvar(pd.DataFrame(m_rows, columns=["customer_id","data_missao","tipo_missao"]), "missoes")
salvar(pd.DataFrame(of_rows, columns=["oferta_id","customer_id","data_oferta","tipo_oferta","nivel_agressividade","aceitou"]), "ofertas_historico")

for t in ["clientes","eventos_resgate","vibes_creditos","missoes","ofertas_historico"]:
    spark.sql(f"ALTER TABLE {CATALOG}.{SCHEMA}.{t} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")

# COMMAND ----------

import json
dbutils.notebook.exit(json.dumps({
    "clientes": len(clientes_pd), "resgates": len(r_rows), "creditos": len(c_rows),
    "missoes": len(m_rows), "ofertas": len(of_rows),
    "taxa_aceite": round(float(np.mean([o[5] for o in of_rows])), 3),
}))
