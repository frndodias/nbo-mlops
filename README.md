# NBO — MLOps end-to-end no Databricks

Demo de **ciclo completo de ML (MLOps)** sobre um caso de **Next Best Offer (NBO)** para um programa de pontos/recompensa. Cobre os 4 pilares:

1. **Construção do modelo** — treino de um classificador de propensão `P(aceitar)`
2. **Versionamento & experimentos** — MLflow Tracking + Model Registry no Unity Catalog (aliases `champion`/`challenger`)
3. **Feature Store** — features de cliente com *point-in-time correctness* (offline + online)
4. **Serving** — batch scoring + endpoint real-time
5. **Operação** — monitoramento de *data drift* (Lakehouse Monitoring)

O projeto é empacotado como um **Databricks Asset Bundle (DAB)** — parametrizável e pronto para deploy em qualquer workspace.

---

## 🏗️ A jornada (o que o pipeline faz)

Dado o histórico de um cliente no programa (resgates, pontos, missões, ofertas passadas com o rótulo *aceitou*), o modelo prevê a **probabilidade de aceite** de cada oferta candidata. O motor de decisão combina essa probabilidade com o **custo** de cada oferta (valor esperado) para escolher a **melhor oferta** por cliente.

```
Dados → Features (point-in-time) → Modelo (P(aceitar)) → Serving (batch + RT) → Motor de decisão → Monitoramento
```

---

## ✅ Pré-requisitos

- **Workspace Databricks** com **Unity Catalog** habilitado
- Um **catálogo** e um **schema** onde os assets serão criados (você tem permissão de `CREATE`)
- **Databricks CLI** ≥ 0.240 instalado e autenticado (`databricks auth login`)
- Compute com **ML Runtime** (necessário para Feature Engineering + MLflow) — o job já provisiona
- *(Opcional — camada online/app)* uma instância **Lakebase** e um **SQL Warehouse**

---

## ⚙️ Configuração

Toda a parametrização fica no **`databricks.yml`**. Ajuste as variáveis para o **seu** ambiente:

| Variável | Onde | O que é |
|----------|------|---------|
| `catalog` | `databricks.yml` (target `dev`) | Catálogo UC onde os assets vivem |
| `schema` | `databricks.yml` (target `dev`) | Schema dentro do catálogo |
| `workspace.host` | `databricks.yml` (target `dev`) | Host do seu workspace (ou use `--profile`) |

O bundle passa `catalog`/`schema` para os notebooks automaticamente (via *job parameters* → *widgets*). Rodando um notebook **interativamente**, os *widgets* no topo já vêm com um default — basta ajustar na UI.

> **Camada opcional (Lakebase / App)**: os notebooks `05`, `08`, `09` e o app usam Lakebase. Ajuste `LB_HOST` (nos notebooks) e as variáveis de ambiente em `nbo/app/app.yaml` (`LAKEBASE_HOST`, `DATABRICKS_WAREHOUSE_ID`, `UC_CATALOG`, `UC_SCHEMA`).

---

## 🚀 Deploy & execução

```bash
# 1. Autentique no seu workspace
databricks auth login --host https://SEU-WORKSPACE.cloud.databricks.com

# 2. Valide o bundle
databricks bundle validate --target dev

# 3. Faça deploy (sobe notebooks + job para o workspace)
databricks bundle deploy --target dev

# 4. Rode o pipeline end-to-end
databricks bundle run nbo_pipeline --target dev
```

O job `nbo_pipeline` roda o **núcleo ML** em ordem: gerar dados → features → feature store → treino → registro → batch score → base de scores.

---

## 📓 Notebooks (ordem e propósito)

| # | Notebook | O que faz | Precisa de |
|---|----------|-----------|------------|
| 01 | `01_gerar_dados.py` | Gera dados crus sintéticos (clientes, resgates, créditos de pontos, missões, ofertas com o rótulo `aceitou`) | UC |
| 02 | `02_features_e_selecao.py` | Analisa candidatas a feature (sinal/redundância/importância) e seleciona as 8 features | UC |
| 03 | `03_feature_store.py` | Materializa as 8 features como *feature table* de série temporal (point-in-time) | UC |
| 04 | `04_treino_modelo.py` | Treina o classificador de propensão `P(aceitar)` com FeatureLookup point-in-time; loga no MLflow e registra no UC | UC + ML Runtime |
| 04b | `04b_registrar.py` | Registra e **promove** o modelo com **portão de avaliação** (só vira `champion` se o AUC ≥ atual) | UC |
| 05 | `05_online_store.py` | Publica as features no **online store (Lakebase)** para o serving real-time | UC + **Lakebase** |
| 06 | `06_score_batch.py` | **Batch scoring** + *snapshot de inferência* (grava predição + features + `scored_at`) — base para monitorar drift | UC |
| 06rt | `06_serving_rt.py` | Cria/atualiza o **endpoint de serving real-time** a partir do `champion` | UC + Serving |
| 06b | `06b_chamada_api.py` | Exemplo de chamada **REST** ao endpoint (imprime o curl equivalente) | Serving |
| 07 | `07_score_base.py` | Pré-computa a base de scores (cliente × tipo × nível) → `base_scored`, `base_clientes_score` | UC |
| 08 | `08_motor_oferta.py` | **Motor de oferta (batch)**: lê regras do Lakebase, aplica budget e gera `gold_ofertas_final` | UC + **Lakebase** |
| 09 | `09_load_base_lakebase.py` | Carrega a base de scores no **Lakebase** para o app | UC + **Lakebase** |

**Extras:**
- `nbo/app/` — Databricks App (Streamlit): UI do motor de oferta (edição de regras + simulação de impacto)
- `nbo/postman_nbo_serving.json` — collection Postman para chamar o endpoint via REST

---

## 📊 Monitoramento de drift (operação)

Após rodar o `06_score_batch` (que grava o *snapshot de inferência* com `scored_at` + features), crie um **Lakehouse Monitoring** do tipo **Time series** sobre a tabela `batch_scores`:

- **Timestamp column:** `scored_at`
- **Granularity:** `1 day`
- (opcional) **slice** por `tipo_oferta`

O dashboard auto-gerado mostra o *drift* das features e da predição ao longo do tempo (ex.: teste KS por coluna). Rode o `06_score_batch` em datas diferentes (widget `data_score`) para gerar janelas comparáveis.

---

## 🔁 Padrão de deploy (dev → prod)

Este bundle usa o padrão **"deploy de código"**: o pipeline de treino é promovido entre ambientes e o **modelo é retreinado no ambiente de destino** com os dados de lá. Para adicionar um target `prod`, duplique o bloco `dev` em `databricks.yml` com o catálogo/schema de produção.

---

## 📁 Estrutura

```
.
├── databricks.yml              # bundle: variáveis (catalog, schema) + target dev
├── resources/
│   └── nbo_pipeline.job.yml     # job que orquestra o pipeline ML
├── nbo/
│   ├── 01..09 *.py              # os notebooks do pipeline
│   ├── app/                     # Databricks App (motor de oferta)
│   └── postman_nbo_serving.json # collection Postman
└── README.md
```
