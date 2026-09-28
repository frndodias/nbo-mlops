"""Camada de dados do App: Lakebase (regras, R/W), SQL warehouse (base analítica), endpoint (RT)."""
import os, ssl, json
import pandas as pd
import streamlit as st
from databricks.sdk import WorkspaceClient

LB_HOST = os.getenv("LAKEBASE_HOST", "SEU-LAKEBASE-HOST.database.cloud.databricks.com")
LB_ENDPOINT = os.getenv("LAKEBASE_ENDPOINT", "projects/nbo-app/branches/production/endpoints/primary")
LB_DB = os.getenv("LAKEBASE_DB", "databricks_postgres")
WAREHOUSE_ID = os.getenv("DATABRICKS_WAREHOUSE_ID", "")  # defina o SEU warehouse
SERVING_ENDPOINT = os.getenv("SERVING_ENDPOINT", "nbo-score-oferta")
CATALOG = os.getenv("UC_CATALOG", "fernando_custodio")
SCHEMA = os.getenv("UC_SCHEMA", "nbo")

_w = WorkspaceClient()


# ---------------- Lakebase (pg8000, puro python) ----------------
def _pg_token():
    return _w.api_client.do("POST", "/api/2.0/postgres/credentials", body={"endpoint": LB_ENDPOINT})["token"]

def _pg_user():
    # dentro do app, o usuário Postgres é o client id do service principal
    return os.getenv("PGUSER") or os.getenv("DATABRICKS_CLIENT_ID") or _w.current_user.me().user_name

def pg_connect():
    import pg8000.dbapi
    return pg8000.dbapi.connect(host=LB_HOST, port=5432, database=LB_DB,
                                user=_pg_user(), password=_pg_token(),
                                ssl_context=ssl.create_default_context())

def ler_regras():
    conn = pg_connect(); cur = conn.cursor()
    cur.execute("SELECT id, prioridade, nome, ativo, condicoes, acao_tipo, acao_nivel "
                "FROM nbo.regras_elegibilidade ORDER BY prioridade")
    cols = ["id", "prioridade", "nome", "ativo", "condicoes", "acao_tipo", "acao_nivel"]
    regras = [dict(zip(cols, r)) for r in cur.fetchall()]
    for r in regras:
        if isinstance(r["condicoes"], str):
            r["condicoes"] = json.loads(r["condicoes"])
    cur.execute("SELECT segmento, prioridade, budget_vibes FROM nbo.orcamento_segmento ORDER BY prioridade")
    orc = [dict(zip(["segmento", "prioridade", "budget_vibes"], r)) for r in cur.fetchall()]
    cur.execute("SELECT tipo_oferta, ativo, custo_base_vibes FROM nbo.ofertas_catalogo ORDER BY tipo_oferta")
    cat = [dict(zip(["tipo_oferta", "ativo", "custo_base_vibes"], r)) for r in cur.fetchall()]
    cur.close(); conn.close()
    return regras, orc, cat

def salvar_regras(regras, orcamento, catalogo, usuario="app"):
    conn = pg_connect(); cur = conn.cursor()
    cur.execute("TRUNCATE nbo.regras_elegibilidade RESTART IDENTITY")
    for r in regras:
        cur.execute("INSERT INTO nbo.regras_elegibilidade (prioridade, nome, ativo, condicoes, acao_tipo, acao_nivel) "
                    "VALUES (%s,%s,%s,%s,%s,%s)",
                    (r["prioridade"], r["nome"], r["ativo"], json.dumps(r["condicoes"]), r["acao_tipo"], int(r["acao_nivel"])))
    for o in orcamento:
        cur.execute("UPDATE nbo.orcamento_segmento SET prioridade=%s, budget_vibes=%s WHERE segmento=%s",
                    (o["prioridade"], int(o["budget_vibes"]), o["segmento"]))
    for c in catalogo:
        cur.execute("UPDATE nbo.ofertas_catalogo SET ativo=%s, custo_base_vibes=%s WHERE tipo_oferta=%s",
                    (c["ativo"], int(c["custo_base_vibes"]), c["tipo_oferta"]))
    cur.execute("INSERT INTO nbo.regras_auditoria (usuario, acao, detalhe) VALUES (%s,%s,%s)",
                (usuario, "salvar_regras", json.dumps({"n_regras": len(regras)})))
    conn.commit(); cur.close(); conn.close()


# ---------------- Base analítica (Lakebase) ----------------
@st.cache_data(ttl=600, show_spinner="Carregando base de scores…")
def carregar_base():
    conn = pg_connect(); cur = conn.cursor()
    cur.execute("SELECT customer_id, segmento, score_max, freq_60d, recencia_hoje, freq_total, vibes_acum "
                "FROM nbo.base_clientes")
    cli = pd.DataFrame(cur.fetchall(), columns=["customer_id", "segmento", "score_max", "freq_60d",
                                                "recencia_hoje", "freq_total", "vibes_acum"])
    cur.execute("SELECT customer_id, tipo_oferta, nivel_agressividade, p_aceitar FROM nbo.base_scored")
    scored = pd.DataFrame(cur.fetchall(), columns=["customer_id", "tipo_oferta", "nivel_agressividade", "p_aceitar"])
    cur.close(); conn.close()
    return cli, scored


# ---------------- Serving endpoint (RT) ----------------
def prever(registros):
    resp = _w.serving_endpoints.query(name=SERVING_ENDPOINT, dataframe_records=registros)
    return resp.predictions
