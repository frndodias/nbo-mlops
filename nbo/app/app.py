import time
import pandas as pd
import streamlit as st

import db
import motor

st.set_page_config(page_title="Vibe · Motor de Ofertas", page_icon="🎯", layout="wide")

# ---------------- Branding Vertem/Vibe ----------------
VERDE, LARANJA, AZUL, ESCURO = "#2D8659", "#E67E3C", "#4A90E2", "#22303C"
st.markdown(f"""
<style>
  .stApp {{ background: #F7F9FB; }}
  h1, h2, h3 {{ color: {ESCURO}; }}
  .vibe-hero {{ background: linear-gradient(100deg, {VERDE} 0%, {AZUL} 60%, {LARANJA} 120%);
     padding: 20px 26px; border-radius: 14px; color: white; margin-bottom: 8px; }}
  .vibe-hero h1 {{ color: white; margin: 0; font-size: 26px; }}
  .vibe-hero p {{ color: #EAF7F0; margin: 4px 0 0 0; font-size: 15px; }}
  div[data-testid="stMetric"] {{ background: white; border: 1px solid #E3E8EE;
     border-radius: 12px; padding: 12px 16px; }}
  .stTabs [data-baseweb="tab-list"] {{ gap: 6px; }}
  .stTabs [data-baseweb="tab"] {{ background:#EEF2F6; border-radius:8px 8px 0 0; padding:8px 16px; }}
  .stTabs [aria-selected="true"] {{ background:{VERDE}; color:white; }}
</style>
<div class="vibe-hero">
  <h1>🎯 Vibe · Motor de Ofertas (NBO)</h1>
  <p>Engajamento que gera resultados — regras de negócio + inteligência do modelo, sob seu controle.</p>
</div>
""", unsafe_allow_html=True)

TIPO_LABEL = {"cashback": "💰 Cashback", "bonus_vibes": "✨ Bônus de Vibes",
              "desconto_parceiro": "🤝 Desconto Parceiro", "missao": "🎮 Missão",
              "melhor_ml": "🧠 Melhor (ML decide)", "sem_oferta": "🚫 Sem oferta"}
COR_SEG = {"em_risco": LARANJA, "novo": AZUL, "ativo": VERDE, "campeao": "#8E6FB0"}


@st.cache_data(ttl=300)
def _base():
    cli, scored = db.carregar_base()
    ativos = set()  # preenchido depois com catálogo
    return cli, scored

def build_maps(scored, ativos):
    p_map = {(r.customer_id, r.tipo_oferta, r.nivel_agressividade): r.p_aceitar for r in scored.itertuples()}
    sc = scored[scored.tipo_oferta.isin(ativos)]
    best = (sc.sort_values("p_aceitar", ascending=False)
            .groupby(["customer_id", "nivel_agressividade"]).first().reset_index())
    best_map = {(r.customer_id, r.nivel_agressividade): (r.tipo_oferta, r.p_aceitar) for r in best.itertuples()}
    return p_map, best_map


# carrega estado
if "regras" not in st.session_state:
    regras, orc, cat = db.ler_regras()
    st.session_state.regras = regras
    st.session_state.orc = orc
    st.session_state.cat = cat

tab_regras, tab_arvore, tab_impacto, tab_sim = st.tabs(
    ["⚙️ Regras", "🌳 Árvore de Decisão", "📊 Análise de Impacto", "⚡ Simulador (tempo real)"])

# ============================================================ REGRAS
with tab_regras:
    st.subheader("Motor de elegibilidade — lista de decisão")
    st.caption("Avaliada por prioridade: a **primeira regra que casa** define a oferta. "
               "Condições usam segmento, `score` (P(aceitar) do modelo) e features do cliente.")

    regras_df = pd.DataFrame([{
        "prioridade": r["prioridade"], "nome": r["nome"], "ativo": r["ativo"],
        "condicoes": (r["condicoes"] if isinstance(r["condicoes"], str)
                      else __import__("json").dumps(r["condicoes"], ensure_ascii=False)),
        "acao_tipo": r["acao_tipo"], "acao_nivel": r["acao_nivel"],
    } for r in st.session_state.regras])

    edited = st.data_editor(
        regras_df, num_rows="dynamic", use_container_width=True, key="ed_regras",
        column_config={
            "prioridade": st.column_config.NumberColumn("Prioridade", min_value=1, step=1),
            "nome": st.column_config.TextColumn("Nome da regra", width="large"),
            "ativo": st.column_config.CheckboxColumn("Ativa"),
            "condicoes": st.column_config.TextColumn("Condições (JSON)", width="large",
                help='ex: [{"campo":"segmento","op":"=","valor":"em_risco"},{"campo":"score","op":">=","valor":0.5}]'),
            "acao_tipo": st.column_config.SelectboxColumn("Ação", options=list(TIPO_LABEL.keys())),
            "acao_nivel": st.column_config.NumberColumn("Nível", min_value=1, max_value=3, step=1),
        })

    c1, c2 = st.columns(2)
    with c1:
        st.markdown("**Baldes de budget por segmento**")
        orc_df = st.data_editor(pd.DataFrame(st.session_state.orc), key="ed_orc",
                                use_container_width=True, hide_index=True)
    with c2:
        st.markdown("**Catálogo de ofertas**")
        cat_df = st.data_editor(pd.DataFrame(st.session_state.cat), key="ed_cat",
                                use_container_width=True, hide_index=True)

    if st.button("💾 Salvar no Lakebase", type="primary"):
        import json
        novas = []
        for _, row in edited.iterrows():
            try:
                conds = json.loads(row["condicoes"]) if row["condicoes"] else []
            except Exception:
                st.error(f"JSON inválido nas condições da regra '{row['nome']}'"); st.stop()
            novas.append({"prioridade": int(row["prioridade"]), "nome": row["nome"],
                          "ativo": bool(row["ativo"]), "condicoes": conds,
                          "acao_tipo": row["acao_tipo"], "acao_nivel": int(row["acao_nivel"])})
        db.salvar_regras(novas, orc_df.to_dict("records"), cat_df.to_dict("records"),
                         usuario="app-growth")
        st.session_state.regras = sorted(novas, key=lambda x: x["prioridade"])
        st.session_state.orc = orc_df.to_dict("records")
        st.session_state.cat = cat_df.to_dict("records")
        st.success("Regras salvas no Lakebase ✅ — o pipeline vai usar essa versão no próximo run.")

# ============================================================ ÁRVORE
with tab_arvore:
    st.subheader("Como a decisão é tomada")
    st.caption("Fluxo avaliado de cima pra baixo. Verde = ação de oferta, laranja = condição, vermelho = sem oferta.")
    regras_ord = sorted(st.session_state.regras, key=lambda x: x["prioridade"])
    dot = ["digraph G {", 'rankdir=TB; node [fontname="Helvetica", style="filled,rounded", shape=box];',
           'edge [fontname="Helvetica", fontsize=10];']
    dot.append(f'inicio [label="Cliente\\n(segmento + score + features)", fillcolor="#DCEBF9", color="{AZUL}"];')
    prev = "inicio"
    for i, r in enumerate([x for x in regras_ord if x["ativo"]]):
        cond_txt = " E ".join(f'{c["campo"]} {c["op"]} {c["valor"]}' for c in r["condicoes"]) or "qualquer cliente"
        rn = f"r{i}"; an = f"a{i}"
        dot.append(f'{rn} [label="{r["nome"]}\\n({cond_txt})", fillcolor="#FFF0E2", color="{LARANJA}"];')
        acao = TIPO_LABEL.get(r["acao_tipo"], r["acao_tipo"])
        cor = "#FDE0E0" if r["acao_tipo"] == "sem_oferta" else "#E2F3EA"
        borda = "#C0392B" if r["acao_tipo"] == "sem_oferta" else VERDE
        nivel = "" if r["acao_tipo"] == "sem_oferta" else f"\\nnível {r['acao_nivel']}"
        dot.append(f'{an} [label="{acao}{nivel}", fillcolor="{cor}", color="{borda}"];')
        dot.append(f'{prev} -> {rn} [label="{"não casou" if i else ""}"];')
        dot.append(f'{rn} -> {an} [label="casou", color="{VERDE}"];')
        prev = rn
    dot.append("}")
    st.graphviz_chart("\n".join(dot), use_container_width=True)

# ============================================================ IMPACTO
with tab_impacto:
    st.subheader("Análise de impacto — e se eu mudar uma regra?")
    cli, scored = db.carregar_base()
    ativos = {c["tipo_oferta"] for c in st.session_state.cat if c["ativo"]}
    custo_base = {c["tipo_oferta"]: c["custo_base_vibes"] for c in st.session_state.cat}
    budget = {o["segmento"]: o["budget_vibes"] for o in st.session_state.orc}
    p_map, best_map = build_maps(scored, ativos)

    base_ativas = [r for r in sorted(st.session_state.regras, key=lambda x: x["prioridade"]) if r["ativo"]]
    dec_atual = motor.simular(cli, best_map, p_map, custo_base, budget, base_ativas)
    r_atual = motor.resumo(dec_atual)

    st.markdown("**Cenário proposto** — edite abaixo e veja o impacto na hora:")
    colp = st.columns(4)
    seg_edit = colp[0].selectbox("Ajustar budget do segmento", list(budget.keys()))
    novo_budget = colp[1].number_input("Novo budget (Vibes)", value=int(budget[seg_edit]), step=10000)
    thr_shift = colp[2].slider("Deslocar thresholds de score (±)", -0.2, 0.2, 0.0, 0.05,
                               help="Soma esse valor em toda condição de 'score' das regras")
    colp[3].write(""); colp[3].write("")

    budget_prop = dict(budget); budget_prop[seg_edit] = novo_budget
    regras_prop = []
    for r in base_ativas:
        rc = {**r, "condicoes": [dict(c) for c in r["condicoes"]]}
        for c in rc["condicoes"]:
            if c["campo"] == "score":
                c["valor"] = round(min(max(c["valor"] + thr_shift, 0), 1), 3)
        regras_prop.append(rc)
    dec_prop = motor.simular(cli, best_map, p_map, custo_base, budget_prop, regras_prop)
    r_prop = motor.resumo(dec_prop)

    def _delta(a, b): return f"{b - a:+,}".replace(",", ".")
    m = st.columns(4)
    m[0].metric("Clientes atendidos", f"{r_prop['atendidos']:,}".replace(",", "."),
                _delta(r_atual["atendidos"], r_prop["atendidos"]))
    m[1].metric("Cobertura", f"{r_prop['cobertura_pct']}%",
                f"{r_prop['cobertura_pct'] - r_atual['cobertura_pct']:+.1f} p.p.")
    m[2].metric("Vibes gastos", f"{r_prop['vibes_gastos']:,}".replace(",", "."),
                _delta(r_atual["vibes_gastos"], r_prop["vibes_gastos"]))
    m[3].metric("Aceites esperados", f"{r_prop['aceites_esperados']:,}".replace(",", "."),
                _delta(r_atual["aceites_esperados"], r_prop["aceites_esperados"]))

    st.markdown("**Distribuição de ofertas — atual vs proposto**")
    def dist(dec, nome):
        d = dec.groupby("oferta_final").size().rename(nome)
        return d
    comp = pd.concat([dist(dec_atual, "Atual"), dist(dec_prop, "Proposto")], axis=1).fillna(0).astype(int)
    st.bar_chart(comp)

    with st.expander("Ver amostra da decisão proposta"):
        st.dataframe(dec_prop[["customer_id", "segmento", "regra_aplicada", "oferta_final",
                               "nivel_agressividade", "p_aceitar", "custo_vibes", "elegivel"]].head(200),
                     use_container_width=True, hide_index=True)

# ============================================================ SIMULADOR
with tab_sim:
    st.subheader("Simulador em tempo real")
    st.caption("Chama o endpoint de Model Serving. Manda só o `customer_id` + a oferta — "
               "as features vêm do Lakebase (online store) automaticamente.")
    cid = st.number_input("customer_id", value=1, min_value=1, step=1, help="1 = Maria (persona do workshop)")
    nivel = st.select_slider("Nível de agressividade", [1, 2, 3], value=3)
    if st.button("⚡ Consultar oferta", type="primary"):
        cands = [{"customer_id": int(cid), "tipo_oferta": t, "nivel_agressividade": int(nivel)}
                 for t in ["cashback", "bonus_vibes", "desconto_parceiro", "missao"]]
        t0 = time.time()
        try:
            preds = db.prever(cands)
            dt = (time.time() - t0) * 1000
            res = sorted(zip(cands, preds), key=lambda x: x[1], reverse=True)
            st.success(f"Melhor oferta: **{TIPO_LABEL[res[0][0]['tipo_oferta']]}** "
                       f"· P(aceitar) = {res[0][1]:.3f}  ·  {dt:.0f} ms")
            st.bar_chart(pd.DataFrame(
                {"P(aceitar)": [p for _, p in res]},
                index=[TIPO_LABEL[c["tipo_oferta"]] for c, _ in res]))
        except Exception as e:
            st.error(f"Erro ao consultar o endpoint: {e}")
