"""Motor de elegibilidade NBO — mesma lógica do pipeline (06_motor_oferta).
Lista de decisão: primeira regra (por prioridade) cujas condições casam vence."""
import json
import pandas as pd

CAMPOS = ["segmento", "score", "freq_60d", "recencia_hoje", "freq_total", "vibes_acum"]
OPS = ["=", "!=", ">=", "<=", ">", "<"]


def _val(row, campo):
    return row["score_max"] if campo == "score" else row[campo]


def _testa(row, cond):
    v = _val(row, cond["campo"]); alvo = cond["valor"]; op = cond["op"]
    try:
        if op == "=":  return v == alvo
        if op == "!=": return v != alvo
        if op == ">=": return v >= alvo
        if op == "<=": return v <= alvo
        if op == ">":  return v > alvo
        if op == "<":  return v < alvo
    except TypeError:
        return False
    return False


def _conds(r):
    c = r["condicoes"]
    return c if isinstance(c, list) else json.loads(c)


def aplica(rowdict, regras):
    """regras: lista de dicts (só ativas), ordenada por prioridade."""
    for r in regras:
        if all(_testa(rowdict, c) for c in _conds(r)):
            return r["nome"], r["acao_tipo"], int(r["acao_nivel"])
    return "sem_regra", "sem_oferta", 1


def simular(base_clientes, best_map, p_map, custo_base, budget, regras):
    """Avalia as regras sobre a base e aplica o budget por balde.
    Retorna DataFrame com a decisão por cliente."""
    linhas = []
    for row in base_clientes.itertuples():
        d = row._asdict()
        nome, acao, nivel = aplica(d, regras)
        if acao == "sem_oferta":
            tipo, p = "sem_oferta", 0.0
        elif acao == "melhor_ml":
            tipo, p = best_map.get((row.customer_id, nivel), ("sem_oferta", 0.0))
        else:
            tipo, p = acao, p_map.get((row.customer_id, acao, nivel), 0.0)
        custo = int(custo_base.get(tipo, 0) * nivel) if tipo != "sem_oferta" else 0
        linhas.append((row.customer_id, row.segmento, nome, tipo, nivel, float(p), custo))

    dec = pd.DataFrame(linhas, columns=["customer_id", "segmento", "regra_aplicada",
                                        "tipo_oferta", "nivel_agressividade", "p_aceitar", "custo_vibes"])
    dec = dec.sort_values(["segmento", "p_aceitar"], ascending=[True, False])
    dec["custo_acum"] = dec.groupby("segmento")["custo_vibes"].cumsum()
    dec["budget_seg"] = dec["segmento"].map(budget).fillna(0)
    dec["elegivel"] = (dec.tipo_oferta != "sem_oferta") & (dec.custo_acum <= dec.budget_seg)
    dec["oferta_final"] = dec.apply(lambda r: r.tipo_oferta if r.elegivel else "sem_oferta", axis=1)
    return dec


def resumo(dec):
    """Métricas agregadas de um cenário."""
    atend = dec[dec.oferta_final != "sem_oferta"]
    return {
        "clientes": int(len(dec)),
        "atendidos": int(len(atend)),
        "cobertura_pct": round(100 * len(atend) / max(len(dec), 1), 1),
        "vibes_gastos": int(atend.custo_vibes.sum()),
        "p_aceitar_medio": round(float(atend.p_aceitar.mean()) if len(atend) else 0.0, 3),
        "aceites_esperados": int(round(atend.p_aceitar.sum())),
    }
