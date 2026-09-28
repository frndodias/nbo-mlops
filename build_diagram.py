import sys
SP = "/Users/fernando.custodio/.vibe/marketplace/plugins/fe-specialized-agents/skills/drawio-diagram"
sys.path.insert(0, SP + "/scripts")
from generate_drawio import DrawioBuilder, load_icons

icons = load_icons()
b = DrawioBuilder(width=1560, height=880, icons_cache=icons)

b.add_title(40, 8, 470, "Motor de Oferta NBO — Vibe",
            "Feature Store compartilhada · Batch + RT")

# ---- group labels (columns) ----
b.add_group_label(40, 250, 180, "FEATURE STORE")
b.add_group_label(280, 118, 300, "CONSTRUÇÃO DO MODELO")
b.add_group_label(560, 250, 190, "REGISTRO & SERVING")
b.add_group_label(560, 62, 470, "PLANO DE CONTROLE (Growth via Databricks App)")
b.add_group_label(830, 355, 200, "MOTOR DE OFERTA (BATCH)")
b.add_group_label(1110, 300, 180, "SAÍDA & CONSUMO")

# ---- Control plane ----
App    = b.add_node(560, 88, 180, 60, "Databricks App\n(Growth: parâmetros)", "indigo")
Ofertas= b.add_node(800, 88, 135, 60, "Ofertas\n(catálogo)", "yellow", shape="cylinder")
Regras = b.add_node(965, 88, 150, 60, "Regras Negócio\n(agressiv. / budget)", "gov", shape="cylinder")

# ---- Feature Store column ----
FS       = b.add_node(50, 290, 160, 78, "Feature Store\n(offline · Delta / UC)", "orange", shape="cylinder")
Lakebase = b.add_node(50, 405, 160, 78, "Lakebase\n(online · sync)", "teal", shape="cylinder")
Input    = b.add_node(55, 560, 150, 78, "Input\n(Gold histórico)", "gold", shape="cylinder")

# ---- Construção do modelo ----
AutoML = b.add_node(300, 150, 120, 55, "AutoML", "blue")
Manual = b.add_node(440, 150, 120, 55, "Manual\n(notebook)", "blue")
Modelo = b.add_node(330, 300, 175, 80, "Modelo:\nScore Oferta", "orange")

# ---- Registro & Serving ----
Registry = b.add_node(560, 290, 180, 78, "Model Registry\nchampion / challenger", "purple")
Serving  = b.add_node(560, 470, 180, 78, "Serving Endpoint (RT)\nP(aceitar) via API", "teal")

# ---- Motor batch ----
Pipeline = b.add_node(840, 430, 180, 95, "Pipeline (batch)\nscore + regras + budget", "green")

# ---- Saída ----
Gold    = b.add_node(1120, 330, 155, 78, "Gold: Final\n(cliente → oferta)", "gold", shape="cylinder")
Consumo = b.add_node(1120, 490, 165, 82, "Consumo\nMktg (batch)\nApp / WhatsApp (RT)", "green")

# ================= EDGES =================
# Construção
b.add_edge(AutoML, Modelo, "", exit_x=0.5, exit_y=1, entry_x=0.35, entry_y=0)
b.add_edge(Manual, Modelo, "", exit_x=0.5, exit_y=1, entry_x=0.75, entry_y=0)
b.add_edge(FS, Modelo, "FeatureLookup\n(point-in-time)", exit_x=1, exit_y=0.4, entry_x=0, entry_y=0.5)
b.add_edge(Input, Modelo, "treino", exit_x=1, exit_y=0.2, entry_x=0.2, entry_y=1)
b.add_edge(Modelo, Registry, "registra", exit_x=1, exit_y=0.5, entry_x=0, entry_y=0.5, stroke_width=2.5, font_style=1)

# Online / RT
b.add_edge(FS, Lakebase, "sync online", exit_x=0.5, exit_y=1, entry_x=0.5, entry_y=0, dashed=True)
b.add_edge(Lakebase, Serving, "feature lookup (ms)", exit_x=1, exit_y=0.5, entry_x=0, entry_y=0.6)
b.add_edge(Registry, Serving, "champion (RT)", exit_x=0.5, exit_y=1, entry_x=0.5, entry_y=0, stroke_width=2.5, font_style=1)
b.add_edge(Serving, Consumo, "oferta em tempo real", exit_x=1, exit_y=0.85, entry_x=0, entry_y=0.85, stroke_color="#00695C", stroke_width=2.5, font_style=1)

# Batch
b.add_edge(Registry, Pipeline, "champion (batch)", exit_x=1, exit_y=0.6, entry_x=0, entry_y=0.25)
b.add_edge(FS, Pipeline, "FeatureLookup (batch)", exit_x=1, exit_y=0.75, entry_x=0, entry_y=0.6)
b.add_edge(Input, Pipeline, "base a pontuar", exit_x=1, exit_y=0.6, entry_x=0, entry_y=0.9)
b.add_edge(Ofertas, Pipeline, "catálogo", exit_x=0.5, exit_y=1, entry_x=0.45, entry_y=0)
b.add_edge(Regras, Pipeline, "config", exit_x=0.5, exit_y=1, entry_x=0.75, entry_y=0)
b.add_edge(Pipeline, Gold, "grava", exit_x=1, exit_y=0.4, entry_x=0, entry_y=0.5, stroke_width=2.5, font_style=1)
b.add_edge(Gold, Consumo, "", exit_x=0.5, exit_y=1, entry_x=0.5, entry_y=0)

# Control plane writes
b.add_edge(App, Ofertas, "", exit_x=1, exit_y=0.4, entry_x=0, entry_y=0.5)
b.add_edge(App, Regras, "escreve config", exit_x=1, exit_y=0.85, entry_x=0, entry_y=0.85)
b.add_edge(FS, App, "lê features\n(segmentos)", exit_x=0.7, exit_y=0, entry_x=0.1, entry_y=1, dashed=True)

# ---- Notes ----
b.add_note(1300, 70, 250, 150,
           "ANTI-SKEW\n• Features: treino, batch e RT puxam do mesmo lugar (Feature Store).\n• Lógica: regras só no batch (RT serve o modelo puro) — nada a divergir.")
b.add_note(760, 590, 300, 110,
           "BUDGET = estado → só no batch.\nO Pipeline aloca/decrementa os baldes por segmento sobre a base inteira.")

b.print_validation()
b.save("docs/diagrams/motor_oferta_nbo.drawio")
print("SAVED")
