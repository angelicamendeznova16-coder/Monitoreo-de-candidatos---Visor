import os, math
import pandas as pd
from flask import Flask, jsonify, request, render_template_string

EXCEL_PATH = os.environ.get("EXCEL_PATH", os.path.join("data","Monitoreo_de_candidatos_largo.xlsx"))

COL_ESPECTRO  = "Espectro"
COL_CANDIDATO = "Candidato"
COL_RED       = "Red Social"
COL_LIKES     = "Promedio likes x semana"
COL_MAXLIKES  = "Publicación con más likes"
COL_TEMA      = "Tema"
COL_COMENT    = "Promedio comentarios  por publicación"

def load_all():
    xls = pd.ExcelFile(EXCEL_PATH)
    frames = []
    for sh in xls.sheet_names:
        df = pd.read_excel(EXCEL_PATH, sheet_name=sh)
        if df.empty or df.dropna(how="all").empty: continue
        df["Semana"] = sh
        frames.append(df)
    if not frames:
        return pd.DataFrame(columns=[COL_ESPECTRO,COL_CANDIDATO,COL_RED,COL_LIKES,COL_MAXLIKES,COL_TEMA,COL_COMENT,"Semana"])
    df = pd.concat(frames, ignore_index=True)
    for c in [COL_LIKES, COL_MAXLIKES, COL_COMENT]:
        if c in df: df[c] = pd.to_numeric(df[c], errors="coerce")
    for c in [COL_ESPECTRO, COL_CANDIDATO, COL_RED, COL_TEMA]:
        if c in df: df[c] = df[c].astype(str).str.strip()
    return df

app = Flask(__name__)

@app.route("/")
def index():
    df = load_all()
    total_filas = len(df)
    redes   = sorted(df[COL_RED].dropna().unique().tolist()) if not df.empty else []
    semanas = sorted(df["Semana"].dropna().unique().tolist()) if not df.empty else []
    total_likes  = int(df[COL_LIKES].fillna(0).sum()) if COL_LIKES in df else 0
    total_coment = int(df[COL_COMENT].fillna(0).sum()) if COL_COMENT in df else 0
    n_candidatos = df[COL_CANDIDATO].nunique() if not df.empty else 0

    html = f"""<!doctype html><html lang="es"><head>
<meta charset="utf-8"/><meta name="viewport" content="width=device-width, initial-scale=1"/>
<title>Dashboard de Candidatos</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
<style>
body{{font-family:system-ui,-apple-system,Segoe UI,Roboto;background:#f6f8fa;margin:0;padding:24px}}
h1{{margin:0 0 8px}}.sub{{color:#6b7280;margin-bottom:20px}}
.cards{{display:grid;grid-template-columns:repeat(4,1fr);gap:16px;margin:16px 0 28px}}
.card{{background:#fff;border-radius:12px;padding:18px;box-shadow:0 6px 16px rgba(0,0,0,.06)}}
.kpi{{font-size:13px;color:#6b7280;text-transform:uppercase;letter-spacing:.5px}}
.val{{font-size:32px;font-weight:800;margin-top:6px}}
.grid{{display:grid;grid-template-columns:repeat(3,1fr);gap:20px}}
.panel{{background:#fff;border-radius:12px;padding:16px;box-shadow:0 6px 16px rgba(0,0,0,.06)}}
.filters{{display:flex;gap:12px;align-items:center;margin:8px 0 16px;flex-wrap:wrap}}
select,button{{padding:8px 10px;border-radius:8px;border:1px solid #e5e7eb;background:#fff}}
@media(max-width:1100px){{.cards{{grid-template-columns:1fr 1fr}}.grid{{grid-template-columns:1fr}}}}
</style></head><body>
<h1>Dashboard de Candidatos por Red</h1>
<div class="sub">Fuente: Excel (todas las semanas). Archivo: {os.path.basename(EXCEL_PATH)}</div>
<div class="cards">
  <div class="card"><div class="kpi">Filas analizadas</div><div class="val">{total_filas:,}</div></div>
  <div class="card"><div class="kpi">Suma de likes promedio</div><div class="val">{total_likes:,}</div></div>
  <div class="card"><div class="kpi">Suma de comentarios promedio</div><div class="val">{total_coment:,}</div></div>
  <div class="card"><div class="kpi">Candidatos únicos</div><div class="val">{n_candidatos:,}</div></div>
</div>
<div class="panel">
  <div class="filters">
    <label>Red:</label>
    <select id="selRed"><option value="">(todas)</option>{"".join(f'<option value="{r}">{r}</option>' for r in redes)}</select>
    <label>Semana:</label>
    <select id="selSemana"><option value="">(todas)</option>{"".join(f'<option value="{s}">{s}</option>' for s in semanas)}</select>
    <button onclick="recargar()">Aplicar</button><button onclick="limpiar()">Limpiar</button>
  </div>
  <div class="grid">
    <div class="panel"><h3>Likes promedio por red</h3><canvas id="likesPorRed"></canvas></div>
    <div class="panel"><h3>Comentarios promedio por red</h3><canvas id="comentPorRed"></canvas></div>
    <div class="panel"><h3>Top 10 candidatos por likes (según filtros)</h3><canvas id="topCandidatos"></canvas></div>
  </div>
</div>
<script>
function qs(p){{const u=new URL(window.location.href);return u.searchParams.get(p)||""}}
function recargar(){{const r=document.getElementById('selRed').value;const s=document.getElementById('selSemana').value;
  const u=new URL(window.location.href);u.searchParams.set('red',r);u.searchParams.set('semana',s);window.location.href=u.toString()}}
function limpiar(){{const u=new URL(window.location.href);u.searchParams.delete('red');u.searchParams.delete('semana');window.location.href=u.toString()}}
document.getElementById('selRed').value=qs('red');document.getElementById('selSemana').value=qs('semana');
async function drawCharts(){{
  const p=new URLSearchParams(); if(qs('red')) p.set('red',qs('red')); if(qs('semana')) p.set('semana',qs('semana'));
  const likesRed=await fetch('/api/likes-por-red?'+p.toString()).then(r=>r.json());
  const comentRed=await fetch('/api/comentarios-por-red?'+p.toString()).then(r=>r.json());
  const topCand=await fetch('/api/top-candidatos?'+p.toString()).then(r=>r.json());
  new Chart(document.getElementById('likesPorRed'),{{type:'bar',data:{{labels:Object.keys(likesRed),datasets:[{{label:'Likes promedio',data:Object.values(likesRed)}}]}},options:{{plugins:{{legend:{{display:false}}}}}}}});
  new Chart(document.getElementById('comentPorRed'),{{type:'bar',data:{{labels:Object.keys(comentRed),datasets:[{{label:'Comentarios promedio',data:Object.values(comentRed)}}]}},options:{{plugins:{{legend:{{display:false}}}}}}}});
  new Chart(document.getElementById('topCandidatos'),{{type:'bar',data:{{labels:topCand.map(d=>d.candidato),datasets:[{{label:'Likes promedio',data:topCand.map(d=>d.likes)}}]}},options:{{indexAxis:'y',plugins:{{legend:{{display:false}}}}}}}});
}} drawCharts();
</script></body></html>"""
    return render_template_string(html)

def aplicar_filtros(df):
    red = request.args.get("red") or ""
    semana = request.args.get("semana") or ""
    if red: df = df[df[COL_RED] == red]
    if semana: df = df[df["Semana"] == semana]
    return df

@app.route("/api/likes-por-red")
def api_likes_por_red():
    df = aplicar_filtros(load_all())
    if df.empty: return jsonify({})
    s = df.groupby(COL_RED)[COL_LIKES].mean().round(2).sort_values(ascending=False)
    return jsonify({k: float(v) if not math.isnan(v) else 0.0 for k, v in s.items()})

@app.route("/api/comentarios-por-red")
def api_coment_por_red():
    df = aplicar_filtros(load_all())
    if df.empty: return jsonify({})
    s = df.groupby(COL_RED)[COL_COMENT].mean().round(2).sort_values(ascending=False)
    return jsonify({k: float(v) if not math.isnan(v) else 0.0 for k, v in s.items()})

@app.route("/api/top-candidatos")
def api_top_candidatos():
    df = aplicar_filtros(load_all())
    if df.empty: return jsonify([])
    g = df.groupby(COL_CANDIDATO)[COL_LIKES].mean().sort_values(ascending=False).head(10).reset_index()
    out = [{"candidato": r[COL_CANDIDATO], "likes": 0.0 if math.isnan(r[COL_LIKES]) else float(r[COL_LIKES])} for _, r in g.iterrows()]
    return jsonify(out)

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
