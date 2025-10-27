import os
import math
import pandas as pd
from flask import Flask, jsonify, request, render_template_string
from functools import lru_cache

# === Ruta del Excel (en la raíz del repo) ===
EXCEL_PATH = os.environ.get("EXCEL_PATH", "Monitoreo_de_candidatos_largo.xlsx")

# === Nombres de columnas (según tu archivo) ===
COL_ESPECTRO   = "Espectro"
COL_CANDIDATO  = "Candidato"
COL_RED        = "Red Social"
COL_LIKES      = "Promedio likes x semana"
COL_MAXLIKES   = "Publicación con más likes"
COL_TEMA       = "Tema"
COL_COMENT     = "Promedio comentarios  por publicación"

# ---------- CARGA Y LIMPIEZA (cache para velocidad) ----------
@lru_cache(maxsize=1)
def _cache_key():
    # si cambias EXCEL_PATH en variables de entorno y redeploy, se renueva
    return os.path.abspath(EXCEL_PATH)

@lru_cache(maxsize=1)
def _load_all_cached(_key):
    xls = pd.ExcelFile(EXCEL_PATH)
    frames = []
    for sh in xls.sheet_names:
        df = pd.read_excel(EXCEL_PATH, sheet_name=sh)
        if df.empty or df.dropna(how="all").empty:
            continue
        df["Semana"] = sh
        frames.append(df)

    if not frames:
        return pd.DataFrame(columns=[
            COL_ESPECTRO, COL_CANDIDATO, COL_RED, COL_LIKES,
            COL_MAXLIKES, COL_TEMA, COL_COMENT, "Semana"
        ])

    df = pd.concat(frames, ignore_index=True)

    # Tipos
    for c in [COL_LIKES, COL_MAXLIKES, COL_COMENT]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    for c in [COL_ESPECTRO, COL_CANDIDATO, COL_RED, COL_TEMA, "Semana"]:
        if c in df.columns:
            df[c] = df[c].astype(str).str.strip()

    # Interacciones = likes + comentarios (NaN se vuelve 0 SOLO para esta suma)
    df["Interacciones"] = df[COL_LIKES].fillna(0) + df[COL_COMENT].fillna(0)
    return df

def load_all():
    return _load_all_cached(_cache_key())

def aplicar_filtros(df):
    red = (request.args.get("red") or "").strip()
    semana = (request.args.get("semana") or "").strip()
    espectro = (request.args.get("espectro") or "").strip()
    if red:
        df = df[df[COL_RED] == red]
    if semana:
        df = df[df["Semana"] == semana]
    if espectro:
        df = df[df[COL_ESPECTRO] == espectro]
    return df

# ---------- APP ----------
app = Flask(__name__)

@app.route("/")
def index():
    df = load_all()
    total_filas   = len(df)
    redes         = sorted(df[COL_RED].dropna().unique().tolist()) if not df.empty else []
    semanas       = sorted(df["Semana"].dropna().unique().tolist()) if not df.empty else []
    espectros     = sorted(df[COL_ESPECTRO].dropna().unique().tolist()) if not df.empty else []
    total_likes   = int(df[COL_LIKES].fillna(0).sum()) if COL_LIKES in df else 0
    total_coment  = int(df[COL_COMENT].fillna(0).sum()) if COL_COMENT in df else 0
    n_candidatos  = df[COL_CANDIDATO].nunique() if not df.empty else 0

    # Paleta por espectro (colores suaves)
    espectro_colors = {
        "Centro":    "rgba(16,185,129,0.35)",   # verde claro
        "Derecha":   "rgba(59,130,246,0.35)",   # azul claro
        "Izquierda": "rgba(245,158,11,0.35)",   # naranja claro
    }

    html = f"""
<!doctype html>
<html lang="es">
<head>
  <meta charset="utf-8" />
  <title>Dashboard de Candidatos</title>
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
  <style>
    body {{ font-family: system-ui,-apple-system,Segoe UI,Roboto; background:#f6f8fa; margin:0; padding:24px; }}
    h1 {{ margin:0 0 8px; }}
    .sub {{ color:#6b7280; margin-bottom:20px; }}
    .cards {{ display:grid; grid-template-columns: repeat(4, 1fr); gap:16px; margin: 16px 0 28px; }}
    .card {{ background: #fff; border-radius:12px; padding:18px; box-shadow:0 6px 16px rgba(0,0,0,.06); }}
    .kpi {{ font-size:13px; color:#6b7280; text-transform:uppercase; letter-spacing:.5px; }}
    .val {{ font-size:32px; font-weight:800; margin-top:6px; }}
    .grid3 {{ display:grid; grid-template-columns: repeat(3, 1fr); gap:20px; }}
    .grid2 {{ display:grid; grid-template-columns: repeat(2, 1fr); gap:20px; }}
    .panel {{ background:#fff; border-radius:12px; padding:16px; box-shadow:0 6px 16px rgba(0,0,0,.06); overflow: visible; }}
    .filters {{ display:flex; gap:12px; align-items:center; margin: 8px 0 16px; flex-wrap: wrap; }}
    select, button {{ padding:8px 10px; border-radius:8px; border:1px solid #e5e7eb; background:#fff; }}
    table {{ width:100%; border-collapse:collapse; }}
    th, td {{ padding:8px 10px; border-bottom:1px solid #e5e7eb; text-align:left; }}
    .cell {{ text-align:center; }}
    /* asegura que el canvas pueda crecer en alto */
    .panel canvas {{ width: 100% !important; }}
    #heatmap {{ overflow:auto; }}
    #heatmap table {{ table-layout: fixed; }}
    #heatmap th, #heatmap td {{ white-space: nowrap; }}
    @media (max-width: 1200px) {{
      .grid3 {{ grid-template-columns: 1fr; }}
      .grid2 {{ grid-template-columns: 1fr; }}
      .cards {{ grid-template-columns: 1fr 1fr; }}
    }}
  </style>
</head>
<body>
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
      <select id="selRed"><option value="">(todas)</option>{"".join(f'<option value="{{r}}">{{r}}</option>' for r in redes)}</select>
      <label>Semana:</label>
      <select id="selSemana"><option value="">(todas)</option>{"".join(f'<option value="{{s}}">{{s}}</option>' for s in semanas)}</select>
      <label>Espectro:</label>
      <select id="selEspectro"><option value="">(todos)</option>{"".join(f'<option value="{{e}}">{{e}}</option>' for e in espectros)}</select>
      <button onclick="aplicar()">Aplicar</button>
      <button onclick="limpiar()">Limpiar</button>
    </div>

    <div class="grid3">
      <div class="panel">
        <h3>Likes promedio por candidato (según filtros)</h3>
        <canvas id="likesPorCandidato"></canvas>
      </div>
      <div class="panel">
        <h3>Comentarios promedio por candidato (según filtros)</h3>
        <canvas id="comentPorCandidato"></canvas>
      </div>
      <div class="panel">
        <h3>Candidatos por likes (según filtros) — todos</h3>
        <canvas id="candidatosTodos"></canvas>
      </div>
    </div>
  </div>

  <div class="panel" style="margin-top:20px">
    <h3>Ganador(a) por semana y espectro (más interacciones: likes + comentarios)</h3>
    <div id="tablaGanadores"></div>
  </div>

  <div class="panel" style="margin-top:20px">
    <h3>Heatmap de interacciones (candidato × red)</h3>
    <div id="heatmap"></div>
  </div>

<script>
  // --- colores por espectro desde el backend ---
  const ESPECTRO_COLORS = {{"Centro":"{espectro_colors.get('Centro','rgba(16,185,129,0.35)')}",
                            "Derecha":"{espectro_colors.get('Derecha','rgba(59,130,246,0.35)')}",
                            "Izquierda":"{espectro_colors.get('Izquierda','rgba(245,158,11,0.35)')}" }};
  // paleta para "general": cada candidato color distinto (rotamos)
  const PALETTE = [
    "rgba(99,102,241,0.35)","rgba(236,72,153,0.35)","rgba(34,197,94,0.35)","rgba(59,130,246,0.35)",
    "rgba(234,179,8,0.35)","rgba(244,114,182,0.35)","rgba(16,185,129,0.35)","rgba(251,113,133,0.35)",
    "rgba(96,165,250,0.35)","rgba(250,204,21,0.35)","rgba(147,197,253,0.35)","rgba(253,186,116,0.35)"
  ];

  function qs(p){{ const u=new URL(window.location.href); return u.searchParams.get(p)||""; }}
  function setSel(id,val){{ const el=document.getElementById(id); if(el) el.value=val||""; }}
  function aplicar(){{
    const u=new URL(window.location.href);
    u.searchParams.set('red', document.getElementById('selRed').value);
    u.searchParams.set('semana', document.getElementById('selSemana').value);
    u.searchParams.set('espectro', document.getElementById('selEspectro').value);
    window.location.href = u.toString();
  }}
  function limpiar(){{
    const u=new URL(window.location.href);
    ['red','semana','espectro'].forEach(p=>u.searchParams.delete(p));
    window.location.href=u.toString();
  }}
  setSel('selRed', qs('red')); setSel('selSemana', qs('semana')); setSel('selEspectro', qs('espectro'));

  // --- altura dinámica para que quepan TODOS los candidatos en móvil/desktop ---
  function setDynamicHeight(canvasId, count, rowHeight = 28, padding = 140, minRows = 6) {{
    const c = document.getElementById(canvasId);
    const rows = Math.max(count, minRows);
    c.style.height = (rows * rowHeight + padding) + 'px';
  }}

  // helper colores
  function colorsBySpectro(arr, espectros) {{
    return arr.map((_,i)=> ESPECTRO_COLORS[espectros[i]] || "rgba(107,114,128,0.25)");
  }}
  function colorsByCandidate(n) {{ return Array.from({{length:n}}, (_,i)=> PALETTE[i % PALETTE.length]); }}

  async function draw(){{
    const params = new URLSearchParams();
    if(qs('red')) params.set('red', qs('red'));
    if(qs('semana')) params.set('semana', qs('semana'));
    if(qs('espectro')) params.set('espectro', qs('espectro'));

    const likesCand = await fetch('/api/likes-por-candidato?'+params.toString()).then(r=>r.json());
    const comCand   = await fetch('/api/comentarios-por-candidato?'+params.toString()).then(r=>r.json());
    const todos     = await fetch('/api/candidatos-todos?'+params.toString()).then(r=>r.json());
    const winners   = await fetch('/api/ganador-semanal?'+params.toString()).then(r=>r.json());
    const matrix    = await fetch('/api/heatmap?'+params.toString()).then(r=>r.json());

    // ajustar altura antes de dibujar
    setDynamicHeight('likesPorCandidato', likesCand.length);
    setDynamicHeight('comentPorCandidato', comCand.length);
    setDynamicHeight('candidatosTodos',   todos.length);

    new Chart(document.getElementById('likesPorCandidato').getContext('2d'), {{
      type: 'bar',
      data: {{
        labels: likesCand.map(d=>d.candidato),
        datasets: [{{
          label: 'Likes promedio',
          data: likesCand.map(d=>d.likes),
          backgroundColor: qs('espectro') ? colorsBySpectro(likesCand, likesCand.map(d=>d.espectro))
                                           : colorsByCandidate(likesCand.length)
        }}]
      }},
      options: {{
        indexAxis:'y',
        responsive:true,
        maintainAspectRatio:false,
        animation:false,
        plugins: {{ legend: {{ display:false }} }},
        scales: {{ y: {{ ticks: {{ autoSkip:false }} }}, x: {{ ticks: {{ maxTicksLimit: 8 }} }} }}
      }}
    }});

    new Chart(document.getElementById('comentPorCandidato').getContext('2d'), {{
      type: 'bar',
      data: {{
        labels: comCand.map(d=>d.candidato),
        datasets: [{{
          label: 'Comentarios promedio',
          data: comCand.map(d=>d.comentarios),
          backgroundColor: qs('espectro') ? colorsBySpectro(comCand, comCand.map(d=>d.espectro))
                                           : colorsByCandidate(comCand.length)
        }}]
      }},
      options: {{
        indexAxis:'y',
        responsive:true,
        maintainAspectRatio:false,
        animation:false,
        plugins: {{ legend: {{ display:false }} }},
        scales: {{ y: {{ ticks: {{ autoSkip:false }} }}, x: {{ ticks: {{ maxTicksLimit: 8 }} }} }}
      }}
    }});

    new Chart(document.getElementById('candidatosTodos').getContext('2d'), {{
      type: 'bar',
      data: {{
        labels: todos.map(d=>d.candidato),
        datasets: [{{
          label: 'Likes promedio',
          data: todos.map(d=>d.likes),
          backgroundColor: qs('espectro') ? colorsBySpectro(todos, todos.map(d=>d.espectro))
                                           : colorsByCandidate(todos.length)
        }}]
      }},
      options: {{
        indexAxis:'y',
        responsive:true,
        maintainAspectRatio:false,
        animation:false,
        plugins: {{ legend: {{ display:false }} }},
        scales: {{ y: {{ ticks: {{ autoSkip:false }} }}, x: {{ ticks: {{ maxTicksLimit: 8 }} }} }}
      }}
    }});

    // tabla de ganadores
    const cont = document.getElementById('tablaGanadores');
    if(!winners.length) {{
      cont.innerHTML = '<em>Sin datos para los filtros seleccionados.</em>';
    }} else {{
      let html = '<table><thead><tr><th>Semana</th><th>Espectro</th><th>Candidato</th><th>Interacciones</th></tr></thead><tbody>';
      for (const w of winners) {{
        html += `<tr><td>${{w.semana}}</td><td>${{w.espectro}}</td><td>${{w.candidato}}</td><td class="cell">${{Intl.NumberFormat('es-ES').format(w.interacciones)}}</td></tr>`;
      }}
      html += '</tbody></table>';
      cont.innerHTML = html;
    }}

    // Heatmap simple con tabla coloreada
    const hm = document.getElementById('heatmap');
    if(!matrix.values.length) {{
      hm.innerHTML = '<em>Sin datos para los filtros seleccionados.</em>';
    }} else {{
      const rows = matrix.rows, cols = matrix.cols, vals = matrix.values;
      const max = Math.max(...vals.map(v=>v.valor||0));
      let html = '<table><thead><tr><th></th>';
      for (const col of cols) html += `<th>${{col}}</th>`;
      html += '</tr></thead><tbody>';
      for (const r of rows) {{
        html += `<tr><th>${{r}}</th>`;
        for (const c of cols) {{
          const item = vals.find(v => v.candidato===r && v.red===c);
          const v = item ? (item.valor||0) : 0;
          const pct = max? (v/max) : 0;
          const bg = `rgba(59,130,246,${{0.1 + 0.6*pct}})`; // azul con intensidad
          const disp = item && item.nd ? 'ND' : (v ? Intl.NumberFormat('es-ES').format(Math.round(v)) : '');
          html += `<td class="cell" style="background:${{bg}}">${{disp}}</td>`;
        }}
        html += '</tr>';
      }}
      html += '</tbody></table>';
      hm.innerHTML = html;
    }}
  }}
  draw();
</script>
</body>
</html>
"""
    return render_template_string(html)

# -------- APIs (incluyen espectro para colorear) --------
@app.route("/api/likes-por-candidato")
def api_likes_por_candidato():
    df = aplicar_filtros(load_all())
    if df.empty: return jsonify([])
    g = (df.groupby([COL_CANDIDATO, COL_ESPECTRO], as_index=False)[COL_LIKES]
           .mean()
           .rename(columns={COL_LIKES:"likes"}))
    g = g.sort_values("likes", ascending=False)
    out = [{"candidato": r[COL_CANDIDATO], "espectro": r[COL_ESPECTRO],
            "likes": float(0 if pd.isna(r["likes"]) else r["likes"])}
           for _, r in g.iterrows()]
    return jsonify(out)

@app.route("/api/comentarios-por-candidato")
def api_comentarios_por_candidato():
    df = aplicar_filtros(load_all())
    if df.empty: return jsonify([])
    g = (df.groupby([COL_CANDIDATO, COL_ESPECTRO], as_index=False)[COL_COMENT]
           .mean()
           .rename(columns={COL_COMENT:"comentarios"}))
    g = g.sort_values("comentarios", ascending=False)
    out = [{"candidato": r[COL_CANDIDATO], "espectro": r[COL_ESPECTRO],
            "comentarios": float(0 if pd.isna(r["comentarios"]) else r["comentarios"])}
           for _, r in g.iterrows()]
    return jsonify(out)

@app.route("/api/candidatos-todos")
def api_candidatos_todos():
    df = aplicar_filtros(load_all())
    if df.empty: return jsonify([])
    g = (df.groupby([COL_CANDIDATO, COL_ESPECTRO], as_index=False)[COL_LIKES]
           .mean()
           .rename(columns={COL_LIKES:"likes"}))
    g = g.sort_values("likes", ascending=False)
    out = [{"candidato": r[COL_CANDIDATO], "espectro": r[COL_ESPECTRO],
            "likes": float(0 if pd.isna(r["likes"]) else r["likes"])}
           for _, r in g.iterrows()]
    return jsonify(out)

@app.route("/api/ganador-semanal")
def api_ganador_semanal():
    df = aplicar_filtros(load_all())
    if df.empty: return jsonify([])
    g = df.groupby(["Semana", COL_ESPECTRO, COL_CANDIDATO], as_index=False)["Interacciones"].mean()
    idx = g.groupby(["Semana", COL_ESPECTRO])["Interacciones"].idxmax()
    gan = g.loc[idx].sort_values(["Semana", COL_ESPECTRO])
    out = [{"semana": r["Semana"], "espectro": r[COL_ESPECTRO], "candidato": r[COL_CANDIDATO],
            "interacciones": float(0 if pd.isna(r["Interacciones"]) else r["Interacciones"])}
           for _, r in gan.iterrows()]
    return jsonify(out)

# Heatmap candidato × red con Interacciones promedio
@app.route("/api/heatmap")
def api_heatmap():
    df = aplicar_filtros(load_all())
    if df.empty:
        return jsonify({"rows": [], "cols": [], "values": []})
    rows = sorted(df[COL_CANDIDATO].unique().tolist())
    cols = sorted(df[COL_RED].unique().tolist())
    g = df.groupby([COL_CANDIDATO, COL_RED], as_index=False)["Interacciones"].mean()
    values = []
    for r in rows:
        for c in cols:
            sub = g[(g[COL_CANDIDATO]==r) & (g[COL_RED]==c)]
            if sub.empty:
                values.append({"candidato": r, "red": c, "valor": 0, "nd": True})
            else:
                v = float(sub["Interacciones"].iloc[0]) if not pd.isna(sub["Interacciones"].iloc[0]) else 0
                values.append({"candidato": r, "red": c, "valor": v, "nd": False})
    return jsonify({"rows": rows, "cols": cols, "values": values})

# ---- Run local (no usado en Render) ----
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
