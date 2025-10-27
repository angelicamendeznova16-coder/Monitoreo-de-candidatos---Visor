import os
import pandas as pd
from flask import Flask, jsonify, request, render_template_string
from functools import lru_cache

# === Ruta del Excel (ajústala si cambias nombre/carpeta) ===
EXCEL_PATH = os.environ.get("EXCEL_PATH", "Monitoreo_de_candidatos_largo.xlsx")

# === Columnas del Excel ===
COL_ESPECTRO   = "Espectro"
COL_CANDIDATO  = "Candidato"
COL_RED        = "Red Social"
COL_LIKES      = "Promedio likes x semana"
COL_MAXLIKES   = "Publicación con más likes"
COL_TEMA       = "Tema"
COL_COMENT     = "Promedio comentarios  por publicación"

# ---------- CARGA + LIMPIEZA (con cache) ----------
@lru_cache(maxsize=1)
def _cache_key():
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
        cols = [COL_ESPECTRO, COL_CANDIDATO, COL_RED, COL_LIKES, COL_MAXLIKES, COL_TEMA, COL_COMENT, "Semana"]
        return pd.DataFrame(columns=cols)

    df = pd.concat(frames, ignore_index=True)

    # Tipos
    for c in [COL_LIKES, COL_MAXLIKES, COL_COMENT]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    for c in [COL_ESPECTRO, COL_CANDIDATO, COL_RED, COL_TEMA, "Semana"]:
        if c in df.columns:
            df[c] = df[c].astype(str).str.strip()

    # Interacciones = likes + comentarios (NaN->0 SOLO para esta suma)
    df["Interacciones"] = df[COL_LIKES].fillna(0) + df[COL_COMENT].fillna(0)
    return df

def load_all():
    return _load_all_cached(_cache_key())

def _parse_multi(param_value: str):
    """'A,B,C' -> ['A','B','C']  | '', None -> []"""
    if not param_value:
        return []
    parts = [p.strip() for p in param_value.split(",") if p.strip()]
    return list(dict.fromkeys(parts))  # sin duplicados, preserva orden

def aplicar_filtros(df):
    """
    Filtros:
      red=R1,R2    (opcional, multi)
      semana=S1    (opcional, única)
      espectro=E1,E2 (opcional, multi)
    """
    red_multi      = _parse_multi((request.args.get("red") or "").strip())
    semana         = (request.args.get("semana") or "").strip()
    espectro_multi = _parse_multi((request.args.get("espectro") or "").strip())

    if red_multi:
        df = df[df[COL_RED].isin(red_multi)]
    if semana:
        df = df[df["Semana"] == semana]
    if espectro_multi:
        df = df[df[COL_ESPECTRO].isin(espectro_multi)]
    return df

# ============== APP ==============
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

    espectro_colors = {
        "Centro":    "rgba(16,185,129,0.55)",   # verde
        "Derecha":   "rgba(59,130,246,0.55)",   # azul
        "Izquierda": "rgba(245,158,11,0.55)",   # naranja
    }

    template = """
<!doctype html>
<html lang="es">
<head>
<meta charset="utf-8" />
<title>Dashboard de Candidatos</title>
<meta name="viewport" content="width=device-width, initial-scale=1" />
<script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
<style>
  :root{ --maxw: 1200px; }
  body { font-family: system-ui,-apple-system,Segoe UI,Roboto; background:#f6f8fa; margin:0; }
  .container{ max-width: var(--maxw); margin:0 auto; padding:24px 18px 36px; }
  h1 { margin:0 0 6px; font-size:28px; }
  .sub { color:#6b7280; margin-bottom:18px; font-weight:600 }
  .cards { display:grid; grid-template-columns: repeat(4, 1fr); gap:16px; margin: 14px 0 24px; }
  .card { background:#fff; border-radius:12px; padding:16px; box-shadow:0 6px 16px rgba(0,0,0,.06); }
  .kpi { font-size:12px; color:#6b7280; text-transform:uppercase; letter-spacing:.5px; }
  .val { font-size:28px; font-weight:800; margin-top:6px; }
  .grid3 { display:grid; grid-template-columns: repeat(3, 1fr); gap:16px; }
  .panel { background:#fff; border-radius:12px; padding:14px; box-shadow:0 6px 16px rgba(0,0,0,.06); overflow:auto; }
  .filters { display:flex; gap:16px; align-items:flex-start; margin:8px 0 16px; flex-wrap:wrap; }
  .chipwrap label{ display:inline-flex; align-items:center; gap:6px; margin:4px 6px; padding:6px 10px; border:1px solid #e5e7eb; border-radius:999px; background:#fff; cursor:pointer; }
  select, button { padding:8px 10px; border-radius:8px; border:1px solid #e5e7eb; background:#fff; }
  table { width:100%; border-collapse:collapse; }
  th, td { padding:8px 10px; border-bottom:1px solid #e5e7eb; text-align:left; }
  .cell { text-align:center; }
  /* tamaños por defecto de gráficos (más razonables en desktop) */
  #likesPorCandidato, #comentPorCandidato, #candidatosTodos { height: 360px; }
  #ganadoresStack { height: 380px; }

  /* Heatmaps: scroll horizontal; semanal con ancho mayor */
  .heatwrap{ overflow-x:auto; -webkit-overflow-scrolling: touch; }
  .heatwrap table{ min-width: 760px; table-layout: fixed; border-collapse: separate; border-spacing: 0; }
  .heatwrap th, .heatwrap td{ white-space: nowrap; font-size:12px; }
  /* El semanal necesita aún más ancho + fuentes más chicas para evitar superposición en móvil */
  #heatmapSemanal .heatwrap table{ min-width: 1100px; }
  #heatmapSemanal .heatwrap th, #heatmapSemanal .heatwrap td{ font-size:11px; }

  @media (max-width:1200px) {
    .grid3 { grid-template-columns: 1fr; }
    .cards { grid-template-columns: 1fr 1fr; }
  }
  @media (max-width: 768px){
    .container{ padding:18px 12px 28px; }
    #likesPorCandidato, #comentPorCandidato, #candidatosTodos { height: 320px; }
    #ganadoresStack { height: 340px; }
    .heatwrap th, .heatwrap td{ font-size: 11px; }
    #heatmapSemanal .heatwrap table{ min-width: 1200px; }
    #heatmapSemanal .heatwrap th, #heatmapSemanal .heatwrap td{ font-size:10px; padding:6px 8px; }
  }
</style>
</head>
<body>
<div class="container">
  <h1>Dashboard de Candidatos por Red</h1>
  <div class="sub">Elaborado por Angélica Méndez</div>

  <div class="cards">
    <div class="card"><div class="kpi">Filas analizadas</div><div class="val">{{ total_filas | int }}</div></div>
    <div class="card"><div class="kpi">Suma de likes promedio</div><div class="val">{{ total_likes | int }}</div></div>
    <div class="card"><div class="kpi">Suma de comentarios promedio</div><div class="val">{{ total_coment | int }}</div></div>
    <div class="card"><div class="kpi">Candidatos únicos</div><div class="val">{{ n_candidatos | int }}</div></div>
  </div>

  <div class="panel">
    <div class="filters">
      <div>
        <strong>Red(es):</strong><br>
        <span id="chipsRed" class="chipwrap"></span>
      </div>

      <div>
        <strong>Semana:</strong><br>
        <select id="selSemana"><option value="">(todas)</option>{% for s in semanas %}<option value="{{ s }}">{{ s }}</option>{% endfor %}</select>
      </div>

      <div>
        <strong>Espectro(s):</strong><br>
        <span id="chipsEsp" class="chipwrap"></span>
      </div>

      <div style="align-self:flex-end">
        <button onclick="aplicar()">Aplicar</button>
        <button onclick="limpiar()">Limpiar</button>
      </div>
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

  <div class="panel" style="margin-top:16px">
    <h3>Ganadores por semana y espectro</h3>
    <canvas id="ganadoresStack"></canvas>
    <div id="tablaGanadores" style="margin-top:10px"></div>
  </div>

  <div class="panel" style="margin-top:16px">
    <h3>Heatmap de interacciones (Candidato × Red)</h3>
    <div id="heatmap"></div>
  </div>

  <div class="panel" style="margin-top:16px">
    <div class="filters">
      <h3 style="margin:0">Heatmaps semanales (Candidato × Semana)</h3>
      <span style="flex:1"></span>
      <label>Métrica:</label>
      <select id="selMetric">
        <option value="interacciones" selected>Interacciones</option>
        <option value="likes">Likes</option>
        <option value="comentarios">Comentarios</option>
      </select>
      <button onclick="redibujarSemanal()">Aplicar</button>
    </div>
    <div id="heatmapSemanal"></div>
  </div>
</div>

<script>
  // --- datos desde backend (seguros con tojson) ---
  const ESPECTRO_COLORS = {{ espectro_colors | tojson }};
  const REDES           = {{ redes | tojson }};
  const SEMANAS         = {{ semanas | tojson }};
  const ESPECTROS       = {{ espectros | tojson }};

  // --- registro global de instancias Chart.js (para destruir antes de redibujar) ---
  const CH = { likes:null, coment:null, todos:null, winners:null };
  function drawChart(ctx, cfg, key){
    if (CH[key]) { try { CH[key].destroy(); } catch(e){} }
    CH[key] = new Chart(ctx, cfg);
    return CH[key];
  }

  // paleta para "general"
  const PALETTE = [
    "rgba(99,102,241,0.55)","rgba(236,72,153,0.55)","rgba(34,197,94,0.55)","rgba(59,130,246,0.55)",
    "rgba(234,179,8,0.55)","rgba(244,114,182,0.55)","rgba(16,185,129,0.55)","rgba(251,113,133,0.55)",
    "rgba(96,165,250,0.55)","rgba(250,204,21,0.55)","rgba(147,197,253,0.55)","rgba(253,186,116,0.55)"
  ];

  // ===== Helpers URL =====
  function qs(name){ const u=new URL(window.location.href); return u.searchParams.get(name)||""; }
  function qsmulti(name){ const v=qs(name); return v? v.split(",").map(s=>s.trim()).filter(Boolean) : []; }

  // ===== Chips (checkbox) para móvil/desktop =====
  function renderChips(containerId, items, qsParam){
    const cont = document.getElementById(containerId);
    const sel = new Set(qsmulti(qsParam)); // pre-selección desde URL
    cont.innerHTML = items.map(v => {
      const checked = sel.has(v) ? 'checked' : '';
      return `
        <label>
          <input type="checkbox" name="${qsParam}" value="${v}" ${checked} />
          <span>${v}</span>
        </label>`;
    }).join('');
  }
  function getChipValues(name){
    return Array.from(document.querySelectorAll('input[type=checkbox][name="'+name+'"]:checked')).map(i=>i.value);
  }

  // render chips
  renderChips('chipsRed', REDES, 'red');
  renderChips('chipsEsp', ESPECTROS, 'espectro');

  // pre-selección de semana
  document.getElementById('selSemana').value = qs('semana');

  function aplicar(){
    const u=new URL(window.location.href);
    const reds = getChipValues('red');
    const esps = getChipValues('espectro');
    const sem  = document.getElementById('selSemana').value || '';

    if(reds.length) u.searchParams.set('red', reds.join(',')); else u.searchParams.delete('red');
    if(esps.length) u.searchParams.set('espectro', esps.join(',')); else u.searchParams.delete('espectro');
    if(sem) u.searchParams.set('semana', sem); else u.searchParams.delete('semana');

    window.location.href = u.toString();
  }
  function limpiar(){
    const u=new URL(window.location.href);
    ['red','semana','espectro'].forEach(p=>u.searchParams.delete(p));
    window.location.href=u.toString();
  }

  // ===== Layout helpers =====
  function setDynamicHeight(canvasId, count){
    const c = document.getElementById(canvasId);
    const espectroFiltrado = qsmulti('espectro').length>0;
    const rowHeight = espectroFiltrado ? 18 : 24;
    const padding   = (count <= 3) ? 36 : 96;
    const maxPx     = 700;
    const rows = Math.max(count, 1);
    const h = Math.min(rows * rowHeight + padding, maxPx);
    c.style.height = h + 'px';
  }
  function colorsBySpectro(arr, espectros) {
    return arr.map((_,i)=> ESPECTRO_COLORS[espectros[i]] || "rgba(107,114,128,0.35)");
  }
  function colorsByCandidate(n) { return Array.from({length:n}, (_,i)=> PALETTE[i % PALETTE.length]); }

  async function draw(){
    const params = new URLSearchParams();
    const reds = qsmulti('red'), esps = qsmulti('espectro');
    if(reds.length) params.set('red', reds.join(','));
    if(qs('semana')) params.set('semana', qs('semana'));
    if(esps.length) params.set('espectro', esps.join(','));

    const likesCand = await fetch('/api/likes-por-candidato?'+params.toString()).then(r=>r.json());
    const comCand   = await fetch('/api/comentarios-por-candidato?'+params.toString()).then(r=>r.json());
    const todos     = await fetch('/api/candidatos-todos?'+params.toString()).then(r=>r.json());
    const winners   = await fetch('/api/ganador-semanal?'+params.toString()).then(r=>r.json());
    const winSeries = await fetch('/api/ganador-semanal-series?'+params.toString()).then(r=>r.json());
    const matrix    = await fetch('/api/heatmap?'+params.toString()).then(r=>r.json());

    // alturas horizontales
    setDynamicHeight('likesPorCandidato', likesCand.length);
    setDynamicHeight('comentPorCandidato', comCand.length);
    setDynamicHeight('candidatosTodos',   todos.length);

    const baseOpts = {
      indexAxis:'y',
      responsive:true,
      maintainAspectRatio:false,
      animation:false,
      plugins: { legend: { display:false } },
      scales: { y: { ticks: { autoSkip:false } }, x:{ ticks:{ maxTicksLimit: 8 } } }
    };
    const espectroOn = esps.length>0;
    const barCfg = espectroOn
      ? { categoryPercentage:0.62, barPercentage:0.62, maxBarThickness:18 }
      : { categoryPercentage:0.78, barPercentage:0.78, maxBarThickness:26 };

    // Likes
    drawChart(
      document.getElementById('likesPorCandidato').getContext('2d'),
      {
        type: 'bar',
        data: {
          labels: likesCand.map(d=>d.candidato),
          datasets: [{
            label: 'Likes promedio',
            data: likesCand.map(d=>d.likes),
            backgroundColor: espectroOn ? colorsBySpectro(likesCand, likesCand.map(d=>d.espectro))
                                         : colorsByCandidate(likesCand.length),
            ...barCfg
          }]
        },
        options: baseOpts
      },
      'likes'
    );

    // Comentarios
    drawChart(
      document.getElementById('comentPorCandidato').getContext('2d'),
      {
        type: 'bar',
        data: {
          labels: comCand.map(d=>d.candidato),
          datasets: [{
            label: 'Comentarios promedio',
            data: comCand.map(d=>d.comentarios),
            backgroundColor: espectroOn ? colorsBySpectro(comCand, comCand.map(d=>d.espectro))
                                         : colorsByCandidate(comCand.length),
            ...barCfg
          }]
        },
        options: baseOpts
      },
      'coment'
    );

    // Todos (likes)
    drawChart(
      document.getElementById('candidatosTodos').getContext('2d'),
      {
        type: 'bar',
        data: {
         
