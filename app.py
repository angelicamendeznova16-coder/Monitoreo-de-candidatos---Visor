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
    # Página liviana: NO lee Excel. Todo se carga por fetch().
    espectro_colors = {
        "Centro":    "rgba(16,185,129,0.55)",   # verde
        "Derecha":   "rgba(59,130,246,0.55)",   # azul
        "Izquierda": "rgba(245,158,11,0.55)",   # naranja
    }
    template = r'''
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
  /* "skeleton" simple mientras cargan datos */
  .skeleton{ background:linear-gradient(90deg,#eee,#f5f5f5,#eee); background-size:200% 100%; animation:sh 1.2s infinite; border-radius:8px; height:20px; }
  @keyframes sh{ 0%{background-position:200% 0} 100%{background-position:-200% 0} }
</style>
</head>
<body>
<div class="container">
  <h1>Dashboard de Candidatos por Red</h1>
  <div class="sub">Elaborado por Angélica Méndez</div>

  <div class="cards" id="kpis">
    <div class="card"><div class="kpi">Filas analizadas</div><div class="val" id="kpiFilas"><span class="skeleton" style="width:80px;display:inline-block"></span></div></div>
    <div class="card"><div class="kpi">Suma de likes promedio</div><div class="val" id="kpiLikes"><span class="skeleton" style="width:80px;display:inline-block"></span></div></div>
    <div class="card"><div class="kpi">Suma de comentarios promedio</div><div class="val" id="kpiCom"><span class="skeleton" style="width:80px;display:inline-block"></span></div></div>
    <div class="card"><div class="kpi">Candidatos únicos</div><div class="val" id="kpiCand"><span class="skeleton" style="width:80px;display:inline-block"></span></div></div>
  </div>

  <div class="panel">
    <div class="filters">
      <div>
        <strong>Red(es):</strong><br>
        <span id="chipsRed" class="chipwrap"><span class="skeleton" style="display:inline-block;width:200px"></span></span>
      </div>

      <div>
        <strong>Semana:</strong><br>
        <select id="selSemana"><option value="">(todas)</option></select>
      </div>

      <div>
        <strong>Espectro(s):</strong><br>
        <span id="chipsEsp" class="chipwrap"><span class="skeleton" style="display:inline-block;width:200px"></span></span>
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
  // --- colores de espectro desde el servidor ---
  const ESPECTRO_COLORS = {{ espectro_colors | tojson }};

  // paleta
  const PALETTE = [
    "rgba(99,102,241,0.55)","rgba(236,72,153,0.55)","rgba(34,197,94,0.55)","rgba(59,130,246,0.55)",
    "rgba(234,179,8,0.55)","rgba(244,114,182,0.55)","rgba(16,185,129,0.55)","rgba(251,113,133,0.55)",
    "rgba(96,165,250,0.55)","rgba(250,204,21,0.55)","rgba(147,197,253,0.55)","rgba(253,186,116,0.55)"
  ];

  // Bootstrap data (redes, semanas, espectros) llega por API:
  let REDES = [], SEMANAS = [], ESPECTROS = [];

  // --- registro global de charts para evitar "gráficos apilados" ---
  const CH = { likes:null, coment:null, todos:null, winners:null };
  function drawChart(ctx, cfg, key){
    if (CH[key]) { try { CH[key].destroy(); } catch(e){} }
    CH[key] = new Chart(ctx, cfg);
    return CH[key];
  }

  // ===== Helpers URL =====
  function qs(name){ const u=new URL(window.location.href); return u.searchParams.get(name)||""; }
  function qsmulti(name){ const v=qs(name); return v? v.split(",").map(s=>s.trim()).filter(Boolean) : []; }

  // ===== Chips (checkbox) =====
  function renderChips(containerId, items, qsParam){
    const cont = document.getElementById(containerId);
    const sel = new Set(qsmulti(qsParam));
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

  // ========== Bootstrap inicial ==========
  async function bootstrap(){
    const boot = await fetch('/api/bootstrap').then(r=>r.json());

    REDES = boot.redes || [];
    SEMANAS = boot.semanas || [];
    ESPECTROS = boot.espectros || [];

    // KPIs
    document.getElementById('kpiFilas').innerText = (boot.kpis.filas || 0).toLocaleString('es-ES');
    document.getElementById('kpiLikes').innerText = (boot.kpis.likes || 0).toLocaleString('es-ES');
    document.getElementById('kpiCom').innerText   = (boot.kpis.coment || 0).toLocaleString('es-ES');
    document.getElementById('kpiCand').innerText  = (boot.kpis.candidatos || 0).toLocaleString('es-ES');

    // Semana
    const selSemana = document.getElementById('selSemana');
    selSemana.innerHTML = '<option value="">(todas)</option>' + SEMANAS.map(s=>`<option value="${s}">${s}</option>`).join('');
    selSemana.value = qs('semana');

    // Chips
    renderChips('chipsRed', REDES, 'red');
    renderChips('chipsEsp', ESPECTROS, 'espectro');

    // Dibujar todo
    await drawAll();
  }

  async function drawAll(){
    const params = new URLSearchParams();
    const reds = qsmulti('red'), esps = qsmulti('espectro');
    if(reds.length) params.set('red', reds.join(','));
    if(qs('semana')) params.set('semana', qs('semana'));
    if(esps.length) params.set('espectro', esps.join(','));

    const [likesCand, comCand, todos, winners, winSeries, matrix] = await Promise.all([
      fetch('/api/likes-por-candidato?'+params.toString()).then(r=>r.json()),
      fetch('/api/comentarios-por-candidato?'+params.toString()).then(r=>r.json()),
      fetch('/api/candidatos-todos?'+params.toString()).then(r=>r.json()),
      fetch('/api/ganador-semanal?'+params.toString()).then(r=>r.json()),
      fetch('/api/ganador-semanal-series?'+params.toString()).then(r=>r.json()),
      fetch('/api/heatmap?'+params.toString()).then(r=>r.json())
    ]);

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
    const espectroOn = qsmulti('espectro').length>0;
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
          labels: todos.map(d=>d.candidato),
          datasets: [{
            label: 'Likes promedio',
            data: todos.map(d=>d.likes),
            backgroundColor: espectroOn ? colorsBySpectro(todos, todos.map(d=>d.espectro))
                                         : colorsByCandidate(todos.length),
            ...barCfg
          }]
        },
        options: baseOpts
      },
      'todos'
    );

    // Ganadores (dual)
    const canvasStack = document.getElementById('ganadoresStack');
    const ctxStack = canvasStack.getContext('2d');
    const espsSel = qsmulti('espectro');
    const fmt = (v) => new Intl.NumberFormat('es-ES').format(Math.round(v||0));

    if (espsSel.length === 1) {
      const esp = espsSel[0];
      const w = winners.filter(x => x.espectro === esp);
      const labels = w.map(x => x.candidato || 'ND');
      const data   = w.map(x => x.nd ? 0 : x.interacciones);
      const baseH = 340, extra = Math.max(0, (labels.length - 8)) * 10;
      canvasStack.style.height = (baseH + extra) + 'px';

      drawChart(ctxStack, {
        type: 'bar',
        data: {
          labels,
          datasets: [{
            label: esp,
            data,
            backgroundColor: ESPECTRO_COLORS[esp] || 'rgba(107,114,128,0.35)',
            borderColor: ESPECTRO_COLORS[esp] || 'rgba(107,114,128,0.55)',
            borderWidth: 1,
            maxBarThickness: 28,
            categoryPercentage: 0.6,
            barPercentage: 0.6
          }]
        },
        options: {
          responsive:true, maintainAspectRatio:false, animation:false,
          plugins:{
            legend:{ display:false },
            tooltip:{ callbacks:{ label: (ctx)=> fmt(ctx.raw) + ' interacciones' } }
          },
          scales:{ x:{ ticks:{ autoSkip:false } }, y:{ title:{ display:true, text:'Interacciones' } } }
        }
      }, 'winners');

    } else {
      const nadaFiltrado = espsSel.length === 0;
      canvasStack.style.height = '380px';

      const stackDatasets = (winSeries.espectros || []).map(esp => ({
        label: esp,
        data: (winSeries.semanas || []).map(sem => {
          const cell = (winSeries.values || []).find(v => v.espectro===esp && v.semana===sem);
          return cell ? (cell.nd? 0 : cell.interacciones) : 0;
        }),
        backgroundColor: ESPECTRO_COLORS[esp] || 'rgba(107,114,128,0.35)',
        borderColor: ESPECTRO_COLORS[esp] || 'rgba(107,114,128,0.55)',
        borderWidth: nadaFiltrado ? 1 : 0,
        maxBarThickness: 28
      }));

      drawChart(ctxStack, {
        type: 'bar',
        data: { labels: (winSeries.semanas || []).map((s,i)=>'S'+(i+1)), datasets: stackDatasets },
        options: {
          responsive:true, maintainAspectRatio:false, animation:false,
          plugins:{ 
            legend:{ position:'top' },
            tooltip: {
              callbacks: {
                title: (items)=> {
                  const idx = items?.[0]?.dataIndex ?? 0;
                  const s = winSeries.semanas?.[idx] || '';
                  return 'Semana: ' + s;
                },
                label: (ctx) => {
                  const esp = ctx.dataset.label;
                  const idx = ctx.dataIndex;
                  const sem = winSeries.semanas?.[idx];
                  const cell = (winSeries.values || []).find(v => v.espectro===esp && v.semana===sem);
                  const ganador = cell?.nd ? 'ND' : (cell && cell.interacciones>0 ? 'Ganador: '+(cell.candidato||'') : 'Sin ganador');
                  const valor = ' • ' + fmt(ctx.raw) + ' interacciones';
                  return `${ganador}${valor}`;
                }
              }
            }
          },
          scales:{ x:{ stacked:true }, y:{ stacked:true, title:{ display:true, text:'Interacciones (ganador por espectro)' } } }
        }
      }, 'winners');
    }

    // Heatmap general
    const hm = document.getElementById('heatmap');
    if(!matrix.values.length) {
      hm.innerHTML = '<em>Sin datos.</em>';
    } else {
      const rows = matrix.rows, cols = matrix.cols, vals = matrix.values;
      const max = Math.max(...vals.map(v=>v.valor||0));
      let html = '<table><thead><tr><th></th>';
      for (const col of cols) html += `<th>${col}</th>`;
      html += '</tr></thead><tbody>';
      for (const r of rows) {
        html += `<tr><th>${r}</th>`;
        for (const c of cols) {
          const item = vals.find(v => v.candidato===r && v.red===c);
          const v = item ? (item.valor||0) : 0;
          const pct = max? (v/max) : 0;
          const bg = `rgba(59,130,246,${0.08 + 0.6*pct})`;
          const disp = item && item.nd ? 'ND' : (v ? new Intl.NumberFormat('es-ES').format(Math.round(v)) : '');
          html += `<td class="cell" style="background:${bg}">${disp}</td>`;
        }
        html += '</tr>';
      }
      html += '</tbody></table>';
      hm.innerHTML = '<div class="heatwrap">' + html + '</div>';
    }

    // Heatmap semanal
    await redibujarSemanal();
  }

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

  async function redibujarSemanal(){
    const metric = document.getElementById('selMetric').value;
    const params = new URLSearchParams();
    const reds = qsmulti('red'), esps = qsmulti('espectro');
    if(reds.length) params.set('red', reds.join(','));
    if(qs('semana')) params.set('semana', qs('semana'));
    if(esps.length) params.set('espectro', esps.join(','));
    params.set('metric', metric);

    const m = await fetch('/api/heatmap-semanal?'+params.toString()).then(r=>r.json());
    const el = document.getElementById('heatmapSemanal');
    if(!m.values.length){
      el.innerHTML = '<em>Sin datos para los filtros/semana.</em>';
      return;
    }
    const rows = m.rows, cols = m.cols, vals = m.values;
    const max = Math.max(...vals.map(v=>v.valor||0));
    const shortCols = cols.map((c,i)=> 'S'+(i+1)); // columnas cortas

    let html = '<table><thead><tr><th></th>';
    for (const sc of shortCols) html += `<th>${sc}</th>`;
    html += '</tr></thead><tbody>';
    for (let i=0;i<rows.length;i++){
      const r = rows[i];
      html += `<tr><th>${r}</th>`;
      for (let j=0;j<cols.length;j++){
        const c = cols[j];
        const item = vals.find(v => v.candidato===r && v.semana===c);
        const v = item ? (item.valor||0) : 0;
        const pct = max? (v/max) : 0;
        const bg = `rgba(234,88,12,${0.07 + 0.6*pct})`; // naranja suave
        const disp = item && item.nd ? 'ND' : (v ? new Intl.NumberFormat('es-ES').format(Math.round(v)) : '');
        html += `<td class="cell" style="background:${bg}">${disp}</td>`;
      }
      html += '</tr>';
    }
    html += '</tbody></table>';
    el.innerHTML = '<div class="heatwrap">' + html + '</div>';
  }

  // Arrancar
  bootstrap();
</script>
</body>
</html>
'''
    # 🔧 FIX: pasar espectro_colors al template (evita 'Undefined' en tojson)
    return render_template_string(template, espectro_colors=espectro_colors)

# ================== APIs ==================
@app.route("/api/bootstrap")
def api_bootstrap():
    df = load_all()
    redes     = sorted(df[COL_RED].dropna().unique().tolist()) if not df.empty else []
    semanas   = sorted(df["Semana"].dropna().unique().tolist()) if not df.empty else []
    espectros = sorted(df[COL_ESPECTRO].dropna().unique().tolist()) if not df.empty else []
    kpis = {
        "filas": len(df),
        "likes": int(df[COL_LIKES].fillna(0).sum()) if COL_LIKES in df else 0,
        "coment": int(df[COL_COMENT].fillna(0).sum()) if COL_COMENT in df else 0,
        "candidatos": df[COL_CANDIDATO].nunique() if not df.empty else 0
    }
    return jsonify({"redes": redes, "semanas": semanas, "espectros": espectros, "kpis": kpis})

@app.route("/api/likes-por-candidato")
def api_likes_por_candidato():
    df = aplicar_filtros(load_all())
    if df.empty: return jsonify([])
    g = (df.groupby([COL_CANDIDATO, COL_ESPECTRO], as_index=False)[COL_LIKES]
           .mean()
           .rename(columns={COL_LIKES:"likes"})
           .sort_values("likes", ascending=False))
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
           .rename(columns={COL_COMENT:"comentarios"})
           .sort_values("comentarios", ascending=False))
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
           .rename(columns={COL_LIKES:"likes"})
           .sort_values("likes", ascending=False))
    out = [{"candidato": r[COL_CANDIDATO], "espectro": r[COL_ESPECTRO],
            "likes": float(0 if pd.isna(r["likes"]) else r["likes"])}
           for _, r in g.iterrows()]
    return jsonify(out)

@app.route("/api/ganador-semanal")
def api_ganador_semanal():
    """
    Devuelve SIEMPRE la grilla completa (Semana × Espectro).
    Si no hay datos para una combinación, marca nd=True (no data).
    """
    full = load_all()
    filtered = aplicar_filtros(full)

    semanas_dom  = sorted(full["Semana"].unique().tolist()) if not (request.args.get("semana") or "").strip() else \
                   sorted(filtered["Semana"].unique().tolist())
    espectros_q  = (request.args.get("espectro") or "").strip()
    espectros_dom = sorted(full[COL_ESPECTRO].unique().tolist()) if not espectros_q else \
                    sorted(_parse_multi(espectros_q))

    out = []
    for sem in semanas_dom:
        for esp in espectros_dom:
            df_se = filtered[(filtered["Semana"] == sem) & (filtered[COL_ESPECTRO] == esp)]
            if df_se.empty:
                out.append({"semana": sem, "espectro": esp, "candidato": None, "interacciones": 0.0, "nd": True})
            else:
                g = df_se.groupby(COL_CANDIDATO, as_index=False)["Interacciones"].mean()
                row = g.loc[g["Interacciones"].idxmax()]
                out.append({
                    "semana": sem, "espectro": esp, "candidato": row[COL_CANDIDATO],
                    "interacciones": float(row["Interacciones"]), "nd": False
                })
    out.sort(key=lambda x: (x["semana"], x["espectro"]))
    return jsonify(out)

@app.route("/api/ganador-semanal-series")
def api_ganador_semanal_series():
    """
    Para la gráfica apilada: por cada Semana y Espectro, el valor del ganador (Interacciones).
    """
    full = load_all()
    filtered = aplicar_filtros(full)
    if filtered.empty:
        return jsonify({"semanas": [], "espectros": [], "values": []})

    semanas = sorted(filtered["Semana"].unique().tolist())
    espectros = sorted(filtered[COL_ESPECTRO].unique().tolist())

    values = []
    for sem in semanas:
        for esp in espectros:
            df_se = filtered[(filtered["Semana"] == sem) & (filtered[COL_ESPECTRO] == esp)]
            if df_se.empty:
                values.append({"semana": sem, "espectro": esp, "interacciones": 0.0, "nd": True})
            else:
                g = df_se.groupby(COL_CANDIDATO, as_index=False)["Interacciones"].mean()
                row = g.loc[g["Interacciones"].idxmax()]
                values.append({"semana": sem, "espectro": esp, "interacciones": float(row["Interacciones"]), "nd": False, "candidato": row[COL_CANDIDATO]})
    return jsonify({"semanas": semanas, "espectros": espectros, "values": values})

@app.route("/api/heatmap")
def api_heatmap():
    """Heatmap general (Candidato × Red) por Interacciones promedio."""
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
            if sub.empty or pd.isna(sub["Interacciones"].iloc[0]):
                values.append({"candidato": r, "red": c, "valor": 0, "nd": True})
            else:
                v = float(sub["Interacciones"].iloc[0])
                values.append({"candidato": r, "red": c, "valor": v, "nd": False})
    return jsonify({"rows": rows, "cols": cols, "values": values})

@app.route("/api/heatmap-semanal")
def api_heatmap_semanal():
    """
    Heatmap semanal (Candidato × Semana) con métrica:
    - metric=interacciones | likes | comentarios
    Respeta filtros de red/espectro/semana.
    """
    metric = (request.args.get("metric") or "interacciones").lower()
    df = aplicar_filtros(load_all())
    if df.empty:
        return jsonify({"rows": [], "cols": [], "values": []})

    if metric == "likes":
        col = COL_LIKES
    elif metric == "comentarios":
        col = COL_COMENT
    else:
        col = "Interacciones"

    rows = sorted(df[COL_CANDIDATO].unique().tolist())
    cols = sorted(df["Semana"].unique().tolist())
    g = df.groupby([COL_CANDIDATO, "Semana"], as_index=False)[col].mean()

    values = []
    for r in rows:
        for c in cols:
            sub = g[(g[COL_CANDIDATO]==r) & (g["Semana"]==c)]
            if sub.empty or pd.isna(sub[col].iloc[0]):
                values.append({"candidato": r, "semana": c, "valor": 0, "nd": True})
            else:
                values.append({"candidato": r, "semana": c, "valor": float(sub[col].iloc[0]), "nd": False})
    return jsonify({"rows": rows, "cols": cols, "values": values})

# ---- Run local (no usado en Render) ----
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
