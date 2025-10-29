import os
import re
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

# === Mapeo de nombres de hoja -> etiqueta visible de semana ===
WEEK_MAP = {
    "Semana 1": "7 Sep - 14 Sep",
    "Semana 2": "15 Sep - 21 Sep",
    "Semana 3": "23 Sep - 1 Oct",
    "Semana 4": "2 Oct - 8 Oct",
    "Semana 5": "9 Oct - 15 Oct",
    "Semana 6": "16 Oct - 22 Oct",
    "Semana 7": "23 Oct - 28 Oct",
}
WEEK_ORDER = list(WEEK_MAP.values())

# === Utils ===
def _natural_key(s):
    return [int(t) if t.isdigit() else t.lower() for t in re.split(r'(\d+)', str(s))]

def _parse_multi(param_value: str):
    if not param_value:
        return []
    parts = [p.strip() for p in param_value.split(",") if p.strip()]
    return list(dict.fromkeys(parts))

def _valid_str(x):
    """True si es string no vacío ni 'nan'/'none'."""
    if pd.isna(x): return False
    s = str(x).strip().lower()
    if s == "" or s in {"nan","none","null"}: return False
    return True

# ---------- CARGA + LIMPIEZA (con cache) ----------
@lru_cache(maxsize=1)
def _cache_key():
    return os.path.abspath(EXCEL_PATH)

@lru_cache(maxsize=1)
def _load_all_cached(_key):
    if not os.path.exists(EXCEL_PATH):
        cols = [COL_ESPECTRO, COL_CANDIDATO, COL_RED, COL_LIKES, COL_MAXLIKES, COL_TEMA, COL_COMENT, "Semana"]
        return pd.DataFrame(columns=cols)

    xls = pd.ExcelFile(EXCEL_PATH)
    frames = []
    for sh in xls.sheet_names:
        df = pd.read_excel(EXCEL_PATH, sheet_name=sh)
        if df.empty or df.dropna(how="all").empty:
            continue
        etiqueta = WEEK_MAP.get(sh, sh)
        df["Semana"] = etiqueta
        frames.append(df)

    if not frames:
        cols = [COL_ESPECTRO, COL_CANDIDATO, COL_RED, COL_LIKES, COL_MAXLIKES, COL_TEMA, COL_COMENT, "Semana"]
        return pd.DataFrame(columns=cols)

    df = pd.concat(frames, ignore_index=True)

    # --- Limpieza básica de strings —
    # (NO convertir a str todo; solo normalizar si existe y filtrar vacíos reales)
    for c in [COL_ESPECTRO, COL_CANDIDATO, COL_RED, COL_TEMA, "Semana"]:
        if c in df.columns:
            df[c] = df[c].apply(lambda x: None if not _valid_str(x) else str(x).strip())

    # --- DEDUP: (Candidato, Red, Semana) para evitar inflar por repetidos
    dedup_keys = [COL_CANDIDATO, COL_RED, "Semana"]
    if all(k in df.columns for k in dedup_keys):
        df = df.drop_duplicates(subset=dedup_keys, keep="first")

    # --- Tipos numéricos (decimales con punto, sin miles)
    for c in [COL_LIKES, COL_MAXLIKES, COL_COMENT]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    # Filtrar filas sin candidato/red/semana válidos
    df = df[df[COL_CANDIDATO].notna() & df[COL_RED].notna() & df["Semana"].notna()]

    # Interacciones simple (para heatmaps/ganadores)
    df["Interacciones"] = df[COL_LIKES].fillna(0) + df[COL_COMENT].fillna(0)
    return df

def load_all():
    return _load_all_cached(_cache_key())

def aplicar_filtros(df):
    red_multi      = _parse_multi((request.args.get("red") or "").strip())
    semana_multi   = _parse_multi((request.args.get("semana") or "").strip())
    espectro_multi = _parse_multi((request.args.get("espectro") or "").strip())
    mes_multi      = _parse_multi((request.args.get("mes") or "").strip())

    if red_multi:
        df = df[df[COL_RED].isin(red_multi)]
    if semana_multi:
        df = df[df["Semana"].isin(semana_multi)]
    if espectro_multi:
        df = df[df[COL_ESPECTRO].isin(espectro_multi)]
    if mes_multi:
        abrev = []
        for m in mes_multi:
            ml = m.strip().lower()
            if ml.startswith("sep"): abrev.append("Sep")
            elif ml.startswith("oct"): abrev.append("Oct")
        if abrev:
            mask = df["Semana"].astype(str).apply(lambda s: any(a in s for a in abrev))
            df = df[mask]
    return df

# === Agregación clave para las BARRAS ===
def _agg_semana_then_candidato(df, value_col):
    """
    Paso 1: promedio por Candidato×Semana (promedia entre redes de esa semana).
    Paso 2: promedio entre semanas por Candidato (y mantenemos Espectro para color).
    """
    # 1) Candidato×Semana (promedio entre redes)
    by_week = (df.groupby([COL_CANDIDATO, "Semana"], as_index=False)[value_col]
                 .mean())
    # Traer Espectro (modo estable: el más frecuente por candidato en el df filtrado)
    esp_map = (df.groupby(COL_CANDIDATO)[COL_ESPECTRO]
                 .agg(lambda s: s.mode().iat[0] if not s.mode().empty else s.dropna().iat[0]
                      if s.dropna().size else None)
                 .to_dict())
    by_week[COL_ESPECTRO] = by_week[COL_CANDIDATO].map(esp_map)

    # 2) Promedio entre semanas por candidato
    final = (by_week.groupby([COL_CANDIDATO, COL_ESPECTRO], as_index=False)[value_col]
                   .mean())
    return final

# ============== APP ==============
app = Flask(__name__)

@app.route("/")
def index():
    espectro_colors = {
        "Centro":    "rgba(16,185,129,0.55)",
        "Derecha":   "rgba(59,130,246,0.55)",
        "Izquierda": "rgba(245,158,11,0.55)",
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

  canvas { display:block; width:100%; }
  #likesPorCandidato, #comentPorCandidato, #candidatosTodos { min-height: 180px; }
  #ganadoresStack { min-height: 200px; }

  .heatwrap{ overflow-x:auto; -webkit-overflow-scrolling: touch; }
  .heatwrap table{ min-width: 760px; table-layout: fixed; border-collapse: separate; border-spacing: 0; }
  .heatwrap th, .heatwrap td{ white-space: nowrap; font-size:12px; }
  #heatmapSemanal .heatwrap table{ min-width: 1100px; }
  #heatmapSemanal .heatwrap th, #heatmapSemanal .heatwrap td{ font-size:11px; }

  @media (max-width:1200px) {
    .grid3 { grid-template-columns: 1fr; }
    .cards { grid-template-columns: 1fr 1fr; }
  }
  @media (max-width: 768px){
    .container{ padding:18px 12px 28px; }
    .heatwrap th, .heatwrap td{ font-size: 11px; }
    #heatmapSemanal .heatwrap table{ min-width: 1200px; }
    #heatmapSemanal .heatwrap th, #heatmapSemanal .heatwrap td{ font-size:10px; padding:6px 8px; }
  }
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
        <strong>Tiempo:</strong><br>
        <div style="font-size:12px;color:#6b7280;margin-bottom:4px">Semanas</div>
        <span id="chipsSemana" class="chipwrap"><span class="skeleton" style="display:inline-block;width:220px"></span></span>
        <div style="font-size:12px;color:#6b7280;margin:8px 0 4px">Meses</div>
        <span id="chipsMes" class="chipwrap"><span class="skeleton" style="display:inline-block;width:160px"></span></span>
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
  const ESPECTRO_COLORS = {{ espectro_colors | tojson }};

  const PALETTE = [
    "rgba(99,102,241,0.55)","rgba(236,72,153,0.55)","rgba(34,197,94,0.55)","rgba(59,130,246,0.55)",
    "rgba(234,179,8,0.55)","rgba(244,114,182,0.55)","rgba(16,185,129,0.55)","rgba(251,113,133,0.55)",
    "rgba(96,165,250,0.55)","rgba(250,204,21,0.55)","rgba(147,197,253,0.55)","rgba(253,186,116,0.55)"
  ];

  let REDES = [], SEMANAS = [], ESPECTROS = [], MESES = [];
  const CH = { likes:null, coment:null, todos:null, winners:null };

  function drawChart(ctx, cfg, key){
    if (CH[key]) { try { CH[key].destroy(); } catch(e){} }
    CH[key] = new Chart(ctx, cfg);
    return CH[key];
  }

  function qs(name){ const u=new URL(window.location.href); return u.searchParams.get(name)||""; }
  function qsmulti(name){ const v=qs(name); return v? v.split(",").map(s=>s.trim()).filter(Boolean) : []; }

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

  // ===== Altura/Ancho DINÁMICO =====
  function setDynamicHeight(canvasId, count){
    const c = document.getElementById(canvasId);
    const espectroFiltrado = qsmulti('espectro').length > 0;
    const rowHeight = espectroFiltrado ? 26 : 28;
    const padding   = 40;
    const rows = Math.max(count || 1, 1);
    const h = Math.max(180, Math.min(rows * rowHeight + padding, 600));
    c.height = h;
    const w = (c.parentElement && c.parentElement.clientWidth) ? c.parentElement.clientWidth : 800;
    c.width = w;
  }

  function colorsBySpectro(arr, espectros) {
    return arr.map((_,i)=> ESPECTRO_COLORS[espectros[i]] || "rgba(107,114,128,0.35)");
  }
  function colorsByCandidate(n) { return Array.from({length:n}, (_,i)=> PALETTE[i % PALETTE.length]); }

  async function bootstrap(){
    const boot = await fetch('/api/bootstrap').then(r=>r.json());

    REDES = boot.redes || [];
    SEMANAS = boot.semanas || [];
    MESES = boot.meses || [];
    ESPECTROS = boot.espectros || [];

    document.getElementById('kpiFilas').innerText = (boot.kpis.filas || 0).toLocaleString('es-ES');
    document.getElementById('kpiLikes').innerText = (boot.kpis.likes || 0).toLocaleString('es-ES');
    document.getElementById('kpiCom').innerText   = (boot.kpis.coment || 0).toLocaleString('es-ES');
    document.getElementById('kpiCand').innerText  = (boot.kpis.candidatos || 0).toLocaleString('es-ES');

    renderChips('chipsRed', REDES, 'red');
    renderChips('chipsEsp', ESPECTROS, 'espectro');
    renderChips('chipsSemana', SEMANAS, 'semana');
    renderChips('chipsMes', MESES, 'mes');

    await drawAll();
  }

  async function drawAll(){
    const params = new URLSearchParams();
    const reds = qsmulti('red'), esps = qsmulti('espectro');
    const weeks = qsmulti('semana'), months = qsmulti('mes');

    if(reds.length) params.set('red', reds.join(','));
    if(esps.length) params.set('espectro', esps.join(','));
    if(weeks.length) params.set('semana', weeks.join(','));
    if(months.length) params.set('mes', months.join(','));
    if(qs('semana') && !weeks.length) params.set('semana', qs('semana'));

    const [likesCand, comCand, todos, winners, winSeries, matrix] = await Promise.all([
      fetch('/api/likes-por-candidato?'+params.toString()).then(r=>r.json()),
      fetch('/api/comentarios-por-candidato?'+params.toString()).then(r=>r.json()),
      fetch('/api/candidatos-todos?'+params.toString()).then(r=>r.json()),
      fetch('/api/ganador-semanal?'+params.toString()).then(r=>r.json()),
      fetch('/api/ganador-semanal-series?'+params.toString()).then(r=>r.json()),
      fetch('/api/heatmap?'+params.toString()).then(r=>r.json())
    ]);

    setDynamicHeight('likesPorCandidato', likesCand.length);
    setDynamicHeight('comentPorCandidato', comCand.length);
    setDynamicHeight('candidatosTodos',   todos.length);

    const baseOpts = {
      indexAxis:'y',
      responsive:false,
      maintainAspectRatio:false,
      animation:false,
      plugins: { legend: { display:false } },
      scales: { y: { ticks: { autoSkip:false } }, x:{ ticks:{ maxTicksLimit: 8 } } }
    };
    const espectroOn = qsmulti('espectro').length>0;
    const barCfg = { barThickness: espectroOn ? 16 : 20, categoryPercentage: 0.9, barPercentage: 0.9 };

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

    // Ganadores (sin cambios de lógica)
    const canvasStack = document.getElementById('ganadoresStack');
    const ctxStack = canvasStack.getContext('2d');
    const espsSel = qsmulti('espectro');
    const fmt = (v) => new Intl.NumberFormat('es-ES').format(Math.round(v||0));

    if (espsSel.length === 1) {
      const esp = espsSel[0];
      const w = winners
        .filter(x => x.espectro === esp)
        .sort((a,b) => SEMANAS.indexOf(a.semana) - SEMANAS.indexOf(b.semana));

      const labels = w.map(x => {
        const idx = SEMANAS.indexOf(x.semana);
        const prefix = idx >= 0 ? `S${idx + 1}. ` : '';
        return `${prefix}${x.candidato || 'ND'}`;
      });

      const data   = w.map(x => x.nd ? 0 : x.interacciones);

      setDynamicHeight('ganadoresStack', labels.length);

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
            barThickness: 18,
            categoryPercentage: 0.9,
            barPercentage: 0.9
          }]
        },
        options: {
          indexAxis:'y',
          responsive:false,
          maintainAspectRatio:false,
          animation:false,
          plugins:{
            legend:{ display:false },
            tooltip:{ callbacks:{ 
              title: (items) => {
                const i = items[0].dataIndex;
                const sem = w[i]?.semana || '';
                return sem ? `${sem}` : items[0].label;
              },
              label: (ctx)=> fmt(ctx.raw) + ' interacciones' 
            } }
          },
          scales:{ 
            x:{ ticks:{ maxTicksLimit: 8 } }, 
            y:{ ticks:{ autoSkip:false }, title:{ display:true, text:'Interacciones' } } 
          }
        }
      }, 'winners');

    } else {
      setDynamicHeight('ganadoresStack', (qsmulti('espectro').length || 3) * (SEMANAS.length || 6));

      const stackDatasets = (winSeries.espectros || []).map(esp => ({
        label: esp,
        data: (winSeries.semanas || []).map(sem => {
          const cell = (winSeries.values || []).find(v => v.espectro===esp && v.semana===sem);
          return cell ? (cell.nd? 0 : cell.interacciones) : 0;
        }),
        backgroundColor: ESPECTRO_COLORS[esp] || 'rgba(107,114,128,0.35)',
        borderColor: ESPECTRO_COLORS[esp] || 'rgba(107,114,128,0.55)',
        borderWidth: 0,
        barThickness: 18,
        categoryPercentage: 0.9,
        barPercentage: 0.9
      }));

      drawChart(ctxStack, {
        type: 'bar',
        data: { labels: (winSeries.semanas || []).map((s,i)=>'S'+(i+1)), datasets: stackDatasets },
        options: {
          indexAxis:'x',
          responsive:false,
          maintainAspectRatio:false,
          animation:false,
          plugins:{ legend:{ position:'top' } },
          scales:{ x:{ stacked:true, ticks:{ autoSkip:false } }, y:{ stacked:true, title:{ display:true, text:'Interacciones (ganador por espectro)' } } }
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

    await redibujarSemanal();
  }

  function aplicar(){
    const u=new URL(window.location.href);
    const reds = getChipValues('red');
    const esps = getChipValues('espectro');
    const weeks = getChipValues('semana');
    const months = getChipValues('mes');

    if(reds.length) u.searchParams.set('red', reds.join(',')); else u.searchParams.delete('red');
    if(esps.length) u.searchParams.set('espectro', esps.join(',')); else u.searchParams.delete('espectro');
    if(weeks.length) u.searchParams.set('semana', weeks.join(',')); else u.searchParams.delete('semana');
    if(months.length) u.searchParams.set('mes', months.join(',')); else u.searchParams.delete('mes');

    window.location.href = u.toString();
  }
  function limpiar(){
    const u=new URL(window.location.href);
    ['red','semana','mes','espectro'].forEach(p=>u.searchParams.delete(p));
    window.location.href=u.toString();
  }

  async function redibujarSemanal(){
    const metric = document.getElementById('selMetric').value;
    const params = new URLSearchParams();
    const reds = qsmulti('red'), esps = qsmulti('espectro');
    const weeks = qsmulti('semana'), months = qsmulti('mes');

    if(reds.length) params.set('red', reds.join(','));
    if(esps.length) params.set('espectro', esps.join(','));
    if(weeks.length) params.set('semana', weeks.join(','));
    if(months.length) params.set('mes', months.join(','));
    params.set('metric', metric);

    const m = await fetch('/api/heatmap-semanal?'+params.toString()).then(r=>r.json());
    const el = document.getElementById('heatmapSemanal');
    if(!m.values.length){
      el.innerHTML = '<em>Sin datos para los filtros/semana.</em>';
      return;
    }
    const rows = m.rows, cols = m.cols, vals = m.values;
    const max = Math.max(...vals.map(v=>v.valor||0));
    const shortCols = cols.map((c,i)=> 'S'+(i+1));

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
        const bg = `rgba(234,88,12,${0.07 + 0.6*pct})`;
        const disp = item && item.nd ? 'ND' : (v ? new Intl.NumberFormat('es-ES').format(Math.round(v)) : '');
        html += `<td class="cell" style="background:${bg}">${disp}</td>`;
      }
      html += '</tr>';
    }
    html += '</tbody></table>';
    el.innerHTML = '<div class="heatwrap">' + html + '</div>';
  }

  bootstrap();
</script>
</body>
</html>
'''
    return render_template_string(template, espectro_colors=espectro_colors)

# ================== APIs ==================
@app.route("/api/bootstrap")
def api_bootstrap():
    df = load_all()
    redes     = sorted(df[COL_RED].dropna().unique().tolist()) if not df.empty else []

    if not df.empty:
        semanas_raw = df["Semana"].dropna().unique().tolist()
        semanas = [w for w in WEEK_ORDER if w in semanas_raw] or sorted(semanas_raw, key=_natural_key)
    else:
        semanas = []

    meses = []
    if not df.empty:
        etiquetas = set(df["Semana"].dropna().astype(str).tolist())
        if any("Sep" in s for s in etiquetas): meses.append("Septiembre")
        if any("Oct" in s for s in etiquetas): meses.append("Octubre")

    espectros = sorted(df[COL_ESPECTRO].dropna().unique().tolist()) if not df.empty else []
    kpis = {
        "filas": len(df),
        "likes": int(df[COL_LIKES].fillna(0).sum()) if COL_LIKES in df else 0,
        "coment": int(df[COL_COMENT].fillna(0).sum()) if COL_COMENT in df else 0,
        "candidatos": df[COL_CANDIDATO].nunique() if not df.empty else 0
    }
    return jsonify({"redes": redes, "semanas": semanas, "meses": meses, "espectros": espectros, "kpis": kpis})

# === BARRAS: usar agregación Semana→Candidato ===
@app.route("/api/likes-por-candidato")
def api_likes_por_candidato():
    df = aplicar_filtros(load_all())
    if df.empty: return jsonify([])
    df = df[pd.to_numeric(df[COL_LIKES], errors="coerce").notna()]
    g = (_agg_semana_then_candidato(df, COL_LIKES)
           .rename(columns={COL_LIKES:"likes"})
           .sort_values("likes", ascending=False))
    out = [{"candidato": r[COL_CANDIDATO], "espectro": r[COL_ESPECTRO],
            "likes": float(r["likes"])} for _, r in g.iterrows()]
    return jsonify(out)

@app.route("/api/comentarios-por_candidato")  # compat viejo
def _deprecated():
    return api_comentarios_por_candidato()

@app.route("/api/comentarios-por-candidato")
def api_comentarios_por_candidato():
    df = aplicar_filtros(load_all())
    if df.empty: return jsonify([])
    df = df[pd.to_numeric(df[COL_COMENT], errors="coerce").notna()]
    g = (_agg_semana_then_candidato(df, COL_COMENT)
           .rename(columns={COL_COMENT:"comentarios"})
           .sort_values("comentarios", ascending=False))
    out = [{"candidato": r[COL_CANDIDATO], "espectro": r[COL_ESPECTRO],
            "comentarios": float(r["comentarios"])} for _, r in g.iterrows()]
    return jsonify(out)

@app.route("/api/candidatos-todos")
def api_candidatos_todos():
    df = aplicar_filtros(load_all())
    if df.empty: return jsonify([])
    df = df[pd.to_numeric(df[COL_LIKES], errors="coerce").notna()]
    g = (_agg_semana_then_candidato(df, COL_LIKES)
           .rename(columns={COL_LIKES:"likes"})
           .sort_values("likes", ascending=False))
    out = [{"candidato": r[COL_CANDIDATO], "espectro": r[COL_ESPECTRO],
            "likes": float(r["likes"])} for _, r in g.iterrows()]
    return jsonify(out)

# === RESTO sin cambios de lógica ===
@app.route("/api/ganador-semanal")
def api_ganador_semanal():
    full = load_all()
    filtered = aplicar_filtros(full)

    if not (request.args.get("semana") or "").strip() and not filtered.empty:
        semanas_presentes = full["Semana"].dropna().unique().tolist()
    else:
        semanas_presentes = filtered["Semana"].dropna().unique().tolist() if not filtered.empty else []
    semanas_dom = [w for w in WEEK_ORDER if w in semanas_presentes] or sorted(semanas_presentes, key=_natural_key)

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
    return jsonify(out)

@app.route("/api/ganador-semanal-series")
def api_ganador_semanal_series():
    filtered = aplicar_filtros(load_all())
    if filtered.empty:
        return jsonify({"semanas": [], "espectros": [], "values": []})

    semanas_presentes = filtered["Semana"].dropna().unique().tolist()
    semanas = [w for w in WEEK_ORDER if w in semanas_presentes] or sorted(semanas_presentes, key=_natural_key)
    espectros = sorted(filtered[COL_ESPECTRO].dropna().unique().tolist())

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
    cols_raw = df["Semana"].dropna().unique().tolist()
    cols = [w for w in WEEK_ORDER if w in cols_raw] or sorted(cols_raw, key=_natural_key)

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

# ====== DIAGNÓSTICO (opcional) ======
@app.route("/api/debug-candidato")
def api_debug_candidato():
    nombre = (request.args.get("candidato") or "").strip()
    if not nombre:
        return jsonify({"error": "falta ?candidato="}), 400
    df = aplicar_filtros(load_all())
    if df.empty:
        return jsonify([])

    sub = (df[df[COL_CANDIDATO] == nombre]
             .drop_duplicates(subset=["Semana", COL_RED], keep="first")
             .sort_values(["Semana", COL_RED]))[["Semana", COL_RED, COL_LIKES, COL_COMENT]]

    out = []
    for _, r in sub.iterrows():
        out.append({
            "semana": r["Semana"], "red": r.get(COL_RED),
            "likes": None if pd.isna(r.get(COL_LIKES)) else float(r.get(COL_LIKES)),
            "comentarios": None if pd.isna(r.get(COL_COMENT)) else float(r.get(COL_COMENT)),
        })
    likes_vals = pd.to_numeric(sub[COL_LIKES], errors="coerce").dropna()
    coment_vals = pd.to_numeric(sub[COL_COMENT], errors="coerce").dropna()
    resumen = {
        "likes_mean": float(likes_vals.mean()) if len(likes_vals) else None,
        "likes_max": float(likes_vals.max()) if len(likes_vals) else None,
        "coment_mean": float(coment_vals.mean()) if len(coment_vals) else None,
        "coment_max": float(coment_vals.max()) if len(coment_vals) else None,
        "n_filas": int(len(sub))
    }
    return jsonify({"filas": out, "resumen": resumen})

@app.route("/api/debug-top-valores")
def api_debug_top_valores():
    df = load_all()
    if df.empty:
        return jsonify([])

    cols = [COL_ESPECTRO, COL_CANDIDATO, COL_RED, "Semana"]
    out = []
    if COL_LIKES in df:
        top_l = (df[cols + [COL_LIKES]].dropna(subset=[COL_LIKES])
                 .sort_values(COL_LIKES, ascending=False).head(50))
        for _, r in top_l.iterrows():
            out.append({"tipo":"likes","candidato":r.get(COL_CANDIDATO),"espectro":r.get(COL_ESPECTRO),
                        "red":r.get(COL_RED),"semana":r.get("Semana"),
                        "valor": float(r.get(COL_LIKES, 0.0))})
    if COL_COMENT in df:
        top_c = (df[cols + [COL_COMENT]].dropna(subset=[COL_COMENT])
                 .sort_values(COL_COMENT, ascending=False).head(50))
        for _, r in top_c.iterrows():
            out.append({"tipo":"comentarios","candidato":r.get(COL_CANDIDATO),"espectro":r.get(COL_ESPECTRO),
                        "red":r.get(COL_RED),"semana":r.get("Semana"),
                        "valor": float(r.get(COL_COMENT, 0.0))})
    return jsonify(out)

# Health-check
@app.route("/health")
def health():
    return "ok", 200

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
