import os
import re
import pandas as pd
from flask import Flask, jsonify, request, render_template_string
from functools import lru_cache

# === Ruta del Excel ===
EXCEL_PATH = os.environ.get("EXCEL_PATH", "Monitoreo_de_candidatos_largo.xlsx")

# === Columnas del Excel (hojas semanales) ===
COL_ESPECTRO   = "Espectro"
COL_CANDIDATO  = "Candidato"
COL_RED        = "Red Social"
COL_LIKES      = "Promedio likes x semana"
COL_MAXLIKES   = "Publicación con más likes"
COL_TEMA       = "Tema"
COL_COMENT     = "Promedio comentarios  por publicación"

# === Hoja y columnas de la hoja de promedios ===
PROM_SHEET = "Promedios totales candidato"
PROM_COL_ESPECTRO  = "Espectro"
PROM_COL_CANDIDATO = "Candidato"
PROM_COL_RED       = "Red Social"
PROM_COL_SEMANA    = "Semana"
PROM_COL_INTERSEM  = "Candidatos por promedio de interacciones a la semana"
PROM_COL_LIKES     = "Likes promedio candidato"
PROM_COL_COMENT    = "Comentarios promedio candidato"

# === Nombres visibles de semanas (mapeo de hoja -> etiqueta) ===
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
    if pd.isna(x): return False
    s = str(x).strip().lower()
    return not (s == "" or s in {"nan", "none", "null"})

def _sanitize_numeric(series: pd.Series) -> pd.Series:
    """
    Limpia cada celda: quita espacios, comas y cualquier símbolo no numérico,
    conservando solo dígitos, punto decimal y notación científica.
    Ej: "19,000,000 " -> "19000000" ; " 13 121 " -> "13121"
    """
    if series is None:
        return series
    s = series.astype(str).str.replace(r"[^\d\.\-eE]", "", regex=True)
    # Quita puntos "dobles" accidentales tipo "13.121.0" -> "13121.0"
    s = s.str.replace(r"(?<=\d)\.(?=\d{3}(\D|$))", "", regex=True)  # quita puntos de miles si los hubiera
    return pd.to_numeric(s, errors="coerce")

# Redondeo a 1 decimal
def _r1(x):
    try:
        return round(float(x), 1)
    except Exception:
        return None

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
        # Saltar la hoja de promedios en el loader principal
        if sh.strip() == PROM_SHEET:
            continue
        df = pd.read_excel(EXCEL_PATH, sheet_name=sh)
        if df.empty or df.dropna(how="all").empty:
            continue
        etiqueta = WEEK_MAP.get(sh, sh)  # si no está mapeada, deja el nombre tal cual
        df["Semana"] = etiqueta
        frames.append(df)

    if not frames:
        cols = [COL_ESPECTRO, COL_CANDIDATO, COL_RED, COL_LIKES, COL_MAXLIKES, COL_TEMA, COL_COMENT, "Semana"]
        return pd.DataFrame(columns=cols)

    df = pd.concat(frames, ignore_index=True)

    # Limpieza de strings (sin convertir NaN a "nan")
    for c in [COL_ESPECTRO, COL_CANDIDATO, COL_RED, COL_TEMA, "Semana"]:
        if c in df.columns:
            df[c] = df[c].apply(lambda x: None if not _valid_str(x) else str(x).strip())

    # Tipos numéricos con sanitización
    if COL_LIKES in df.columns:
        df[COL_LIKES] = _sanitize_numeric(df[COL_LIKES])
    if COL_MAXLIKES in df.columns:
        df[COL_MAXLIKES] = _sanitize_numeric(df[COL_MAXLIKES])
    if COL_COMENT in df.columns:
        df[COL_COMENT] = _sanitize_numeric(df[COL_COMENT])

    # Filtrar filas inválidas base
    df = df[df[COL_CANDIDATO].notna() & df[COL_RED].notna() & df["Semana"].notna()]

    # DEDUP fuerte para evitar duplicados accidentales: (Candidato, Semana, Red, Tema) si Tema existe
    keys = [COL_CANDIDATO, "Semana", COL_RED] + ([COL_TEMA] if COL_TEMA in df.columns else [])
    df = df.drop_duplicates(subset=[k for k in keys if k in df.columns], keep="first")

    # Interacciones = likes + comentarios (para heatmaps/ganadores)
    df["Interacciones"] = df[COL_LIKES].fillna(0) + df[COL_COMENT].fillna(0)
    return df

def load_all():
    return _load_all_cached(_cache_key())

# ---------- CARGA DE LA HOJA DE PROMEDIOS (con cache) ----------
@lru_cache(maxsize=1)
def _load_promedios_cached(_key):
    if not os.path.exists(EXCEL_PATH):
        cols = [PROM_COL_ESPECTRO, PROM_COL_CANDIDATO, PROM_COL_RED,
                PROM_COL_SEMANA, PROM_COL_INTERSEM, PROM_COL_LIKES, PROM_COL_COMENT]
        return pd.DataFrame(columns=cols)

    try:
        df = pd.read_excel(EXCEL_PATH, sheet_name=PROM_SHEET)
    except Exception:
        cols = [PROM_COL_ESPECTRO, PROM_COL_CANDIDATO, PROM_COL_RED,
                PROM_COL_SEMANA, PROM_COL_INTERSEM, PROM_COL_LIKES, PROM_COL_COMENT]
        return pd.DataFrame(columns=cols)

    # Limpieza básica
    for c in [PROM_COL_ESPECTRO, PROM_COL_CANDIDATO, PROM_COL_RED, PROM_COL_SEMANA]:
        if c in df.columns:
            df[c] = df[c].apply(lambda x: None if not _valid_str(x) else str(x).strip())

    for numc in [PROM_COL_INTERSEM, PROM_COL_LIKES, PROM_COL_COMENT]:
        if numc in df.columns:
            df[numc] = _sanitize_numeric(df[numc])

    # Filas válidas mínimas
    need = [PROM_COL_CANDIDATO, PROM_COL_RED, PROM_COL_SEMANA]
    for n in need:
        if n not in df.columns:
            df[n] = None
    df = df[df[PROM_COL_CANDIDATO].notna() & df[PROM_COL_RED].notna() & df[PROM_COL_SEMANA].notna()]
    return df

def load_promedios():
    return _load_promedios_cached(_cache_key())

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

def aplicar_filtros_prom(df):
    # mismos filtros que aplicar_filtros(), pero con columnas de la hoja de promedios
    red_multi      = _parse_multi((request.args.get("red") or "").strip())
    semana_multi   = _parse_multi((request.args.get("semana") or "").strip())
    espectro_multi = _parse_multi((request.args.get("espectro") or "").strip())
    mes_multi      = _parse_multi((request.args.get("mes") or "").strip())

    if red_multi:
        df = df[df[PROM_COL_RED].isin(red_multi)]
    if semana_multi:
        df = df[df[PROM_COL_SEMANA].isin(semana_multi)]
    if espectro_multi:
        df = df[df[PROM_COL_ESPECTRO].isin(espectro_multi)]
    if mes_multi:
        abrev = []
        for m in mes_multi:
            ml = m.strip().lower()
            if ml.startswith("sep"): abrev.append("Sep")
            elif ml.startswith("oct"): abrev.append("Oct")
        if abrev:
            mask = df[PROM_COL_SEMANA].astype(str).apply(lambda s: any(a in s for a in abrev))
            df = df[mask]
    return df

# === EXACTAMENTE TU REGLA: promedio simple de TODAS las filas por candidato (loader semanal) ===
def _mean_of_all_rows(df, value_col):
    """
    Para cada candidato (y su espectro), toma el promedio simple de TODAS las filas
    que pasen los filtros (todas las redes × todas las semanas).
    """
    if df.empty:
        return pd.DataFrame(columns=[COL_CANDIDATO, COL_ESPECTRO, value_col])

    x = df.copy()
    x[value_col] = pd.to_numeric(x[value_col], errors="coerce")
    x = x[x[value_col].notna()]

    # Espectro estable por candidato (modo en el df filtrado)
    esp_map = (
        x.groupby(COL_CANDIDATO)[COL_ESPECTRO]
         .agg(lambda s: s.mode().iat[0] if not s.mode().empty else (s.dropna().iat[0] if s.dropna().size else None))
         .to_dict()
    )

    final = (
        x.groupby(COL_CANDIDATO, as_index=False)[value_col]
         .mean()
         .rename(columns={value_col: value_col})
    )
    final[COL_ESPECTRO] = final[COL_CANDIDATO].map(esp_map)
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

  @media (max_width:1200px) { .grid3 { grid-template-columns: 1fr; } .cards { grid-template-columns: 1fr 1fr; } }
  @media (max-width: 768px){ .container{ padding:18px 12px 28px; } .heatwrap th, .heatwrap td{ font-size: 11px; } }
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
        <h3>Candidatos por promedio de interacciones a la semana (según filtros)</h3>
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

  function drawChart(ctx, cfg, key){ if (CH[key]) { try { CH[key].destroy(); } catch(e){} } CH[key] = new Chart(ctx, cfg); return CH[key]; }
  function qs(name){ const u=new URL(window.location.href); return u.searchParams.get(name)||""; }
  function qsmulti(name){ const v=qs(name); return v? v.split(",").map(s=>s.trim()).filter(Boolean) : []; }

  function renderChips(containerId, items, qsParam){
    const cont = document.getElementById(containerId);
    const sel = new Set(qsmulti(qsParam));
    cont.innerHTML = items.map(v => {
      const checked = sel.has(v) ? 'checked' : '';
      return `<label><input type="checkbox" name="${qsParam}" value="${v}" ${checked} /><span>${v}</span></label>`;
    }).join('');
  }
  function getChipValues(name){ return Array.from(document.querySelectorAll('input[type=checkbox][name="'+name+'"]:checked')).map(i=>i.value); }

  function setDynamicHeight(id,count){
    const c = document.getElementById(id);
    const espectroFiltrado = qsmulti('espectro').length > 0;
    const rowHeight = espectroFiltrado ? 26 : 28;
    const padding   = 40;
    const rows = Math.max(count || 1, 1);
    const h = Math.max(180, Math.min(rows * rowHeight + padding, 600));
    c.height = h; c.width = (c.parentElement && c.parentElement.clientWidth) ? c.parentElement.clientWidth : 800;
  }
  function colorsBySpectro(arr, espectros) { return arr.map((_,i)=> ESPECTRO_COLORS[espectros[i]] || "rgba(107,114,128,0.35)"); }
  function colorsByCandidate(n) { return Array.from({length:n}, (_,i)=> PALETTE[i % PALETTE.length]); }

  async function bootstrap(){
    const boot = await fetch('/api/bootstrap').then(r=>r.json());
    REDES = boot.redes || []; SEMANAS = boot.semanas || []; MESES = boot.meses || []; ESPECTROS = boot.espectros || [];
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
    const reds = qsmulti('red'), esps = qsmulti('espectro'), weeks = qsmulti('semana'), months = qsmulti('mes');
    if(reds.length) params.set('red', reds.join(',')); if(esps.length) params.set('espectro', esps.join(','));
    if(weeks.length) params.set('semana', weeks.join(',')); if(months.length) params.set('mes', months.join(','));
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

    const baseOpts = { indexAxis:'y', responsive:false, maintainAspectRatio:false, animation:false,
      plugins: { legend: { display:false } }, scales: { y: { ticks: { autoSkip:false } }, x:{ ticks:{ maxTicksLimit: 8 } } } };
    const espectroOn = qsmulti('espectro').length>0;
    const barCfg = { barThickness: espectroOn ? 16 : 20, categoryPercentage: 0.9, barPercentage: 0.9 };

    // Likes
    drawChart(document.getElementById('likesPorCandidato').getContext('2d'), {
      type: 'bar',
      data: { labels: likesCand.map(d=>d.candidato),
              datasets: [{ label: 'Likes promedio', data: likesCand.map(d=>d.likes),
                backgroundColor: espectroOn ? colorsBySpectro(likesCand, likesCand.map(d=>d.espectro)) : colorsByCandidate(likesCand.length),
                ...barCfg }] },
      options: baseOpts
    }, 'likes');

    // Comentarios
    drawChart(document.getElementById('comentPorCandidato').getContext('2d'), {
      type: 'bar',
      data: { labels: comCand.map(d=>d.candidato),
              datasets: [{ label: 'Comentarios promedio', data: comCand.map(d=>d.comentarios),
                backgroundColor: espectroOn ? colorsBySpectro(comCand, comCand.map(d=>d.espectro)) : colorsByCandidate(comCand.length),
                ...barCfg }] },
      options: baseOpts
    }, 'coment');

    // Promedio de interacciones a la semana (reusamos la tercera tarjeta)
    drawChart(document.getElementById('candidatosTodos').getContext('2d'), {
      type: 'bar',
      data: { labels: todos.map(d=>d.candidato),
              datasets: [{ label: 'Interacciones promedio/semana', data: todos.map(d=>d.likes),  // "likes" contiene interacciones aquí
                backgroundColor: espectroOn ? colorsBySpectro(todos, todos.map(d=>d.espectro)) : colorsByCandidate(todos.length),
                ...barCfg }] },
      options: baseOpts
    }, 'todos');

    // Ganadores (etiquetas S{n}. Nombre si hay 1 espectro)
    const canvasStack = document.getElementById('ganadoresStack');
    const ctxStack = canvasStack.getContext('2d');
    const espsSel = qsmulti('espectro');
    const fmt = (v) => new Intl.NumberFormat('es-ES').format(Math.round(v||0));

    if (espsSel.length === 1) {
      const esp = espsSel[0];
      const w = winners.filter(x => x.espectro === esp).sort((a,b) => SEMANAS.indexOf(a.semana) - SEMANAS.indexOf(b.semana));
      const labels = w.map(x => { const idx = SEMANAS.indexOf(x.semana); const p = idx>=0?`S${idx+1}. `:''; return `${p}${x.candidato || 'ND'}`; });
      const data   = w.map(x => x.nd ? 0 : x.interacciones);
      setDynamicHeight('ganadoresStack', labels.length);
      drawChart(ctxStack, {
        type:'bar',
        data:{ labels, datasets:[{ label:esp, data,
          backgroundColor: ESPECTRO_COLORS[esp] || 'rgba(107,114,128,0.35)', borderColor: ESPECTRO_COLORS[esp] || 'rgba(107,114,128,0.55)',
          borderWidth:1, barThickness:18, categoryPercentage:0.9, barPercentage:0.9 }] },
        options:{ indexAxis:'y', responsive:false, maintainAspectRatio:false, animation:false,
          plugins:{ legend:{ display:false }, tooltip:{ callbacks:{
            title:(items)=>{const i=items[0].dataIndex; const sem=w[i]?.semana||''; return sem?`${sem}`:items[0].label; },
            label:(ctx)=> new Intl.NumberFormat('es-ES', {minimumFractionDigits:1, maximumFractionDigits:1}).format(ctx.raw)+' interacciones' } } },
          scales:{ x:{ ticks:{ maxTicksLimit:8 } }, y:{ ticks:{ autoSkip:false }, title:{ display:true, text:'Interacciones' } } } }
      }, 'winners');
    } else {
      setDynamicHeight('ganadoresStack', (qsmulti('espectro').length || 3) * (SEMANAS.length || 6));
      const stackDatasets = (winSeries.espectros || []).map(esp => ({
        label: esp,
        data: (winSeries.semanas || []).map(sem => {
          const cell = (winSeries.values || []).find(v => v.espectro===esp && v.semana===sem);
          return cell ? (cell.nd? 0 : cell.interacciones) : 0;
        }),
        backgroundColor: ESPECTRO_COLORS[esp] || 'rgba(107,114,128,0.35)', borderColor: ESPECTRO_COLORS[esp] || 'rgba(107,114,128,0.55)',
        borderWidth: 0, barThickness: 18, categoryPercentage: 0.9, barPercentage: 0.9
      }));
      drawChart(ctxStack, {
        type:'bar', data:{ labels:(winSeries.semanas||[]).map((s,i)=>'S'+(i+1)), datasets:stackDatasets },
        options:{ indexAxis:'x', responsive:false, maintainAspectRatio:false, animation:false, plugins:{ legend:{ position:'top' } },
          scales:{ x:{ stacked:true, ticks:{ autoSkip:false } }, y:{ stacked:true, title:{ display:true, text:'Interacciones (ganador por espectro)' } } } }
      }, 'winners');
    }

    // Heatmap general
    const hm = document.getElementById('heatmap');
    if(!matrix.values.length) { hm.innerHTML = '<em>Sin datos.</em>'; }
    else {
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
          const disp = item && item.nd ? 'ND' : (v ? new Intl.NumberFormat('es-ES', {minimumFractionDigits:1, maximumFractionDigits:1}).format(v) : '');
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
    const reds = getChipValues('red'); const esps = getChipValues('espectro');
    const weeks = getChipValues('semana'); const months = getChipValues('mes');
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
    const reds = qsmulti('red'), esps = qsmulti('espectro'), weeks = qsmulti('semana'), months = qsmulti('mes');
    if(reds.length) params.set('red', reds.join(',')); if(esps.length) params.set('espectro', esps.join(','));
    if(weeks.length) params.set('semana', weeks.join(',')); if(months.length) params.set('mes', months.join(','));
    params.set('metric', metric);

    const m = await fetch('/api/heatmap-semanal?'+params.toString()).then(r=>r.json());
    const el = document.getElementById('heatmapSemanal');
    if(!m.values.length){ el.innerHTML = '<em>Sin datos para los filtros/semana.</em>'; return; }
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
        const disp = item && item.nd ? 'ND' : (v ? new Intl.NumberFormat('es-ES', {minimumFractionDigits:1, maximumFractionDigits:1}).format(v) : '');
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

# === BARRAS usando HOJA DE PROMEDIOS ===
@app.route("/api/likes-por-candidato")
def api_likes_por_candidato():
    df = aplicar_filtros_prom(load_promedios())
    if df.empty or PROM_COL_LIKES not in df.columns:
        return jsonify([])

    x = df[[PROM_COL_CANDIDATO, PROM_COL_ESPECTRO, PROM_COL_LIKES]].copy()
    x = x[pd.to_numeric(x[PROM_COL_LIKES], errors="coerce").notna()]
    g = (x.groupby(PROM_COL_CANDIDATO, as_index=False)
           .agg({PROM_COL_LIKES: "mean", PROM_COL_ESPECTRO: lambda s: s.mode().iat[0] if not s.mode().empty else s.dropna().iat[0] if s.dropna().size else None})
           .rename(columns={PROM_COL_LIKES: "likes", PROM_COL_CANDIDATO: "candidato", PROM_COL_ESPECTRO: "espectro"})
           .sort_values("likes", ascending=False))
    out = [
        {"candidato": r["candidato"], "espectro": r["espectro"], "likes": _r1(r["likes"])}
        for _, r in g.iterrows()
    ]
    return jsonify(out)

