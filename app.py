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
# Si agregaste una columna de equivalencia, ajústala aquí:
PROM_COL_SEMANA_EQ = "Semana web"   # <--- cambia a tu nombre exacto si es distinto

PROM_COL_INTERSEM  = "Candidatos por promedio de interacciones a la semana"
PROM_COL_LIKES     = "Likes promedio candidato"
PROM_COL_COMENT    = "Comentarios promedio candidato"

# === Nombres visibles de semanas (mapeo hoja -> etiqueta canónica) ===
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
    if series is None:
        return series
    s = series.astype(str).str.replace(r"[^\d\.\-eE]", "", regex=True)
    s = s.str.replace(r"(?<=\d)\.(?=\d{3}(\D|$))", "", regex=True)  # separador de miles con punto
    return pd.to_numeric(s, errors="coerce")

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
        if sh.strip() == PROM_SHEET:
            continue
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

    # Limpieza de strings
    for c in [COL_ESPECTRO, COL_CANDIDATO, COL_RED, COL_TEMA, "Semana"]:
        if c in df.columns:
            df[c] = df[c].apply(lambda x: None if not _valid_str(x) else str(x).strip())

    # Numéricos
    if COL_LIKES in df.columns:
        df[COL_LIKES] = _sanitize_numeric(df[COL_LIKES])
    if COL_MAXLIKES in df.columns:
        df[COL_MAXLIKES] = _sanitize_numeric(df[COL_MAXLIKES])
    if COL_COMENT in df.columns:
        df[COL_COMENT] = _sanitize_numeric(df[COL_COMENT])

    # Filtrado base + dedup
    df = df[df[COL_CANDIDATO].notna() & df[COL_RED].notna() & df["Semana"].notna()]
    keys = [COL_CANDIDATO, "Semana", COL_RED] + ([COL_TEMA] if COL_TEMA in df.columns else [])
    df = df.drop_duplicates(subset=[k for k in keys if k in df.columns], keep="first")

    # Interacciones
    df["Interacciones"] = df[COL_LIKES].fillna(0) + df[COL_COMENT].fillna(0)
    return df

def load_all():
    return _load_all_cached(_cache_key())

# ---------- Normalización de SEMANA (tolerante) ----------
def _normalize_week_strict(s: str):
    if not _valid_str(s):
        return None
    s1 = str(s).strip()

    if s1 in WEEK_ORDER:
        return s1

    m = re.search(r"(?:semana|s)\s*([1-7])", s1, flags=re.IGNORECASE)
    if m:
        n = int(m.group(1))
        return WEEK_MAP.get(f"Semana {n}", s1)

    s2 = re.sub(r"\s+", " ", s1)

    m2 = re.match(r"^\s*(\d{1,2})\s*([A-Za-zÁÉÍÓÚÜÑáéíóúüñ]{3,})\s*-\s*(\d{1,2})\s*$", s2)
    if m2:
        d1, mtxt, d2 = m2.groups()
        mtxt = mtxt[:3].title()
        candidate = f"{int(d1)} {mtxt} - {int(d2)} {mtxt}"
        for off in WEEK_ORDER:
            if re.sub(r"\s+", " ", off).lower() == re.sub(r"\s+", " ", candidate).lower():
                return off
        return candidate

    m3 = re.match(r"^\s*\d{1,2}\s*[A-Za-z].*-\s*\d{1,2}\s*[A-Za-z].*\s*$", s2)
    if m3:
        for off in WEEK_ORDER:
            if re.sub(r"\s+", " ", off).lower() == s2.lower():
                return off
        return s2

    return s1

# ---------- CARGA DE LA HOJA DE PROMEDIOS ----------
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

    # Strings
    for c in [PROM_COL_ESPECTRO, PROM_COL_CANDIDATO, PROM_COL_RED, PROM_COL_SEMANA]:
        if c in df.columns:
            df[c] = df[c].apply(lambda x: None if not _valid_str(x) else str(x).strip())

    # Normaliza 'Semana'
    if PROM_COL_SEMANA in df.columns:
        df[PROM_COL_SEMANA] = df[PROM_COL_SEMANA].apply(_normalize_week_strict)

    # Coalesce a semana equivalente con prioridad (si existe)
    if PROM_COL_SEMANA_EQ in df.columns:
        df[PROM_COL_SEMANA_EQ] = df[PROM_COL_SEMANA_EQ].apply(_normalize_week_strict)
        df["_SemanaEff"] = df[PROM_COL_SEMANA_EQ].where(df[PROM_COL_SEMANA_EQ].notna(), df[PROM_COL_SEMANA])
    else:
        df["_SemanaEff"] = df[PROM_COL_SEMANA]

    # Numéricos
    for numc in [PROM_COL_INTERSEM, PROM_COL_LIKES, PROM_COL_COMENT]:
        if numc in df.columns:
            df[numc] = _sanitize_numeric(df[numc])

    df = df[df[PROM_COL_CANDIDATO].notna() & df[PROM_COL_RED].notna()]
    return df

def load_promedios():
    return _load_promedios_cached(_cache_key())

# ---------- Filtros ----------
def _month_abbrev_list(mes_multi):
    abrev = []
    for m in mes_multi:
        ml = m.strip().lower()
        if ml.startswith("sep"): abrev.append("Sep")
        elif ml.startswith("oct"): abrev.append("Oct")
    return abrev

def aplicar_filtros(df):
    red_multi      = _parse_multi((request.args.get("red") or "").strip())
    semana_multi   = _parse_multi((request.args.get("semana") or "").strip())
    espectro_multi = _parse_multi((request.args.get("espectro") or "").strip())
    mes_multi      = _parse_multi((request.args.get("mes") or "").strip())

    if red_multi:
        reds = {r.lower() for r in red_multi}
        df = df[df[COL_RED].astype(str).str.lower().isin(reds)]
    if semana_multi:
        semanas_norm = [_normalize_week_strict(s) for s in semana_multi]
        df = df[df["Semana"].isin(semanas_norm)]
    if espectro_multi:
        esps = {e.lower() for e in espectro_multi}
        df = df[df[COL_ESPECTRO].astype(str).str.lower().isin(esps)]
    if mes_multi:
        abrev = _month_abbrev_list(mes_multi)
        if abrev:
            mask = df["Semana"].astype(str).apply(lambda s: any(a in s for a in abrev))
            df = df[mask]
    return df

def aplicar_filtros_prom(df):
    red_multi      = _parse_multi((request.args.get("red") or "").strip())
    semana_multi   = _parse_multi((request.args.get("semana") or "").strip())
    espectro_multi = _parse_multi((request.args.get("espectro") or "").strip())
    mes_multi      = _parse_multi((request.args.get("mes") or "").strip())

    if red_multi:
        reds = {r.lower() for r in red_multi}
        df = df[df[PROM_COL_RED].astype(str).str.lower().isin(reds)]
    if semana_multi:
        semanas_norm = [_normalize_week_strict(s) for s in semana_multi]
        df = df[df["_SemanaEff"].isin(semanas_norm)]
    if espectro_multi:
        esps = {e.lower() for e in espectro_multi}
        df = df[df[PROM_COL_ESPECTRO].astype(str).str.lower().isin(esps)]
    if mes_multi:
        abrev = _month_abbrev_list(mes_multi)
        if abrev:
            mask = df["_SemanaEff"].astype(str).apply(lambda s: any(a in s for a in abrev))
            df = df[mask]
    return df

# === Promedio de filas por candidato (para hojas semanales) ===
def _mean_of_all_rows(df, value_col):
    if df.empty:
        return pd.DataFrame(columns=[COL_CANDIDATO, COL_ESPECTRO, value_col])
    x = df.copy()
    x[value_col] = pd.to_numeric(x[value_col], errors="coerce")
    x = x[x[value_col].notna()]
    esp_map = (
        x.groupby(COL_CANDIDATO)[COL_ESPECTRO]
         .agg(lambda s: s.mode().iat[0] if not s.mode().empty else s.dropna().iat[0] if s.dropna().size else None)
         .to_dict()
    )
    final = x.groupby(COL_CANDIDATO, as_index=False)[value_col].mean()
    final[COL_ESPECTRO] = final[COL_CANDIDATO].map(esp_map)
    return final

# ---------- Ordenación robusta de semanas según filtros ----------
def _ordered_weeks_from_df(df_weeks_series):
    """Ordena semanas presentes priorizando WEEK_ORDER y luego extras."""
    present = df_weeks_series.dropna().unique().tolist()
    if not present:
        return []
    canon = [w for w in WEEK_ORDER if w in present]
    extras = [w for w in present if w not in canon]
    return canon + sorted(extras, key=_natural_key)

def _metric_column_name(metric_param: str):
    m = (metric_param or "interacciones").strip().lower()
    if m == "likes": return COL_LIKES
    if m == "comentarios": return COL_COMENT
    return "Interacciones"  # default

# ============== APP ==============
app = Flask(__name__)

# ---------- Página ----------
@app.route("/", methods=["GET", "HEAD"])
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

  <!-- (tu HTML/JS original tal cual) -->
  <!-- No se modifica para no romper nada. Los nuevos endpoints están listos en backend. -->
</div>
</body>
</html>
'''
    return render_template_string(template, espectro_colors=espectro_colors)

# ---------- Catch-all seguro ----------
@app.route("/<path:subpath>", methods=["GET", "HEAD"])
def catch_all(subpath):
    sp = subpath.strip().lower()
    if sp.startswith("api/") or sp in {"health", "healthz", "healthcheck"}:
        return ("Not found", 404)
    return index()

# ================== APIs ==================
@app.route("/api/bootstrap")
def api_bootstrap():
    df = load_all()
    redes     = sorted(df[COL_RED].dropna().unique().tolist()) if not df.empty else []

    prom = load_promedios()
    if not prom.empty and "_SemanaEff" in prom.columns:
        semanas_raw = prom["_SemanaEff"].dropna().unique().tolist()
    else:
        semanas_raw = []

    if not semanas_raw and not df.empty:
        semanas_raw = df["Semana"].dropna().unique().tolist()

    present = [w for w in WEEK_ORDER if w in semanas_raw]
    extras  = [w for w in semanas_raw if w not in present]
    semanas = present + sorted(extras, key=_natural_key)

    meses = []
    etiquetas = set(semanas)
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
    out = [{"candidato": r["candidato"], "espectro": r["espectro"], "likes": _r1(r["likes"])} for _, r in g.iterrows()]
    return jsonify(out)

@app.route("/api/comentarios-por-candidato")
def api_comentarios_por_candidato():
    df = aplicar_filtros_prom(load_promedios())
    if df.empty or PROM_COL_COMENT not in df.columns:
        return jsonify([])
    x = df[[PROM_COL_CANDIDATO, PROM_COL_ESPECTRO, PROM_COL_COMENT]].copy()
    x = x[pd.to_numeric(x[PROM_COL_COMENT], errors="coerce").notna()]
    g = (x.groupby(PROM_COL_CANDIDATO, as_index=False)
           .agg({PROM_COL_COMENT: "mean", PROM_COL_ESPECTRO: lambda s: s.mode().iat[0] if not s.mode().empty else s.dropna().iat[0] if s.dropna().size else None})
           .rename(columns={PROM_COL_COMENT: "comentarios", PROM_COL_CANDIDATO: "candidato", PROM_COL_ESPECTRO: "espectro"})
           .sort_values("comentarios", ascending=False))
    out = [{"candidato": r["candidato"], "espectro": r["espectro"], "comentarios": _r1(r["comentarios"])} for _, r in g.iterrows()]
    return jsonify(out)

@app.route("/api/candidatos-todos")
def api_candidatos_todos():
    df = aplicar_filtros_prom(load_promedios())
    if df.empty or PROM_COL_INTERSEM not in df.columns:
        return jsonify([])
    x = df[[PROM_COL_CANDIDATO, PROM_COL_ESPECTRO, PROM_COL_INTERSEM]].copy()
    x = x[pd.to_numeric(x[PROM_COL_INTERSEM], errors="coerce").notna()]
    g = (x.groupby(PROM_COL_CANDIDATO, as_index=False)
           .agg({PROM_COL_INTERSEM: "mean", PROM_COL_ESPECTRO: lambda s: s.mode().iat[0] if not s.mode().empty else s.dropna().iat[0] if s.dropna().size else None})
           .rename(columns={PROM_COL_INTERSEM: "interacciones", PROM_COL_CANDIDATO: "candidato", PROM_COL_ESPECTRO: "espectro"})
           .sort_values("interacciones", ascending=False))
    out = [{"candidato": r["candidato"], "espectro": r["espectro"], "likes": _r1(r["interacciones"])} for _, r in g.iterrows()]
    return jsonify(out)

# === Ganadores / Heatmaps (con hojas semanales) ===
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
    espectros_dom = sorted(full[COL_ESPECTRO].dropna().unique().tolist()) if not espectros_q else \
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
                    "interacciones": _r1(row["Interacciones"]), "nd": False
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
                values.append({"semana": sem, "espectro": esp, "interacciones": _r1(row["Interacciones"]), "nd": False, "candidato": row[COL_CANDIDATO]})
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
                v = _r1(sub["Interacciones"].iloc[0])
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
                values.append({"candidato": r, "semana": c, "valor": _r1(sub[col].iloc[0]), "nd": False})
    return jsonify({"rows": rows, "cols": cols, "values": values})

# ================== NUEVO: VARIACIÓN SEMANAL (Δ) ==================

def _build_pivot_for_metric(df_filtered: pd.DataFrame, metric_col: str):
    """
    Devuelve:
      - pivot: tabla candidatos x semanas con promedio de metric_col
      - semanas_ordenadas: semanas (columnas) ordenadas según filtro/subconjunto presente
      - espectro_por_candidato: mapeo candidato -> espectro (moda)
    """
    if df_filtered.empty:
        return pd.DataFrame(), [], {}

    # Semanas en el subset filtrado (respetando orden canónico + extras)
    semanas_ordenadas = _ordered_weeks_from_df(df_filtered["Semana"])

    # Promedio por candidato x semana (para esa métrica)
    g = (df_filtered.groupby([COL_CANDIDATO, "Semana"], as_index=False)[metric_col]
         .mean())

    pivot = g.pivot_table(index=COL_CANDIDATO, columns="Semana", values=metric_col, aggfunc="mean")
    # Asegura todas las columnas en el orden deseado
    for s in semanas_ordenadas:
        if s not in pivot.columns:
            pivot[s] = pd.NA
    pivot = pivot[semanas_ordenadas]

    # Espectro por candidato (moda dentro del subset filtrado)
    esp_map = (df_filtered.groupby(COL_CANDIDATO)[COL_ESPECTRO]
               .agg(lambda s: s.mode().iat[0] if not s.mode().empty else s.dropna().iat[0] if s.dropna().size else None)
               .to_dict())
    return pivot, semanas_ordenadas, esp_map

@app.route("/api/variacion-semanal")
def api_variacion_semanal():
    """
    Devuelve deltas por candidato entre semanas consecutivas del subconjunto filtrado.
    Responde lista de dicts:
      { candidato, espectro, from_semana, to_semana, delta, nd }
    """
    metric_col = _metric_column_name(request.args.get("metric"))
    df = aplicar_filtros(load_all())
    if df.empty:
        return jsonify([])

    pivot, semanas, esp_map = _build_pivot_for_metric(df, metric_col)
    if not semanas or pivot.empty:
        return jsonify([])

    out = []
    # Calcula Δ entre columnas consecutivas (solo dentro del subconjunto presente/filtrado)
    for i in range(1, len(semanas)):
        s_prev, s_curr = semanas[i-1], semanas[i]
        prev_vals = pd.to_numeric(pivot[s_prev], errors="coerce")
        curr_vals = pd.to_numeric(pivot[s_curr], errors="coerce")
        delta = curr_vals - prev_vals

        for cand, d in delta.items():
            if pd.isna(d):
                out.append({
                    "candidato": cand,
                    "espectro": esp_map.get(cand),
                    "from_semana": s_prev,
                    "to_semana": s_curr,
                    "delta": 0.0,
                    "nd": True
                })
            else:
                out.append({
                    "candidato": cand,
                    "espectro": esp_map.get(cand),
                    "from_semana": s_prev,
                    "to_semana": s_curr,
                    "delta": _r1(d),
                    "nd": False
                })
    # Orden útil: por to_semana y delta desc
    out = sorted(out, key=lambda r: (semanas.index(r["to_semana"]), -(r["delta"] or 0)))
    return jsonify(out)

@app.route("/api/ganador-variacion")
def api_ganador_variacion():
    """
    Para cada ESPECTRO y cada salto consecutivo de semana (S1->S2, S2->S3, ...),
    devuelve el candidato con MAYOR DELTA POSITIVO.
    Si todos los deltas <= 0 en ese espectro/salto, retorna nd=True.
    """
    metric_col = _metric_column_name(request.args.get("metric"))
    df = aplicar_filtros(load_all())
    if df.empty:
        return jsonify([])

    pivot, semanas, esp_map = _build_pivot_for_metric(df, metric_col)
    if not semanas or len(semanas) < 2 or pivot.empty:
        return jsonify([])

    espectros_presentes = sorted(df[COL_ESPECTRO].dropna().unique().tolist())
    out = []
    for i in range(1, len(semanas)):
        s_prev, s_curr = semanas[i-1], semanas[i]
        prev_vals = pd.to_numeric(pivot[s_prev], errors="coerce")
        curr_vals = pd.to_numeric(pivot[s_curr], errors="coerce")
        delta = (curr_vals - prev_vals).rename("delta")

        # tabla para buscar por espectro
        tmp = pd.concat([delta], axis=1)
        tmp["candidato"] = tmp.index
        tmp["espectro"] = tmp["candidato"].map(esp_map)

        for esp in espectros_presentes:
            t = tmp[tmp["espectro"] == esp].copy()
            t = t[pd.to_numeric(t["delta"], errors="coerce").notna()]
            if t.empty:
                out.append({"from_semana": s_prev, "to_semana": s_curr, "espectro": esp,
                            "candidato": None, "delta": 0.0, "nd": True})
                continue
            # mayor delta positivo
            idx = t["delta"].idxmax()
            best = t.loc[idx]
            if float(best["delta"]) <= 0:
                out.append({"from_semana": s_prev, "to_semana": s_curr, "espectro": esp,
                            "candidato": None, "delta": 0.0, "nd": True})
            else:
                out.append({"from_semana": s_prev, "to_semana": s_curr, "espectro": esp,
                            "candidato": best["candidato"], "delta": _r1(best["delta"]), "nd": False})
    return jsonify(out)

@app.route("/api/ganador-variacion-series")
def api_ganador_variacion_series():
    """
    Estructura tipo 'series' para pintar rápido:
    {
      "saltos": [ "S1→S2", "S2→S3", ... ],
      "espectros": [ ... ],
      "values": [
         { "salto":"S1→S2", "espectro":"Derecha", "delta": 123.4, "nd":false, "candidato":"X" },
         ...
      ]
    }
    """
    metric_col = _metric_column_name(request.args.get("metric"))
    df = aplicar_filtros(load_all())
    if df.empty:
        return jsonify({"saltos": [], "espectros": [], "values": []})

    pivot, semanas, esp_map = _build_pivot_for_metric(df, metric_col)
    if not semanas or len(semanas) < 2 or pivot.empty:
        return jsonify({"saltos": [], "espectros": [], "values": []})

    espectros_presentes = sorted(df[COL_ESPECTRO].dropna().unique().tolist())
    saltos = []
    values = []

    for i in range(1, len(semanas)):
        s_prev, s_curr = semanas[i-1], semanas[i]
        label_salto = f"{s_prev}→{s_curr}"
        saltos.append(label_salto)

        prev_vals = pd.to_numeric(pivot[s_prev], errors="coerce")
        curr_vals = pd.to_numeric(pivot[s_curr], errors="coerce")
        delta = (curr_vals - prev_vals).rename("delta")

        tmp = pd.concat([delta], axis=1)
        tmp["candidato"] = tmp.index
        tmp["espectro"] = tmp["candidato"].map(esp_map)

        for esp in espectros_presentes:
            t = tmp[tmp["espectro"] == esp].copy()
            t = t[pd.to_numeric(t["delta"], errors="coerce").notna()]
            if t.empty:
                values.append({"salto": label_salto, "espectro": esp, "delta": 0.0, "nd": True})
                continue
            idx = t["delta"].idxmax()
            best = t.loc[idx]
            if float(best["delta"]) <= 0:
                values.append({"salto": label_salto, "espectro": esp, "delta": 0.0, "nd": True})
            else:
                values.append({"salto": label_salto, "espectro": esp, "delta": _r1(best["delta"]), "nd": False, "candidato": best["candidato"]})

    return jsonify({"saltos": saltos, "espectros": espectros_presentes, "values": values})

# === Health checks para Render ===
@app.route("/health", methods=["GET", "HEAD"])
@app.route("/healthz", methods=["GET", "HEAD"])
@app.route("/healthcheck", methods=["GET", "HEAD"])
def health():
    return ("ok", 200, {"Content-Type": "text/plain; charset=utf-8"})

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)

# Health-check duplicado (se mantiene como en tu archivo original)
@app.route("/health")
def health_dup():
    return "ok", 200

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
