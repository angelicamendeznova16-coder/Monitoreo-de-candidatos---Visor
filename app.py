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
    if series is None:
        return series
    s = series.astype(str).str.replace(r"[^\d\.\-eE]", "", regex=True)
    s = s.str.replace(r"(?<=\d)\.(?=\d{3}(\D|$))", "", regex=True)
    return pd.to_numeric(s, errors="coerce")

def _r1(x):
    """Redondea a 1 decimal"""
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

    for c in [COL_ESPECTRO, COL_CANDIDATO, COL_RED, COL_TEMA, "Semana"]:
        if c in df.columns:
            df[c] = df[c].apply(lambda x: None if not _valid_str(x) else str(x).strip())

    if COL_LIKES in df.columns:
        df[COL_LIKES] = _sanitize_numeric(df[COL_LIKES])
    if COL_MAXLIKES in df.columns:
        df[COL_MAXLIKES] = _sanitize_numeric(df[COL_MAXLIKES])
    if COL_COMENT in df.columns:
        df[COL_COMENT] = _sanitize_numeric(df[COL_COMENT])

    df = df[df[COL_CANDIDATO].notna() & df[COL_RED].notna() & df["Semana"].notna()]
    keys = [COL_CANDIDATO, "Semana", COL_RED] + ([COL_TEMA] if COL_TEMA in df.columns else [])
    df = df.drop_duplicates(subset=[k for k in keys if k in df.columns], keep="first")

    df["Interacciones"] = df[COL_LIKES].fillna(0) + df[COL_COMENT].fillna(0)
    return df

def load_all():
    return _load_all_cached(_cache_key())

# ---------- NUEVA FUNCIÓN de normalización de semanas ----------
def _normalize_week_label(v: str) -> str:
    """
    Normaliza cualquier variante de 'Semana N' (p. ej. 'Semana2', 'S-03', 'SEMANA 4')
    a la etiqueta oficial (p. ej. '15 Sep - 21 Sep').
    Si ya viene en formato '7 Sep - 14 Sep', se deja tal cual.
    """
    if not _valid_str(v):
        return None

    s = str(v).strip()

    # Ya es etiqueta final
    if s in WEEK_ORDER:
        return s

    # Coincidencia directa
    k = re.sub(r"\s+", " ", s).strip().title()
    if k in WEEK_MAP:
        return WEEK_MAP[k]

    # Buscar número de semana
    m = re.search(r"(\d+)", s)
    if m:
        n = int(m.group(1))
        key = f"Semana {n}"
        if key in WEEK_MAP:
            return WEEK_MAP[key]

    return s

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

    for c in [PROM_COL_ESPECTRO, PROM_COL_CANDIDATO, PROM_COL_RED, PROM_COL_SEMANA]:
        if c in df.columns:
            df[c] = df[c].apply(lambda x: None if not _valid_str(x) else str(x).strip())

    # Normaliza las semanas
    if PROM_COL_SEMANA in df.columns:
        df[PROM_COL_SEMANA] = df[PROM_COL_SEMANA].apply(_normalize_week_label)

    for numc in [PROM_COL_INTERSEM, PROM_COL_LIKES, PROM_COL_COMENT]:
        if numc in df.columns:
            df[numc] = _sanitize_numeric(df[numc])

    df = df[df[PROM_COL_CANDIDATO].notna() & df[PROM_COL_RED].notna() & df[PROM_COL_SEMANA].notna()]
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
        df = df[df["Semana"].isin(semana_multi)]
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
        df = df[df[PROM_COL_SEMANA].isin(semana_multi)]
    if espectro_multi:
        esps = {e.lower() for e in espectro_multi}
        df = df[df[PROM_COL_ESPECTRO].astype(str).str.lower().isin(esps)]
    if mes_multi:
        abrev = _month_abbrev_list(mes_multi)
        if abrev:
            mask = df[PROM_COL_SEMANA].astype(str).apply(lambda s: any(a in s for a in abrev))
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

# ============== APP ==============
app = Flask(__name__)

# === Rutas principales simplificadas (solo APIs relevantes + health) ===
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

# === Health checks para Render ===
@app.route("/health", methods=["GET", "HEAD"])
@app.route("/healthz", methods=["GET", "HEAD"])
@app.route("/healthcheck", methods=["GET", "HEAD"])
def health():
    return ("ok", 200, {"Content-Type": "text/plain; charset=utf-8"})

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
