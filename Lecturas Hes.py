import streamlit as st
import pandas as pd
import folium
from streamlit_folium import folium_static
from folium.plugins import Fullscreen  
from sqlalchemy import create_engine
import psycopg2
import json
import urllib.parse
import plotly.express as px
import time

# 1. CONFIGURACIÓN
st.set_page_config(
    page_title="MIAA - Tablero de Consumos",
    page_icon="https://www.miaa.mx/favicon.ico", 
    layout="wide"  
)

# ESTILO CSS OPTIMIZADO
st.markdown("""
    <style>
        .titulo-superior {
            position: fixed; top: 15px; left: 50%; transform: translateX(-50%);
            z-index: 9999999; color: white; font-size: 1.2rem; font-weight: bold;
            pointer-events: none; white-space: nowrap;
        }
        .stApp { background-color: #000000 !important; color: white; }
        section[data-testid="stSidebar"] { background-color: #111111 !important; }
        iframe[title="streamlit_folium.folium_static"] {
            border: 2px solid #444; border-radius: 10px;
        }
        /* Mini gráfico de barras para el tooltip */
        .mini-bar-container {
            display: flex; align-items: flex-end; gap: 2px; height: 40px; 
            background: #222; padding: 5px; border-radius: 3px; margin-top: 5px;
        }
        .mini-bar {
            background: #00d4ff; width: 6px; border-radius: 1px;
        }
    </style>
""", unsafe_allow_html=True)

URL_LOGO_MIAA = "https://raw.githubusercontent.com/Miaa-Aguascalientes/Lecturas-Hes/c45d926ef0e34215c237cd3c7f71f7b97bf9a784/LogoMIAA-BpcVaQaq.svg"

@st.cache_resource
def get_mysql_engine():
    try:
        creds = st.secrets["mysql"]
        conn_str = f"mysql+mysqlconnector://{creds['user']}:{urllib.parse.quote_plus(creds['password'])}@{creds['host']}/{creds['database']}"
        return create_engine(conn_str)
    except Exception as e:
        st.error(f"Error MySQL: {e}"); return None

@st.cache_resource
def get_postgres_conn():
    try: return psycopg2.connect(**st.secrets["postgres"])
    except Exception as e: st.error(f"Error Postgres: {e}"); return None

@st.cache_data(ttl=3600)
def get_sectores_cached():
    conn = get_postgres_conn()
    if not conn: return pd.DataFrame()
    try:
        query = 'SELECT sector, ST_AsGeoJSON(ST_Simplify(ST_Transform(geom, 4326), 0.0001)) AS geojson_data FROM "Sectorizacion"."Sectores_hidr"'
        df = pd.read_sql(query, conn); conn.close(); return df
    except: return pd.DataFrame()

def get_color_logic(nivel, consumo):
    v = float(consumo) if consumo else 0
    colors = {"REGULAR": "#00FF00", "NORMAL": "#32CD32", "BAJO": "#FF8C00", "CERO": "#FFFFFF", "MUY ALTO": "#FF0000", "ALTO": "#B22222"}
    config = {'DOMESTICO A': [5, 10, 15, 30], 'DOMESTICO B': [6, 11, 20, 30], 'DOMESTICO C': [8, 19, 37, 50]}
    lim = config.get(str(nivel).upper(), [5, 10, 15, 30])
    if v <= 0: return colors["CERO"], "CERO"
    if v <= lim[0]: return colors["BAJO"], "BAJO"
    if v <= lim[1]: return colors["REGULAR"], "REGULAR"
    if v <= lim[2]: return colors["NORMAL"], "NORMAL"
    if v <= lim[3]: return colors["ALTO"], "ALTO"
    return colors["MUY ALTO"], "MUY ALTO"

# GENERADOR DE MINI-GRÁFICO HTML (MUCHO MÁS RÁPIDO QUE MATPLOTLIB)
def generar_mini_grafico_html(df_medidor):
    if df_medidor.empty: return ""
    df_sorted = df_medidor.sort_values('Fecha').tail(10) # Últimos 10 días
    max_val = df_sorted['Consumo_diario'].max() if df_sorted['Consumo_diario'].max() > 0 else 1
    
    bars_html = ""
    for _, row in df_sorted.iterrows():
        height = (row['Consumo_diario'] / max_val) * 100
        bars_html += f'<div class="mini-bar" style="height:{height}%;" title="{row["Fecha"]}: {row["Consumo_diario"]}"></div>'
    
    return f'<div class="mini-bar-container">{bars_html}</div>'

# CARGA DE DATOS
mysql_engine = get_mysql_engine()
df_sec = get_sectores_cached()
ahora = pd.Timestamp.now()

with st.sidebar:
    st.image(URL_LOGO_MIAA, use_container_width=True)
    if st.button("♻️ Actualizar Datos", use_container_width=True):
        st.cache_data.clear(); st.cache_resource.clear(); st.rerun()
    
    with st.expander("📅 RANGO DE FECHAS", expanded=True):
        opcion_rango = st.selectbox("Rango", ["Este mes", "Última semana", "Mes pasado", "Personalizado"], index=0)
        default_range = (ahora.replace(day=1), ahora)
        if opcion_rango == "Última semana": default_range = (ahora - pd.Timedelta(days=7), ahora)
        fecha_rango = st.date_input("Periodo", value=default_range, max_value=ahora, format="DD/MM/YYYY")

    if len(fecha_rango) == 2:
        # Carga optimizada: Solo columnas necesarias
        query_hes = f"SELECT Medidor, Fecha, Consumo_diario, Lectura, Latitud, Longitud, Nivel, ClienteID_API, Nombre, Domicilio, Sector, Colonia, Giro FROM HES WHERE Fecha BETWEEN '{fecha_rango[0]}' AND '{fecha_rango[1]}'"
        df_hes = pd.read_sql(query_hes, mysql_engine)
        
        with st.expander("🔍 FILTROS", expanded=False):
            filtros_activos = {}
            for col, nom in {"Medidor": "Medidor", "Colonia": "Colonia", "Sector": "Sector"}.items():
                opts = sorted(df_hes[col].dropna().unique().astype(str))
                sel = st.multiselect(nom, options=opts)
                filtros_activos[col] = sel
                if sel: df_hes = df_hes[df_hes[col].astype(str).isin(sel)]
    else: st.stop()

# PROCESAMIENTO PARA MAPA
agg_rules = {col: 'first' for col in df_hes.columns if col not in ['Medidor', 'Consumo_diario', 'Fecha']}
agg_rules['Consumo_diario'] = 'sum'
agg_rules['Fecha'] = 'max'
df_mapa = df_hes.groupby('Medidor').agg(agg_rules).reset_index()
df_valid = df_mapa[(df_mapa['Latitud'] != 0) & (df_mapa['Latitud'].notnull())]

# DASHBOARD UI
st.markdown('<div class="titulo-superior">Medidores inteligentes - MIAA</div>', unsafe_allow_html=True)
m1, m2, m3, m4 = st.columns(4)
m1.metric("📟 Medidores", f"{len(df_mapa):,}")
m2.metric("💧 Total", f"{df_hes['Consumo_diario'].sum():,.1f} m³")
m3.metric("📈 Promedio", f"{df_hes['Consumo_diario'].mean():.2f} m³")
m4.metric("📋 Lecturas", f"{len(df_hes):,}")

col_map, col_der = st.columns([3, 1.2])

with col_map:
    lat_c, lon_c = (df_valid['Latitud'].mean(), df_valid['Longitud'].mean()) if not df_valid.empty else (21.88, -102.29)
    m = folium.Map(location=[lat_c, lon_c], zoom_start=13, tiles="CartoDB dark_matter", prefer_canvas=True)
    Fullscreen().add_to(m)
    
    # Capa de Sectores (Solo si hay pocos o filtrados para no saturar)
    if not df_sec.empty:
        fg_s = folium.FeatureGroup(name="Sectores")
        for _, row in df_sec.iterrows():
            folium.GeoJson(json.loads(row['geojson_data']), style_function=lambda x: {'color': '#00d4ff', 'weight': 1, 'fillOpacity': 0.05}).add_to(fg_s)
        fg_s.add_to(m)

    # Capa de Medidores
    fg_m = folium.FeatureGroup(name="Medidores")
    for _, r in df_valid.iterrows():
        color, etiq = get_color_logic(r['Nivel'], r['Consumo_diario'])
        
        # Historial para el mini gráfico (optimizado)
        hist_html = generar_mini_grafico_html(df_hes[df_hes['Medidor'] == r['Medidor']])
        
        tooltip_html = f"""
        <div style='font-family:sans-serif; font-size:11px; width:220px; color:#333;'>
            <b style='color:#007bff;'>Medidor: {r['Medidor']}</b><br>
            <b>Consumo:</b> {r['Consumo_diario']:.2f} m³<br>
            <div style='border-left:3px solid {color}; padding-left:5px; margin:5px 0;'><b>Status:</b> {etiq}</div>
            <div style='font-size:9px; color:#666;'>Historial últimos 10 envíos:</div>
            {hist_html}
        </div>
        """
        folium.CircleMarker(
            location=[r['Latitud'], r['Longitud']], radius=4, color=color, fill=True, fill_opacity=0.8,
            tooltip=folium.Tooltip(tooltip_html)
        ).add_to(fg_m)
    
    fg_m.add_to(m)
    folium.LayerControl().add_to(m)
    folium_static(m, width=1000, height=600)

with col_der:
    st.write("📊 **Top Consumos**")
    top10 = df_mapa.sort_values('Consumo_diario', ascending=False).head(10)
    st.dataframe(top10[['Medidor', 'Consumo_diario']], hide_index=True)

# Gráfico Inferior
st.divider()
df_t = df_hes.groupby('Fecha')['Consumo_diario'].sum().reset_index()
st.plotly_chart(px.line(df_t, x='Fecha', y='Consumo_diario', title="Tendencia de Consumo Total"), use_container_width=True)
