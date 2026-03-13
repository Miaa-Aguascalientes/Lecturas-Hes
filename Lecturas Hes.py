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
    page_icon="💧", 
    layout="wide"  
)

# ESTILO CSS
st.markdown("""
    <style>
        .titulo-superior {
            position: fixed;
            top: 15px;
            left: 50%;
            transform: translateX(-50%);
            z-index: 9999999;
            color: white;
            font-size: 1.2rem;
            font-weight: bold;
            line-height: normal;
            pointer-events: none;
            white-space: nowrap;
        }
        [data-testid="stSidebarUserContent"] {
            padding-top: 0rem !important;
        }
        [data-testid="stSidebarUserContent"] img {
            margin-top: -70px !important; 
            max-width: 200px !important;
            margin-left: auto;
            margin-right: auto;
            display: block;
        }
        [data-testid="stSidebarUserContent"] img {
            margin-top: -60px !important;
        }
        .block-container {
            padding-top: 1.8rem !important;
            padding-bottom: 0rem !important;
        }
        div[data-testid="stHorizontalBlock"]:has(div[data-testid="stMetric"]) {
            width: 60% !important;
            gap: 0px !important;
        }
        [data-testid="stMetric"] {
            display: flex;
            flex-direction: column;
            align-items: center;
            text-align: center;
            padding: 2px 0px !important;
        }
        [data-testid="stMetricValue"] {
            font-size: 1.6rem !important;
            font-weight: bold;
            justify-content: center !important;
        }
        [data-testid="stMetricLabel"] {
            justify-content: center !important;
        }
        .stApp { background-color: #000000 !important; color: white; }
        section[data-testid="stSidebar"] { background-color: #111111 !important; }
        
        .map-legend {
            display: flex;
            justify-content: center;
            flex-wrap: wrap;
            gap: 20px;
            padding: 10px;
            background-color: #111111;
            border-radius: 8px;
            margin-top: 5px;
            border: 1px solid #333;
        }
        .legend-item {
            display: flex;
            align-items: center;
            font-size: 12px;
            font-weight: bold;
        }
        .legend-color {
            width: 10px;
            height: 10px;
            border-radius: 50%;
            margin-right: 8px;
        }
    </style>
""", unsafe_allow_html=True)

URL_LOGO_MIAA = "https://raw.githubusercontent.com/Miaa-Aguascalientes/Lecturas-Hes/refs/heads/main/LOGO%20HES.png"

@st.cache_resource
def get_mysql_engine():
    try:
        creds = st.secrets["mysql"]
        user = creds["user"]
        pwd = urllib.parse.quote_plus(creds["password"])
        host = creds["host"]
        db = creds["database"]
        conn_str = f"mysql+mysqlconnector://{user}:{pwd}@{host}/{db}"
        return create_engine(conn_str)
    except Exception as e:
        st.error(f"Error configurando motor MySQL: {e}")
        return None

@st.cache_resource
def get_postgres_conn():
    try:
        return psycopg2.connect(**st.secrets["postgres"])
    except Exception as e:
        st.error(f"Error conectando a Postgres: {e}")
        return None

@st.cache_data(ttl=3600)
def get_sectores_cached():
    conn = get_postgres_conn()
    if conn is None:
        return pd.DataFrame()
    try:
        query = 'SELECT sector, ST_AsGeoJSON(ST_Transform(geom, 4326)) AS geojson_data FROM "Sectorizacion"."Sectores_hidr"'
        df = pd.read_sql(query, conn)
        conn.close()
        return df
    except Exception as e:
        st.sidebar.error(f"Error en consulta Postgres: {e}")
        return pd.DataFrame()

def get_color_logic(nivel, consumo_mes):
    v = float(consumo_mes) if consumo_mes else 0
    colors = {"REGULAR": "#00FF00", "NORMAL": "#32CD32", "BAJO": "#FF8C00", "CERO": "#FFFFFF", "MUY ALTO": "#FF0000", "ALTO": "#B22222", "null": "#0000FF"}
    config = {'DOMESTICO A': [5, 10, 15, 30], 'DOMESTICO B': [6, 11, 20, 30], 'DOMESTICO C': [8, 19, 37, 50]}
    n = str(nivel).upper()
    lim = config.get(n, [5, 10, 15, 30])
    if v <= 0: return colors["CERO"], "CONSUMO CERO"
    if v <= lim[0]: return colors["BAJO"], "CONSUMO BAJO"
    if v <= lim[1]: return colors["REGULAR"], "CONSUMO REGULAR"
    if v <= lim[2]: return colors["NORMAL"], "CONSUMO NORMAL"
    if v <= lim[3]: return colors["ALTO"], "CONSUMO ALTO"
    return colors["MUY ALTO"], "CONSUMO MUY ALTO"

# CARGA DE DATOS
mysql_engine = get_mysql_engine()
df_sec = get_sectores_cached()

ahora = pd.Timestamp.now()
inicio_mes_actual = ahora.replace(day=1)

with st.sidebar:
    st.image(URL_LOGO_MIAA, use_container_width=True)
    st.divider()
    if st.button("♻️ Actualizar Datos", use_container_width=True):
        st.cache_data.clear()
        st.cache_resource.clear()
        st.rerun()
    st.divider()
    
    opcion_rango = st.selectbox("Rango", ["Este mes", "Última semana", "Mes pasado", "Personalizado"], index=0)
    fecha_rango = st.date_input("Periodo", value=(inicio_mes_actual, ahora), max_value=ahora, format="DD/MM/YYYY")
    
    if len(fecha_rango) == 2:
        df_hes = pd.read_sql(f"SELECT * FROM HES WHERE Fecha BETWEEN '{fecha_rango[0]}' AND '{fecha_rango[1]}'", mysql_engine)
        # Filtros rápidos
        for col in ["Medidor", "Colonia", "Sector"]:
            if col in df_hes.columns:
                sel = st.multiselect(col, sorted(df_hes[col].unique().astype(str).tolist()))
                if sel: df_hes = df_hes[df_hes[col].astype(str).isin(sel)]
    else:
        st.stop()

# PROCESAMIENTO
mapeo = {'Consumo_diario': 'sum', 'Lectura': 'last', 'Latitud': 'first', 'Longitud': 'first', 'Nivel': 'first', 
         'ClienteID_API': 'first', 'Nombre': 'first', 'Predio': 'first', 'Domicilio': 'first', 
         'Colonia': 'first', 'Sector': 'first', 'Fecha': 'last'}
agg_safe = {k: v for k, v in mapeo.items() if k in df_hes.columns}
df_mapa = df_hes.groupby('Medidor').agg(agg_safe).reset_index()

# DASHBOARD
st.markdown('<div class="titulo-superior">Medidores inteligentes - Tablero de consumos</div>', unsafe_allow_html=True)

# Indicadores
m1, m2, m3, m4 = st.columns(4)
m1.metric("📟 N° de medidores", f"{len(df_mapa):,}")
m2.metric("💧 Consumo total", f"{df_hes['Consumo_diario'].sum():,.1f} m³")
m3.metric("📈 Promedio diario", f"{df_hes['Consumo_diario'].mean():.2f} m³")
m4.metric("📋 Total lecturas", f"{len(df_hes):,}")

# LADO A LADO
col_mapa, col_tabla = st.columns([5.5, 1])

with col_mapa:
    m = folium.Map(location=[21.8853, -102.2916], zoom_start=12, tiles="CartoDB dark_matter")
    Fullscreen(position="topright").add_to(m)
    
    if not df_sec.empty:
        for _, row in df_sec.iterrows():
            folium.GeoJson(json.loads(row['geojson_data']), style_function=lambda x: {'fillColor': '#00d4ff', 'color': '#00d4ff', 'weight': 1, 'fillOpacity': 0.1}).add_to(m)

    for _, r in df_mapa.iterrows():
        if pd.notnull(r['Latitud']) and pd.notnull(r['Longitud']):
            color_hex, etiqueta = get_color_logic(r.get('Nivel'), r.get('Consumo_diario', 0))
            
            # --- REINTEGRACIÓN DEL POPUP (TOOLTIP) ---
            tooltip_html = f"""
            <div style='font-family: Arial, sans-serif; font-size: 12px; color: #333; padding: 10px; white-space: nowrap;'>
                <h5 style='margin:0 0 5px 0; color: #007bff; border-bottom: 1px solid #ccc;'>Detalle: {r['Medidor']}</h5>
                <b>Cliente:</b> {r.get('ClienteID_API', 'N/A')}<br>
                <b>Lectura:</b> {r.get('Lectura', 0):,.2f} m³<br>
                <b>Consumo:</b> {r.get('Consumo_diario', 0):,.2f} m³<br>
                <b>Ubicación:</b> {r.get('Colonia', 'N/A')}<br>
                <div style='margin-top:5px; padding:3px; background:{color_hex}22; border:1px solid {color_hex}; text-align:center;'>
                    <b style='color:{color_hex};'>{etiqueta}</b>
                </div>
            </div>
            """
            folium.CircleMarker(
                location=[r['Latitud'], r['Longitud']], 
                radius=3, color=color_hex, fill=True, fill_opacity=0.9,
                tooltip=folium.Tooltip(tooltip_html, sticky=True)
            ).add_to(m)
    
    folium_static(m, width=1100, height=650)

    st.markdown("""
        <div class="map-legend">
            <div class="legend-item"><div class="legend-color" style="background-color: #00FF00;"></div>REGULAR</div>
            <div class="legend-item"><div class="legend-color" style="background-color: #32CD32;"></div>NORMAL</div>
            <div class="legend-item"><div class="legend-color" style="background-color: #FF8C00;"></div>BAJO</div>
            <div class="legend-item"><div class="legend-color" style="background-color: #FFFFFF;"></div>CERO</div>
            <div class="legend-item"><div class="legend-color" style="background-color: #FF0000;"></div>MUY ALTO</div>
        </div>
    """, unsafe_allow_html=True)

with col_tabla:
    st.markdown("<p style='color:#00d4ff; font-weight:bold;'>🟢 Histórico</p>", unsafe_allow_html=True)
    if not df_hes.empty:
        st.dataframe(df_hes[['Fecha', 'Lectura', 'Consumo_diario']].tail(20).sort_values(by='Fecha', ascending=False), 
                     hide_index=True, height=620)

# GRÁFICOS
st.divider()
# Gráfico Diario
df_d = df_hes.groupby('Fecha')['Consumo_diario'].sum().reset_index()
fig1 = px.bar(df_d, x='Fecha', y='Consumo_diario', text_auto=',.2f', title="Consumo Diario Total")
fig1.update_layout(paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)', font_color="white")
fig1.update_yaxes(tickformat=",")
st.plotly_chart(fig1, use_container_width=True)

# Gráfico Medidores (Todos)
df_m = df_mapa.sort_values(by='Consumo_diario', ascending=False)
fig2 = px.bar(df_m, x='Medidor', y='Consumo_diario', title="Consumo por Medidor (Completo)")
fig2.update_layout(paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)', font_color="white")
fig2.update_yaxes(tickformat=",")
fig2.update_xaxes(type='category')
st.plotly_chart(fig2, use_container_width=True)
