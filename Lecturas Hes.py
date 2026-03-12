import streamlit as st
import pandas as pd
import folium
from streamlit_folium import st_folium
from folium.plugins import Fullscreen
from sqlalchemy import create_engine
import psycopg2
import json
import urllib.parse
import plotly.express as px
import time

# 1. CONFIGURACIÓN
st.set_page_config(page_title="MIAA - Tablero de Consumos", layout="wide")

# ESTILO CSS (Indicadores más chicos y bordes cian)
st.markdown("""
    <style>
        .stApp { background-color: #000000 !important; color: white; }
        section[data-testid="stSidebar"] { background-color: #0c0c0c !important; border-right: 1px solid #00d4ff; }
        
        /* Contenedor de indicadores compactos */
        .metric-row {
            display: flex;
            justify-content: space-around;
            background-color: #000;
            border: 1px solid #00d4ff;
            padding: 5px;
            margin-bottom: 10px;
        }
        .metric-box { 
            display: flex; 
            align-items: center; 
            justify-content: center;
            border-right: 1px solid #333; 
            flex: 1;
            padding: 2px 10px;
        }
        .metric-box:last-child { border-right: none; }
        .metric-content { text-align: left; }
        .metric-label { font-size: 11px; color: #ccc; margin: 0; padding: 0; line-height: 1; }
        .metric-value { font-size: 18px; font-weight: bold; color: white; margin: 0; padding: 0; }
        .metric-icon { width: 24px; height: 24px; margin-right: 8px; }

        /* Ajustes Sidebar */
        [data-testid="stSidebarUserContent"] div[data-testid="stVerticalBlock"] > div {
            padding-bottom: 0px !important; padding-top: 0px !important; margin-bottom: -5px !important;
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
    except: return None

@st.cache_resource
def get_postgres_conn():
    try: return psycopg2.connect(**st.secrets["postgres"])
    except: return None

@st.cache_data(ttl=3600)
def get_sectores_cached():
    conn = get_postgres_conn()
    if conn is None: return pd.DataFrame()
    try:
        query = 'SELECT sector, ST_AsGeoJSON(ST_Transform(geom, 4326)) AS geojson_data FROM "Sectorizacion"."Sectores_hidr"'
        df = pd.read_sql(query, conn)
        conn.close()
        return df
    except: return pd.DataFrame()

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

mysql_engine = get_mysql_engine()
df_sec = get_sectores_cached()

with st.sidebar:
    st.image(URL_LOGO_MIAA, use_container_width=True)
    st.divider()
    if st.button("♻️ Actualizar / Despertar", use_container_width=True):
        st.cache_data.clear()
        st.rerun()
    st.divider()
    ahora = pd.Timestamp.now()
    fecha_rango = st.date_input("Periodo de consulta", value=(ahora.replace(day=1), ahora))
    
    if len(fecha_rango) == 2:
        df_hes = pd.read_sql(f"SELECT * FROM HES WHERE Fecha BETWEEN '{fecha_rango[0]}' AND '{fecha_rango[1]}'", mysql_engine)
        for col in ["ClienteID_API", "Metodoid_API", "Medidor", "Predio", "Colonia", "Giro", "Sector"]:
            if col in df_hes.columns:
                opciones = sorted(df_hes[col].unique().astype(str).tolist())
                sel = st.multiselect(col, opciones, key=f"s_{col}")
                if sel: df_hes = df_hes[df_hes[col].astype(str).isin(sel)]

# PROCESAMIENTO
agg_map = {col: func for col, func in {
    'Consumo_diario': 'sum', 'Lectura': 'last', 'Latitud': 'first', 'Longitud': 'first',
    'Nivel': 'first', 'Nombre': 'first', 'Predio': 'first', 'Domicilio': 'first',
    'Colonia': 'first', 'Giro': 'first', 'Sector': 'first', 'Metodoid_API': 'first',
    'Primer_instalacion': 'first', 'Fecha': 'last', 'ClienteID_API': 'first'
}.items() if col in df_hes.columns}
df_mapa = df_hes.groupby('Medidor').agg(agg_map).reset_index()

st.markdown(f'<h2 style="margin-top:-25px; margin-bottom: 10px;">Medidores inteligentes - Tablero de consumos</h2>', unsafe_allow_html=True)

# INDICADORES COMPACTOS
st.markdown(f"""
    <div class="metric-row">
        <div class="metric-box">
            <img src="https://cdn-icons-png.flaticon.com/512/2622/2622744.png" class="metric-icon">
            <div class="metric-content"><p class="metric-label">N° de medidores</p><p class="metric-value">{len(df_mapa):,}</p></div>
        </div>
        <div class="metric-box">
            <img src="https://cdn-icons-png.flaticon.com/512/3105/3105807.png" class="metric-icon">
            <div class="metric-content"><p class="metric-label">Consumo acumulado m3</p><p class="metric-value">{df_hes['Consumo_diario'].sum():,.2f}</p></div>
        </div>
        <div class="metric-box">
            <img src="https://cdn-icons-png.flaticon.com/512/1570/1570887.png" class="metric-icon">
            <div class="metric-content"><p class="metric-label">Promedio diario m3</p><p class="metric-value">{df_hes['Consumo_diario'].mean():,.2f}</p></div>
        </div>
        <div class="metric-box">
            <img src="https://cdn-icons-png.flaticon.com/512/2666/2666505.png" class="metric-icon">
            <div class="metric-content"><p class="metric-label">Lecturas</p><p class="metric-value">{len(df_hes):,}</p></div>
        </div>
    </div>
""", unsafe_allow_html=True)

c_left, c_right = st.columns([3, 1.2])

with c_left:
    m = folium.Map(location=[21.8853, -102.2916], zoom_start=12, tiles=None)
    folium.TileLayer('CartoDB dark_matter', name="Mapa Negro", control=True).add_to(m)
    folium.TileLayer('OpenStreetMap', name="Mapa Estándar", control=True).add_to(m)
    folium.TileLayer(tiles='https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}', attr='Esri', name='Satélite', control=True).add_to(m)
    Fullscreen().add_to(m)

    if not df_sec.empty:
        s_group = folium.FeatureGroup(name="Sectores Hidrométricos", show=True).add_to(m)
        for _, row in df_sec.iterrows():
            folium.GeoJson(json.loads(row['geojson_data']), style_function=lambda x: {'fillColor': '#00d4ff', 'color': '#00d4ff', 'weight': 1, 'fillOpacity': 0.1}).add_to(s_group)

    m_group = folium.FeatureGroup(name="Medidores", show=True).add_to(m)
    for _, r in df_mapa.iterrows():
        if pd.notnull(r['Latitud']):
            color_hex, etiqueta = get_color_logic(r.get('Nivel'), r.get('Consumo_diario', 0))
            
            # POPUP COMPLETO RESTAURADO DEL RESPALDO
            pop_html = f"""
            <div style='font-family: Arial, sans-serif; font-size: 12px; width: 300px; color: #333; line-height: 1.4;'>
                <h5 style='margin:0 0 8px 0; color: #007bff; border-bottom: 1px solid #ccc; padding-bottom: 3px;'>Detalle del Medidor</h5>
                <b>Cliente:</b> {r.get('ClienteID_API', 'N/A')} - <b>Serie:</b> {r['Medidor']}<br>
                <b>Fecha instalación:</b> {r.get('Primer_instalacion', 'N/A')}<br>
                <b>Predio:</b> {r.get('Predio', 'N/A')}<br>
                <b>Nombre:</b> {r.get('Nombre', 'N/A')}<br>
                <b>Tarifa:</b> {r.get('Nivel', 'N/A')}<br>
                <b>Giro:</b> {r.get('Giro', 'N/A')}<br>
                <b>Dirección:</b> {r.get('Domicilio', 'N/A')}<br>
                <b>Colonia:</b> {r.get('Colonia', 'N/A')}<br>
                <b>Sector:</b> {r.get('Sector', 'N/A')}<br>
                <b>Lectura:</b> {r.get('Lectura', 0):,.2f} (m3) - <b>Última:</b> {r.get('Fecha', 'N/A')}<br>
                <b>Consumo:</b> {r.get('Consumo_diario', 0):,.2f} (m3) acumulado<br>
                <b>Tipo de comunicación:</b> {r.get('Metodoid_API', 'Lorawan')}<br><br>
                <div style='text-align: center; padding: 5px; background-color: {color_hex}22; border-radius: 4px; border: 1px solid {color_hex};'>
                    <b style='color: {color_hex};'>ANILLAS DE CONSUMO: {etiqueta}</b>
                </div>
            </div>
            """
            folium.CircleMarker([r['Latitud'], r['Longitud']], radius=4, color=color_hex, fill=True, fill_opacity=0.9, popup=folium.Popup(pop_html, max_width=350)).add_to(m_group)

    folium.LayerControl(collapsed=False).add_to(m)
    res = st_folium(m, width=900, height=520, key="main_map", returned_objects=["last_object_clicked"])

with c_right:
    med_sel = None
    if res and res.get("last_object_clicked"):
        lat, lon = res["last_object_clicked"]["lat"], res["last_object_clicked"]["lng"]
        match = df_mapa[(abs(df_mapa['Latitud'] - lat) < 0.0001) & (abs(df_mapa['Longitud'] - lon) < 0.0001)]
        if not match.empty: med_sel = match.iloc[0]['Medidor']

    st.markdown(f'<div style="background:#111;padding:8px;border:1px solid #00d4ff;font-size:18px;font-weight:bold;margin-bottom:5px;">📊 {med_sel if med_sel else "Seleccione medidor"}</div>', unsafe_allow_html=True)
    
    if med_sel:
        df_v = df_hes[df_hes['Medidor'] == med_sel].sort_values('Fecha', ascending=False)
        st.dataframe(df_v[['Fecha', 'Lectura', 'Consumo_diario']], height=280, hide_index=True, use_container_width=True)
    else:
        st.dataframe(df_hes[['Medidor', 'Fecha', 'Lectura', 'Consumo_diario']].tail(12), height=280, hide_index=True, use_container_width=True)

    if not df_hes.empty and 'Giro' in df_hes.columns:
        fig = px.pie(df_hes, names='Giro', hole=0.6, color_discrete_sequence=px.colors.qualitative.Safe)
        fig.update_layout(margin=dict(t=10, b=10, l=10, r=10), showlegend=False, paper_bgcolor='rgba(0,0,0,0)', font=dict(color="white", size=10))
        st.plotly_chart(fig, use_container_width=True)
