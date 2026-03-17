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

# ESTILO CSS (Aquí está el truco del marco)
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
        .block-container {
            padding-top: 1.8rem !important;
        }
        /* EL MARCO QUE BUSCAMOS */
        .marco-principal {
            background-color: #0e1117; /* Fondo oscuro sutil */
            border: 2px solid #333;    /* Borde gris oscuro */
            border-radius: 12px;       /* Bordes redondeados */
            padding: 25px;             /* Espacio interno */
            margin-top: 10px;
            margin-bottom: 25px;
            box-shadow: 0px 4px 15px rgba(0,0,0,0.5);
        }
        [data-testid="stMetric"] {
            background-color: #1a1c23;
            border-radius: 8px;
            padding: 10px !important;
        }
        .stApp { background-color: #000000 !important; }
        section[data-testid="stSidebar"] { background-color: #111111 !important; }
        .map-legend {
            display: flex;
            justify-content: center;
            flex-wrap: wrap;
            gap: 15px;
            padding: 10px;
            margin-top: 15px;
        }
    </style>
""", unsafe_allow_html=True)

URL_LOGO_MIAA = "https://raw.githubusercontent.com/Miaa-Aguascalientes/Lecturas-Hes/c45d926ef0e34215c237cd3c7f71f7b97bf9a784/LogoMIAA-BpcVaQaq.svg"

# FUNCIONES DE BASE DE DATOS
@st.cache_resource
def get_mysql_engine():
    try:
        creds = st.secrets["mysql"]
        user = creds["user"]
        pwd = urllib.parse.quote_plus(creds["password"])
        host = creds["host"]
        db = creds["database"]
        return create_engine(f"mysql+mysqlconnector://{user}:{pwd}@{host}/{db}")
    except: return None

@st.cache_resource
def get_postgres_conn():
    try: return psycopg2.connect(**st.secrets["postgres"])
    except: return None

@st.cache_data(ttl=3600)
def get_sectores_cached():
    conn = get_postgres_conn()
    if conn is None: return pd.DataFrame()
    df = pd.read_sql('SELECT sector, ST_AsGeoJSON(ST_Transform(geom, 4326)) AS geojson_data FROM "Sectorizacion"."Sectores_hidr"', conn)
    conn.close()
    return df

def get_color_logic(nivel, consumo_mes):
    v = float(consumo_mes) if consumo_mes else 0
    colors = {"REGULAR": "#00FF00", "NORMAL": "#32CD32", "BAJO": "#FF8C00", "CERO": "#FFFFFF", "MUY ALTO": "#FF0000", "ALTO": "#B22222"}
    config = {'DOMESTICO A': [5, 10, 15, 30], 'DOMESTICO B': [6, 11, 20, 30], 'DOMESTICO C': [8, 19, 37, 50]}
    n = str(nivel).upper()
    lim = config.get(n, [5, 10, 15, 30])
    if v <= 0: return colors["CERO"], "CONSUMO CERO"
    if v <= lim[0]: return colors["BAJO"], "CONSUMO BAJO"
    if v <= lim[1]: return colors["REGULAR"], "CONSUMO REGULAR"
    if v <= lim[2]: return colors["NORMAL"], "CONSUMO NORMAL"
    if v <= lim[3]: return colors["ALTO"], "CONSUMO ALTO"
    return colors["MUY ALTO"], "CONSUMO MUY ALTO"

# CARGA INICIAL
mysql_engine = get_mysql_engine()
df_sec = get_sectores_cached()
ahora = pd.Timestamp.now()

# SIDEBAR
with st.sidebar:
    st.image(URL_LOGO_MIAA, use_container_width=True)
    if st.button("♻️ Actualizar Datos", use_container_width=True):
        st.cache_data.clear()
        st.rerun()
    st.divider()

    # Rango de fechas
    with st.expander("📅 RANGO DE FECHAS", expanded=True):
        dr = (ahora.replace(day=1), ahora)
        fecha_rango = st.date_input("Periodo", value=dr, max_value=ahora, format="DD/MM/YYYY", label_visibility="collapsed")

    if len(fecha_rango) == 2:
        df_hes = pd.read_sql(f"SELECT * FROM HES WHERE Fecha BETWEEN '{fecha_rango[0]}' AND '{fecha_rango[1]}'", mysql_engine)
        
        # Filtros Técnicos (Sin ceros)
        with st.expander("🔍 FILTROS DE BÚSQUEDA", expanded=False):
            mapeo = {"ClienteID_API": "Cliente", "Metodoid_API": "Metodo", "Medidor": "Medidor", "Colonia": "Colonia", "Sector": "Sector"}
            filtros_activos = {}
            for col_real, nombre in mapeo.items():
                if col_real in df_hes.columns:
                    raw = df_hes[col_real].unique()
                    opciones = sorted([str(int(float(x))) if str(x).replace('.0', '').isdigit() else str(x) for x in raw if pd.notnull(x) and str(x).strip() not in ['0', '0.0', '']])
                    c1, c2 = st.columns([1, 2])
                    c1.markdown(f"**{nombre}**")
                    sel = c2.multiselect("", options=opciones, key=f"f_{col_real}", label_visibility="collapsed")
                    if sel: df_hes = df_hes[df_hes[col_real].astype(str).str.replace('.0', '', regex=False).isin(sel)]

        # Ranking Top 10
        with st.expander("🏆 RANKING TOP 10", expanded=True):
            rk = df_hes.groupby('Medidor')['Consumo_diario'].sum().sort_values(ascending=False).head(10).reset_index()
            for _, row in rk.iterrows():
                try: m_id = str(int(float(row['Medidor'])))
                except: m_id = str(row['Medidor'])
                rc1, rc2, rc3 = st.columns([1.2, 0.7, 1.1])
                rc1.markdown(f"<p style='color:#81D4FA; font-weight:800; margin-bottom:10px;'>{m_id}</p>", unsafe_allow_html=True)
                rc2.markdown(f"<p style='text-align:right; margin-bottom:10px;'>{row['Consumo_diario']:,.0f}</p>", unsafe_allow_html=True)
                pct = (row['Consumo_diario'] / (rk['Consumo_diario'].max() or 1)) * 100
                rc3.markdown(f'<div style="background:#262626; height:12px; border-radius:4px;"><div style="width:{pct}%; background:red; height:12px; border-radius:4px;"></div></div>', unsafe_allow_html=True)
    else: st.stop()

# PROCESAMIENTO MAPA
agg_map = {c: f for c, f in {'Consumo_diario': 'sum', 'Lectura': 'last', 'Latitud': 'first', 'Longitud': 'first', 'Nivel': 'first', 'ClienteID_API': 'first', 'Nombre': 'first', 'Predio': 'first', 'Domicilio': 'first', 'Colonia': 'first', 'Giro': 'first', 'Sector': 'first', 'Metodoid_API': 'first', 'Primer_instalacion': 'first', 'Fecha': 'last'}.items() if c in df_hes.columns}
df_mapa = df_hes.groupby('Medidor').agg(agg_map).reset_index()
df_coords = df_mapa[(df_mapa['Latitud'] != 0) & (df_mapa['Latitud'].notnull())]
lat_c, lon_c = (df_coords['Latitud'].mean(), df_coords['Longitud'].mean()) if not df_coords.empty else (21.8853, -102.2916)

# --- DASHBOARD PRINCIPAL CON MARCO ---
st.markdown('<div class="titulo-superior">Medidores inteligentes - Tablero de consumos</div>', unsafe_allow_html=True)

# INICIO DEL MARCO
st.markdown('<div class="marco-principal">', unsafe_allow_html=True)

# 1. Indicadores Superiores
m1, m2, m3, m4 = st.columns(4)
m1.metric("📟 N° de medidores", f"{len(df_mapa):,}")
m2.metric("💧 Consumo total", f"{df_hes['Consumo_diario'].sum():,.1f} m³")
m3.metric("📈 Promedio diario", f"{df_hes['Consumo_diario'].mean():.2f} m³")
m4.metric("📋 Total lecturas", f"{len(df_hes):,}")

st.markdown("<hr style='border: 1px solid #333; margin: 25px 0;'>", unsafe_allow_html=True)

# 2. Mapa e Histórico
col_map, col_der = st.columns([3, 1.2])

with col_map:
    m = folium.Map(location=[lat_c, lon_c], zoom_start=13, tiles="CartoDB dark_matter")
    Fullscreen().add_to(m)
    
    fg_m = folium.FeatureGroup(name="Medidores")
    for _, r in df_mapa.iterrows():
        if pd.notnull(r['Latitud']):
            color_hex, etiqueta = get_color_logic(r.get('Nivel'), r.get('Consumo_diario', 0))
            
            # Tu tooltip_html original completo
            tooltip_html = f"""
            <div style='font-family: Arial, sans-serif; font-size: 12px; color: #333; line-height: 1.4; padding: 10px; white-space: nowrap; display: inline-block;'>
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
                <div style='text-align: center; padding: 5px; background-color: {color_hex}22; border-radius: 2px; border: 1px solid {color_hex}; white-space: normal;'>
                    <b style='color: {color_hex};'>ANILLAS DE CONSUMO: {etiqueta}</b>
                </div>
            </div>
            """
            folium.CircleMarker([r['Latitud'], r['Longitud']], radius=4, color=color_hex, fill=True, tooltip=folium.Tooltip(tooltip_html)).add_to(fg_m)
    
    fg_m.add_to(m)
    folium_static(m, width=850, height=500)

with col_der:
    st.write("🟢 **Histórico Reciente**")
    st.dataframe(df_hes[['Fecha', 'Lectura', 'Consumo_diario']].tail(15).sort_values(by='Fecha', ascending=False), hide_index=True, use_container_width=True)

# CIERRE DEL MARCO
st.markdown('</div>', unsafe_allow_html=True)

# Gráficos inferiores
if not df_hes.empty:
    st.plotly_chart(px.bar(df_hes.groupby('Fecha')['Consumo_diario'].sum().reset_index(), x='Fecha', y='Consumo_diario', title="Consumo por Día", template="plotly_dark"), use_container_width=True)
