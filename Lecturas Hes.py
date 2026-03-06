import streamlit as st
import pandas as pd
import folium
from streamlit_folium import folium_static
from sqlalchemy import create_engine
import psycopg2
import json
import urllib.parse
import plotly.express as px
import time

# 1. CONFIGURACIÓN DE PÁGINA Y ESTILOS CSS
st.set_page_config(page_title="MIAA - Tablero de Consumos", layout="wide")

# Aplicación de estilo visual: Fondo negro y personalización de la barra lateral
st.markdown("""
    <style>
        .stApp { background-color: #000000 !important; color: white; }
        section[data-testid="stSidebar"] { background-color: #111111 !important; }
        
        /* Asegura que el contenedor del multiselect ocupe el ancho total y sea legible */
        div[data-baseweb="select"] {
            width: 100% !important;
        }
    </style>
""", unsafe_allow_html=True)

# URL del logo en GitHub (Raw)
URL_LOGO_MIAA = "https://raw.githubusercontent.com/Miaa-Aguascalientes/Lecturas-Hes/refs/heads/main/LOGO%20HES.png"

# --- CONEXIONES ---

@st.cache_resource
def get_mysql_engine():
    """Establece conexión con MySQL usando SQLAlchemy y Secrets."""
    try:
        creds = st.secrets["mysql"]
        user = creds["user"]
        # quote_plus es vital para manejar caracteres especiales en la contraseña
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
    """Establece conexión con PostgreSQL usando psycopg2 y Secrets."""
    try:
        return psycopg2.connect(**st.secrets["postgres"])
    except Exception as e:
        st.error(f"Error conectando a Postgres: {e}")
        return None

@st.cache_data(ttl=3600)
def get_sectores_cached():
    """Carga polígonos de sectores desde Postgres (PostGIS)."""
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

# --- UTILIDADES ---

def reiniciar_tablero():
    """Limpia el caché y reinicia la aplicación con una animación visual."""
    st.cache_data.clear()
    st.cache_resource.clear()
    placeholder = st.empty()
    with placeholder.container():
        st.markdown("<br><br><br>", unsafe_allow_html=True)
        st.markdown("<h1 style='text-align: center; font-size: 100px;'>🍞</h1>", unsafe_allow_html=True)
        st.markdown("<h3 style='text-align: center; color: white;'>Tu aplicación está en el horno</h3>", unsafe_allow_html=True)
    time.sleep(1.5) 
    st.rerun()

def get_color_logic(nivel, consumo_mes):
    """Determina el color del marcador según el nivel y consumo detectado."""
    v = float(consumo_mes) if consumo_mes else 0
    colors = {
        "REGULAR": "#00FF00", "NORMAL": "#32CD32", "BAJO": "#FF8C00", 
        "CERO": "#FFFFFF", "MUY ALTO": "#FF0000", "ALTO": "#B22222", "null": "#0000FF"
    }
    config = {
        'DOMESTICO A': [5, 10, 15, 30], 
        'DOMESTICO B': [6, 11, 20, 30], 
        'DOMESTICO C': [8, 19, 37, 50]
    }
    
    n = str(nivel).upper()
    lim = config.get(n, [5, 10, 15, 30])
    
    if v <= 0: return colors["CERO"], "CONSUMO CERO"
    if v <= lim[0]: return colors["BAJO"], "CONSUMO BAJO"
    if v <= lim[1]: return colors["REGULAR"], "CONSUMO REGULAR"
    if v <= lim[2]: return colors["NORMAL"], "CONSUMO NORMAL"
    if v <= lim[3]: return colors["ALTO"], "CONSUMO ALTO"
    return colors["MUY ALTO"], "CONSUMO MUY ALTO"

# --- LÓGICA PRINCIPAL ---

mysql_engine = get_mysql_engine()
df_sec = get_sectores_cached()

# --- SIDEBAR Y FILTROS ---
with st.sidebar:
    # Logo de MIAA cargado directamente desde GitHub
    st.image(URL_LOGO_MIAA, use_container_width=True)
    st.divider()
    
    if st.button("♻️ Actualizar Datos", use_container_width=True):
        reiniciar_tablero()
    
    st.divider()
    
    # Rango de fechas para la consulta
    try:
        fecha_rango = st.date_input("Periodo de consulta", value=(pd.Timestamp(2026, 2, 1), pd.Timestamp(2026, 2, 28)))
    except:
        st.stop()
    
    if len(fecha_rango) == 2:
        # Carga de datos HES basada en el motor MySQL verificado
        df_hes = pd.read_sql(f"SELECT * FROM HES WHERE Fecha BETWEEN '{fecha_rango[0]}' AND '{fecha_rango[1]}'", mysql_engine)
        
        # Configuración de filtros con títulos internos (Placeholders)
        filtros_config = [
            ("Metodoid_API", "API de método"),
            ("Medidor", "Medidor"),
            ("Predio", "Predio"),
            ("Colonia", "Colonia"),
            ("Giro", "Giro"),
            ("Sector", "Sector")
        ]
        
        # Generación de multiselects sin etiquetas externas para diseño limpio
        for col_db, titulo in filtros_config:
            if col_db in df_hes.columns:
                opciones = sorted(df_hes[col_db].unique().astype(str).tolist())
                seleccion = st.multiselect(
                    label=titulo,
                    options=opciones,
                    placeholder=titulo, 
                    key=f"f_{col_db}",
                    label_visibility="collapsed"
                )
                if seleccion:
                    df_hes = df_hes[df_hes[col_db].astype(str).isin(seleccion)]

        st.divider()
        st.write("**Ranking Top 10 Consumo**")
        if not df_hes.empty:
            ranking = df_hes.groupby('Medidor')['Consumo_diario'].sum().sort_values(ascending=False).head(10).reset_index()
            for _, row in ranking.iterrows():
                st.caption(f"{row['Medidor']}: {row['Consumo_diario']:,.1f} m3")
    else:
        st.info("Seleccione un rango de fechas en el calendario.")
        st.stop()

# --- DASHBOARD PRINCIPAL ---

st.title("📊 Medidores Inteligentes - Tablero de Consumos")

# Agregación de datos para visualización en mapa
mapeo_columnas = {
    'Consumo_diario': 'sum', 'Lectura': 'last', 'Latitud': 'first', 'Longitud': 'first',
    'Nivel': 'first', 'Colonia': 'first', 'Sector': 'first'
}
agg_actual = {col: func for col, func in mapeo_columnas.items() if col in df_hes.columns}
df_mapa = df_hes.groupby('Medidor').agg(agg_actual).reset_index()

# Métricas de resumen
m1, m2, m3, m4 = st.columns(4)
m1.metric("N° de medidores", f"{len(df_mapa):,}")
m2.metric("Consumo acumulado m3", f"{df_hes['Consumo_diario'].sum():,.1f}")
m3.metric("Promedio diario m3", f"{df_hes['Consumo_diario'].mean():.2f}")
m4.metric("Total lecturas", f"{len(df_hes):,}")

# Layout del Mapa y Tabla de datos
col_map, col_der = st.columns([3, 1.2])

with col_map:
    # Centrado dinámico del mapa basado en coordenadas válidas
    df_coords = df_mapa[(df_mapa['Latitud'] != 0) & (df_mapa['Latitud'].notnull())]
    lat_centro, lon_centro = (df_coords['Latitud'].mean(), df_coords['Longitud'].mean()) if not df_coords.empty else (21.8853, -102.2916)
    
    m = folium.Map(location=[lat_centro, lon_centro], zoom_start=13, tiles="CartoDB dark_matter")
    
    # Renderizado de Sectores Hidrométricos (Postgres)
    if not df_sec.empty:
        for _, row in df_sec.iterrows():
            folium.GeoJson(
                json.loads(row['geojson_data']),
                style_function=lambda x: {'fillColor': '#00d4ff', 'color': '#00d4ff', 'weight': 1, 'fillOpacity': 0.1},
                tooltip=f"Sector: {row['sector']}"
            ).add_to(m)

    # Marcadores circulares para medidores individuales
    for _, r in df_mapa.iterrows():
        if pd.notnull(r['Latitud']) and pd.notnull(r['Longitud']):
            color_hex, _ = get_color_logic(r.get('Nivel'), r.get('Consumo_diario', 0))
            folium.CircleMarker(
                location=[r['Latitud'], r['Longitud']],
                radius=3.5, color=color_hex, fill=True, fill_opacity=0.8,
                popup=f"Medidor: {r['Medidor']}<br>Consumo: {r['Consumo_diario']:.2f} m3"
            ).add_to(m)
    
    folium_static(m, width=900, height=550)

with col_der:
    st.write("🟢 **Consumo en tiempo real**")
    st.dataframe(df_hes[['Fecha', 'Medidor', 'Consumo_diario']].tail(25), hide_index=True)

# Botón inferior para reinicio rápido
if st.button("Reset Sistema"):
    reiniciar_tablero()
