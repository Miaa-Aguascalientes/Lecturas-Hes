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

# 1. CONFIGURACIÓN
st.set_page_config(page_title="MIAA - Tablero de Consumos", layout="wide")

# ESTILO CSS ULTRA COMPACTO PARA EL SIDEBAR
st.markdown("""
    <style>
        /* Fondo general */
        .stApp { background-color: #000000 !important; color: white; }
        section[data-testid="stSidebar"] { background-color: #111111 !important; }

        /* 1. Reducir el espacio superior del sidebar */
        section[data-testid="stSidebar"] .st-emotion-cache-1647z7h { padding-top: 1rem !important; }

        /* 2. Compactar etiquetas de los filtros */
        div[data-testid="stWidgetLabel"] p {
            font-size: 13px !important;
            margin-bottom: -15px !important;  /* Elimina el espacio entre texto y caja */
            color: #81D4FA !important;
        }

        /* 3. Reducir altura de las cajas de selección */
        div[data-baseweb="select"] > div {
            min-height: 30px !important;
            height: 30px !important;
        }

        /* 4. Reducir el espacio vertical entre cada filtro */
        div[data-testid="stVerticalBlock"] > div:has(div[data-testid="stMultiSelect"]) {
            margin-bottom: -15px !important;
            padding-bottom: 0px !important;
        }

        /* 5. Ajustar el divisor */
        hr { margin: 10px 0 !important; }
    </style>
""", unsafe_allow_html=True)

# URL RAW DE TU LOGO EN GITHUB
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
        st.error(f"Error motor MySQL: {e}")
        return None

@st.cache_resource
def get_postgres_conn():
    try:
        return psycopg2.connect(**st.secrets["postgres"])
    except Exception as e:
        st.error(f"Error Postgres: {e}")
        return None

@st.cache_data(ttl=3600)
def get_sectores_cached():
    conn = get_postgres_conn()
    if conn is None: return pd.DataFrame()
    try:
        query = 'SELECT sector, ST_AsGeoJSON(ST_Transform(geom, 4326)) AS geojson_data FROM "Sectorizacion"."Sectores_hidr"'
        df = pd.read_sql(query, conn)
        conn.close()
        return df
    except Exception as e:
        st.sidebar.error(f"Error Postgres: {e}")
        return pd.DataFrame()

def reiniciar_tablero():
    st.cache_data.clear()
    st.cache_resource.clear()
    st.rerun()

def get_color_logic(nivel, consumo_mes):
    v = float(consumo_mes) if consumo_mes else 0
    colors = {"REGULAR": "#00FF00", "NORMAL": "#32CD32", "BAJO": "#FF8C00", "CERO": "#FFFFFF", "MUY ALTO": "#FF0000", "ALTO": "#B22222"}
    config = {'DOMESTICO A': [5, 10, 15, 30], 'DOMESTICO B': [6, 11, 20, 30], 'DOMESTICO C': [8, 19, 37, 50]}
    n = str(nivel).upper()
    lim = config.get(n, [5, 10, 15, 30])
    if v <= 0: return colors["CERO"], "CERO"
    if v <= lim[0]: return colors["BAJO"], "BAJO"
    if v <= lim[1]: return colors["REGULAR"], "REGULAR"
    if v <= lim[2]: return colors["NORMAL"], "NORMAL"
    if v <= lim[3]: return colors["ALTO"], "ALTO"
    return colors["MUY ALTO"], "MUY ALTO"

# LOGICA DE DATOS
mysql_engine = get_mysql_engine()
df_sec = get_sectores_cached()

with st.sidebar:
    st.image(URL_LOGO_MIAA, use_container_width=True)
    
    if st.button("♻️ Actualizar", use_container_width=True):
        reiniciar_tablero()
    
    try:
        fecha_rango = st.date_input("Periodo", value=(pd.Timestamp(2026, 2, 1), pd.Timestamp(2026, 2, 28)))
    except:
        st.stop()
    
    if len(fecha_rango) == 2:
        df_hes = pd.read_sql(f"SELECT * FROM HES WHERE Fecha BETWEEN '{fecha_rango[0]}' AND '{fecha_rango[1]}'", mysql_engine)
        
        # Filtros compactados manualmente
        filtros = ["ClientID_API", "Metodoid_API", "Medidor", "Predio", "Colonia", "Giro", "Sector"]
        filtros_activos = {}
        
        for col in filtros:
            if col in df_hes.columns:
                opciones = sorted(df_hes[col].unique().astype(str).tolist())
                seleccion = st.multiselect(col, opciones, key=f"f_{col}")
                if seleccion:
                    df_hes = df_hes[df_hes[col].astype(str).isin(seleccion)]
                    filtros_activos[col] = seleccion

        st.divider()
        
        # RANKING TOP 20
        st.markdown("<b style='font-size: 14px;'>Ranking Top 20 Consumo</b>", unsafe_allow_html=True)
        if not df_hes.empty:
            ranking_data = df_hes.groupby('Medidor')['Consumo_diario'].sum().sort_values(ascending=False).head(20).reset_index()
            max_c = ranking_data['Consumo_diario'].max() if not ranking_data.empty else 1
            
            for _, row in ranking_data.iterrows():
                rc1, rc2 = st.columns([1.2, 1])
                rc1.markdown(f"<p style='color: #81D4FA; font-size: 10px; margin:0;'>{row['Medidor']}</p>", unsafe_allow_html=True)
                pct = (row['Consumo_diario'] / max_c) * 100
                rc2.markdown(f"""
                    <div style="display: flex; align-items: center; justify-content: flex-end; height: 12px;">
                        <span style="font-size: 9px; margin-right: 4px;">{row['Consumo_diario']:,.0f}</span>
                        <div style="width: 30px; background-color: #333; height: 5px; border-radius: 2px;">
                            <div style="width: {pct}%; background-color: #FF0000; height: 5px; border-radius: 2px;"></div>
                        </div>
                    </div>
                    """, unsafe_allow_html=True)

        st.markdown('<div style="background-color: #B22222; padding: 3px; border-radius: 5px; text-align: center; margin-top: 10px; font-size: 11px;">⚠️ <b>Informe alarmas</b></div>', unsafe_allow_html=True)
    else:
        st.stop()

# DASHBOARD PRINCIPAL (Resto del código igual)
mapeo_columnas = {'Consumo_diario': 'sum', 'Lectura': 'last', 'Latitud': 'first', 'Longitud': 'first', 'Nivel': 'first'}
agg_segura = {col: func for col, func in mapeo_columnas.items() if col in df_hes.columns}
df_mapa = df_hes.groupby('Medidor').agg(agg_segura).reset_index()

lat_centro, lon_centro, zoom_inicial = (21.8853, -102.2916, 12)
df_valid_coords = df_mapa[(df_mapa['Latitud'] != 0) & (df_mapa['Latitud'].notnull())]
if not df_valid_coords.empty and (filtros_activos.get("Colonia") or filtros_activos.get("Sector")):
    lat_centro, lon_centro, zoom_inicial = df_valid_coords['Latitud'].mean(), df_valid_coords['Longitud'].mean(), 14

st.title("Medidores inteligentes - Tablero de consumos")
m1, m2, m3, m4 = st.columns(4)
m1.metric("N° de medidores", f"{len(df_mapa):,}")
m2.metric("Consumo m3", f"{df_hes['Consumo_diario'].sum():,.1f}")
m3.metric("Promedio m3", f"{df_hes['Consumo_diario'].mean():.2f}")
m4.metric("Lecturas", f"{len(df_hes):,}")

col_map, col_der = st.columns([3, 1.2])

with col_map:
    m = folium.Map(location=[lat_centro, lon_centro], zoom_start=zoom_inicial, tiles="CartoDB dark_matter")
    for _, r in df_mapa.iterrows():
        if pd.notnull(r['Latitud']) and pd.notnull(r['Longitud']):
            color_hex, _ = get_color_logic(r.get('Nivel'), r.get('Consumo_diario', 0))
            folium.CircleMarker(location=[r['Latitud'], r['Longitud']], radius=3, color=color_hex, fill=True).add_to(m)
    folium_static(m, width=900, height=550)

with col_der:
    st.write("🟢 **Consumo real**")
    st.dataframe(df_hes[['Fecha', 'Lectura', 'Consumo_diario']].tail(15), hide_index=True)

if st.button("Reset"):
    reiniciar_tablero()
