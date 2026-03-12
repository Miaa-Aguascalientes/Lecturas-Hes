import streamlit as st
import pandas as pd
import folium
from streamlit_folium import st_folium
from folium.plugins import Fullscreen
from sqlalchemy import create_engine
import psycopg2
import json
import urllib.parse
import time

# 1. CONFIGURACIÓN
st.set_page_config(page_title="MIAA - Tablero de Consumos", layout="wide")

# ESTILO CSS
st.markdown("""
    <style>
        .stApp { background-color: #000000 !important; color: white; }
        section[data-testid="stSidebar"] { background-color: #111111 !important; }
        [data-testid="stSidebarUserContent"] div[data-testid="stVerticalBlock"] > div {
            padding-bottom: 0px !important; padding-top: 0px !important; margin-bottom: -5px !important;
        }
        [data-testid="stWidgetLabel"] p { font-size: 14px !important; margin-bottom: 0px !important; }
        .stMultiSelect { margin-bottom: 0px !important; }
        
        /* Estilo para los indicadores con iconos */
        .metric-container {
            background-color: #000;
            border: 1px solid #333;
            padding: 10px;
            border-radius: 5px;
            display: flex;
            align-items: center;
            justify-content: center;
        }
        .metric-icon { width: 50px; margin-right: 15px; }
        .metric-text { line-height: 1.2; }
        .metric-label { font-size: 14px; color: #ccc; margin: 0; }
        .metric-value { font-size: 24px; font-weight: bold; color: white; margin: 0; }
    </style>
""", unsafe_allow_html=True)

URL_LOGO_MIAA = "https://raw.githubusercontent.com/Miaa-Aguascalientes/Lecturas-Hes/refs/heads/main/LOGO%20HES.png"

# Iconos para indicadores (puedes reemplazarlos por rutas locales si las tienes)
ICON_METER = "https://cdn-icons-png.flaticon.com/512/2622/2622744.png"
ICON_DROP = "https://cdn-icons-png.flaticon.com/512/3105/3105807.png"
ICON_AVG = "https://cdn-icons-png.flaticon.com/512/1570/1570887.png"
ICON_LIST = "https://cdn-icons-png.flaticon.com/512/2666/2666505.png"

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
        return None

@st.cache_resource
def get_postgres_conn():
    try:
        return psycopg2.connect(**st.secrets["postgres"])
    except:
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
    except:
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

mysql_engine = get_mysql_engine()
df_sec = get_sectores_cached()

with st.sidebar:
    st.image(URL_LOGO_MIAA, use_container_width=True)
    st.divider()
    
    ahora = pd.Timestamp.now()
    fecha_rango = st.date_input("Periodo de consulta", value=(ahora.replace(day=1), ahora))
    
    if len(fecha_rango) == 2:
        df_hes = pd.read_sql(f"SELECT * FROM HES WHERE Fecha BETWEEN '{fecha_rango[0]}' AND '{fecha_rango[1]}'", mysql_engine)
        
        filtros_sidebar = ["ClienteID_API", "Metodoid_API", "Medidor", "Predio", "Colonia", "Giro", "Sector"]
        for col in filtros_sidebar:
            if col in df_hes.columns:
                opciones = sorted(df_hes[col].unique().astype(str).tolist())
                seleccion = st.multiselect(col, options=opciones)
                if seleccion:
                    df_hes = df_hes[df_hes[col].astype(str).isin(seleccion)]

        st.divider()
        st.write("**Ranking Top 10 Consumo**")
        if not df_hes.empty:
            ranking_data = df_hes.groupby('Medidor')['Consumo_diario'].sum().sort_values(ascending=False).head(10).reset_index()
            max_c = ranking_data['Consumo_diario'].max() if not ranking_data.empty else 1
            for _, row in ranking_data.iterrows():
                rc1, rc2 = st.columns([1, 1])
                rc1.markdown(f"<span style='color: #81D4FA; font-size: 12px;'>{row['Medidor']}</span>", unsafe_allow_html=True)
                pct = (row['Consumo_diario'] / max_c) * 100
                rc2.markdown(f'<div style="display: flex; align-items: center; justify-content: flex-end;"><span style="font-size: 11px; margin-right: 5px;">{row["Consumo_diario"]:,.0f}</span><div style="width: 40px; background-color: #333; height: 8px; border-radius: 2px;"><div style="width: {pct}%; background-color: #FF0000; height: 8px; border-radius: 2px;"></div></div></div>', unsafe_allow_html=True)

# PROCESAMIENTO
agg_segura = {col: func for col, func in {
    'Consumo_diario': 'sum', 'Lectura': 'last', 'Latitud': 'first', 'Longitud': 'first',
    'Nivel': 'first', 'ClienteID_API': 'first', 'Nombre': 'first', 'Predio': 'first',
    'Domicilio': 'first', 'Colonia': 'first', 'Giro': 'first', 'Sector': 'first',
    'Metodoid_API': 'first', 'Primer_instalacion': 'first', 'Fecha': 'last'
}.items() if col in df_hes.columns}
df_mapa = df_hes.groupby('Medidor').agg(agg_segura).reset_index()

st.title("Medidores inteligentes - Tablero de consumos")

# --- INDICADORES CON ICONOS (HTML PERSONALIZADO) ---
cols_m = st.columns(4)
metrics = [
    (ICON_METER, "N° de medidores", f"{len(df_mapa):,}"),
    (ICON_DROP, "Consumo acumulado m3", f"{df_hes['Consumo_diario'].sum():,.2f}"),
    (ICON_AVG, "Prom. de Consumo diario m3", f"{df_hes['Consumo_diario'].mean():,.2f}"),
    (ICON_LIST, "Lecturas", f"{len(df_hes):,}")
]

for i, (icon, label, value) in enumerate(metrics):
    with cols_m[i]:
        st.markdown(f"""
            <div class="metric-container">
                <img src="{icon}" class="metric-icon">
                <div class="metric-text">
                    <p class="metric-label">{label}</p>
                    <p class="metric-value">{value}</p>
                </div>
            </div>
        """, unsafe_allow_html=True)

st.markdown("<br>", unsafe_allow_html=True)
col_map, col_der = st.columns([3, 1.2])

with col_map:
    # Creación del mapa base
    m = folium.Map(location=[lat_centro, lon_centro], zoom_start=zoom_inicial, tiles="CartoDB dark_matter")
    
    # --- BOTÓN PANTALLA COMPLETA ---
    Fullscreen(
        position="topright",
        title="Ver en pantalla completa",
        title_cancel="Salir de pantalla completa",
        force_separate_button=True,
    ).add_to(m)

    if not df_sec.empty:
        sectores_layer = folium.FeatureGroup(name="Sectores Hidrométricos", control=True)
        for _, row in df_sec.iterrows():
            folium.GeoJson(json.loads(row['geojson_data']),
                style_function=lambda x: {'fillColor': '#00d4ff', 'color': '#00d4ff', 'weight': 1, 'fillOpacity': 0.1}
            ).add_to(sectores_layer)
        sectores_layer.add_to(m)

    marcadores_layer = folium.FeatureGroup(name="Medidores", control=True)
    for _, r in df_mapa.iterrows():
        if pd.notnull(r['Latitud']) and pd.notnull(r['Longitud']):
            color_hex, etiqueta = get_color_logic(r.get('Nivel'), r.get('Consumo_diario', 0))
            
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
            folium.CircleMarker(
                location=[r['Latitud'], r['Longitud']],
                radius=5, color=color_hex, fill=True, fill_opacity=0.9,
                tooltip=folium.Tooltip(pop_html)
            ).add_to(marcadores_layer)
    marcadores_layer.add_to(m)

    folium.LayerControl(position='topright').add_to(m)
    
    # Renderizado estable: solo devuelve clics para evitar recargas en zoom
    map_data = st_folium(
        m, width=900, height=550, key="mapa_miaa",
        returned_objects=["last_object_clicked"]
    )

with col_der:
    medidor_clicado = None
    if map_data and map_data.get("last_object_clicked"):
        lat_c, lon_c = map_data["last_object_clicked"]["lat"], map_data["last_object_clicked"]["lng"]
        match = df_mapa[(abs(df_mapa['Latitud'] - lat_c) < 0.0001) & (abs(df_mapa['Longitud'] - lon_c) < 0.0001)]
        if not match.empty:
            medidor_clicado = match.iloc[0]['Medidor']

    if medidor_clicado:
        st.subheader(f"📊 {medidor_clicado}")
        df_click = df_hes[df_hes['Medidor'] == medidor_clicado].sort_values(by='Fecha', ascending=False)
        st.dataframe(df_click[['Fecha', 'Lectura', 'Consumo_diario']], hide_index=True, use_container_width=True)
    else:
        st.write("🟢 **Histórico General**")
        if not df_hes.empty:
            st.dataframe(df_hes[['Medidor', 'Fecha', 'Lectura', 'Consumo_diario']].tail(15), hide_index=True)

if st.button("🔄 Reiniciar", use_container_width=True):
    st.rerun()

