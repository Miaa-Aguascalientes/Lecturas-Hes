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
        [data-testid="stSidebarUserContent"] div[data-testid="stVerticalBlock"] > div {
            padding-bottom: 0px !important;
            padding-top: 0px !important;
            margin-bottom: -5px !important;
        }
        [data-testid="stWidgetLabel"] p {
            font-size: 14px !important;
            margin-bottom: 0px !important;
        }
        .stMultiSelect {
            margin-bottom: 0px !important;
        }
        .map-legend {
            display: flex;
            justify-content: center;
            flex-wrap: wrap;
            gap: 20px;
            padding: 15px;
            background-color: #111111;
            border-radius: 8px;
            margin-top: 10px;
            border: 1px solid #333;
        }
        .legend-item {
            display: flex;
            align-items: center;
            font-size: 13px;
            font-weight: bold;
        }
        .legend-color {
            width: 12px;
            height: 12px;
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

def reiniciar_tablero():
    st.cache_data.clear()
    st.cache_resource.clear()
    time.sleep(1) 
    st.rerun()

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
ultimo_dia_mes_pasado = inicio_mes_actual - pd.Timedelta(days=1)
inicio_mes_pasado = ultimo_dia_mes_pasado.replace(day=1)
inicio_año_actual = ahora.replace(month=1, day=1)
inicio_año_pasado = inicio_año_actual - pd.DateOffset(years=1)
fin_año_pasado = inicio_año_actual - pd.Timedelta(days=1)

with st.sidebar:
    st.image(URL_LOGO_MIAA, use_container_width=True)
    st.divider()
    if st.button("♻️ Actualizar Datos", use_container_width=True):
        st.cache_data.clear()
        st.cache_resource.clear()
        st.rerun()
    st.divider()

    st.write("**📅 Selecciona un rango**")
    opcion_rango = st.selectbox("Rango predefinido", ["Este mes", "Última semana", "Mes pasado", "Últimos 6 meses", "Este año", "Año pasado", "Personalizado"], index=0, label_visibility="collapsed")

    if opcion_rango == "Este mes": default_range = (inicio_mes_actual, ahora)
    elif opcion_rango == "Última semana": default_range = (ahora - pd.Timedelta(days=7), ahora)
    elif opcion_rango == "Mes pasado": default_range = (inicio_mes_pasado, ultimo_dia_mes_pasado)
    elif opcion_rango == "Últimos 6 meses": default_range = (ahora - pd.DateOffset(months=6), ahora)
    elif opcion_rango == "Este año": default_range = (inicio_año_actual, ahora)
    elif opcion_rango == "Año pasado": default_range = (inicio_año_pasado, fin_año_pasado)
    else: default_range = (inicio_mes_actual, ahora)

    try:
        fecha_rango = st.date_input("Periodo de consulta", value=default_range, max_value=ahora, format="DD/MM/YYYY", label_visibility="collapsed")
    except:
        st.stop()
    
    if len(fecha_rango) == 2:
        df_hes = pd.read_sql(f"SELECT * FROM HES WHERE Fecha BETWEEN '{fecha_rango[0]}' AND '{fecha_rango[1]}'", mysql_engine)
        st.markdown("<br>", unsafe_allow_html=True)
        filtros_sidebar = ["ClienteID_API", "Metodoid_API", "Medidor", "Predio", "Colonia", "Giro", "Sector"]
        filtros_activos = {}
        
        for col in filtros_sidebar:
            if col in df_hes.columns:
                opciones = sorted(df_hes[col].unique().astype(str).tolist())
                c1, c2 = st.columns([1, 2])
                with c1: st.markdown(f"<p style='margin-top:10px; font-size: 14px;'>{col}</p>", unsafe_allow_html=True)
                with c2: seleccion = st.multiselect("", options=opciones, key=f"f_{col}", label_visibility="collapsed")
                filtros_activos[col] = seleccion
                if seleccion: df_hes = df_hes[df_hes[col].astype(str).isin(seleccion)]

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
        st.markdown('<div style="background-color: #444; padding: 10px; border-radius: 5px; text-align: center; margin: 15px 0;">⚠️ <b>Informe alarmas</b></div>', unsafe_allow_html=True)
    else:
        st.stop()

# PROCESAMIENTO
mapeo_columnas = {'Consumo_diario': 'sum', 'Lectura': 'last', 'Latitud': 'first', 'Longitud': 'first', 'Nivel': 'first', 'ClienteID_API': 'first', 'Nombre': 'first', 'Predio': 'first', 'Domicilio': 'first', 'Colonia': 'first', 'Giro': 'first', 'Sector': 'first', 'Metodoid_API': 'first', 'Primer_instalacion': 'first', 'Fecha': 'last'}
agg_segura = {col: func for col, func in mapeo_columnas.items() if col in df_hes.columns}
df_mapa = df_hes.groupby('Medidor').agg(agg_segura).reset_index()
df_valid_coords = df_mapa[(df_mapa['Latitud'] != 0) & (df_mapa['Longitud'] != 0) & (df_mapa['Latitud'].notnull())]

if not df_valid_coords.empty and (filtros_activos.get("Colonia") or filtros_activos.get("Sector")):
    lat_centro, lon_centro, zoom_inicial = df_valid_coords['Latitud'].mean(), df_valid_coords['Longitud'].mean(), 14
else:
    lat_centro, lon_centro, zoom_inicial = 21.8853, -102.2916, 12

# DASHBOARD
st.markdown('<div class="titulo-superior">Medidores inteligentes - Tablero de consumos</div>', unsafe_allow_html=True)

# Indicadores
m1, m2, m3, m4 = st.columns(4)
m1.metric("📟 N° de medidores", f"{len(df_mapa):,}")
consumo_total = df_hes['Consumo_diario'].sum() if 'Consumo_diario' in df_hes.columns else 0
m2.metric("💧 Consumo total", f"{consumo_total:,.1f} m³")
promedio = df_hes['Consumo_diario'].mean() if 'Consumo_diario' in df_hes.columns else 0
m3.metric("📈 Promedio diario", f"{promedio:.2f} m³")
m4.metric("📋 Total lecturas", f"{len(df_hes):,}")

col_map, col_der = st.columns([6, 1.2])

with col_map:
    m = folium.Map(location=[lat_centro, lon_centro], zoom_start=zoom_inicial, tiles="CartoDB dark_matter")
    Fullscreen(position="topright", title="Ver en pantalla completa", title_cancel="Salir de pantalla completa", force_separate_button=True).add_to(m)
    
    if not df_sec.empty:
        for _, row in df_sec.iterrows():
            geojson_obj = json.loads(row['geojson_data'])
            folium.GeoJson(geojson_obj, style_function=lambda x: {'fillColor': '#00d4ff', 'color': '#00d4ff', 'weight': 1, 'fillOpacity': 0.1}, highlight_function=lambda x: {'fillColor': '#ffff00', 'color': '#ffff00', 'weight': 3, 'fillOpacity': 0.4}, tooltip=folium.Tooltip(f"Sector: {row['sector']}", sticky=True)).add_to(m)

    for _, r in df_mapa.iterrows():
        if pd.notnull(r['Latitud']) and pd.notnull(r['Longitud']):
            color_hex, etiqueta = get_color_logic(r.get('Nivel'), r.get('Consumo_diario', 0))
            
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
            folium.CircleMarker(location=[r['Latitud'], r['Longitud']], radius=2, color=color_hex, fill=True, fill_opacity=0.9, tooltip=folium.Tooltip(tooltip_html, sticky=True)).add_to(m)
    
    folium_static(m, width=900, height=550)

    st.markdown("""
        <div class="map-legend">
            <div class="legend-item"><div class="legend-color" style="background-color: #00FF00;"></div>CONSUMO REGULAR</div>
            <div class="legend-item"><div class="legend-color" style="background-color: #32CD32;"></div>CONSUMO NORMAL</div>
            <div class="legend-item"><div class="legend-color" style="background-color: #FF8C00;"></div>CONSUMO BAJO</div>
            <div class="legend-item"><div class="legend-color" style="background-color: #FFFFFF; border: 1px solid #555;"></div>CONSUMO CERO</div>
            <div class="legend-item"><div class="legend-color" style="background-color: #FF0000;"></div>CONSUMO MUY ALTO</div>
            <div class="legend-item"><div class="legend-color" style="background-color: #B22222;"></div>CONSUMO ALTO</div>
        </div>
    """, unsafe_allow_html=True)

with col_der:
    st.write("🟢 **Histórico Reciente**")
    if not df_hes.empty:
        st.dataframe(df_hes[['Fecha', 'Lectura', 'Consumo_diario']].tail(15).sort_values(by='Fecha', ascending=False), hide_index=True, use_container_width=True)
    else:
        st.info("No hay lecturas para el periodo seleccionado.")

# --- INTEGRACIÓN DE GRÁFICOS APILADOS ---
st.divider()

if not df_hes.empty:
    # 1. Gráfico de Consumo Total por Día (Ancho Completo)
    df_diario = df_hes.groupby('Fecha')['Consumo_diario'].sum().reset_index()
    fig_diario = px.bar(
        df_diario, x='Fecha', y='Consumo_diario', text_auto=',.2f',
        color_discrete_sequence=['#00d4ff']
    )
    fig_diario.update_layout(
        title="Consumo Total por Día",
        paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)',
        font_color="white", height=350, margin=dict(l=10, r=10, t=40, b=10)
    )
    fig_diario.update_traces(textposition='outside')
    fig_diario.update_yaxes(tickformat=",") # Formato de miles en el eje Y
    st.plotly_chart(fig_diario, use_container_width=True)

    # 2. Gráfico de Consumo por Medidor (Ancho Completo - Todos los medidores)
    df_todos_med = df_mapa.sort_values(by='Consumo_diario', ascending=False)
    fig_med = px.bar(
        df_todos_med, x='Medidor', y='Consumo_diario',
        color_discrete_sequence=['#00d4ff']
    )
    fig_med.update_layout(
        title="Consumo por Medidor (Registros Totales)",
        paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)',
        font_color="white", height=350, margin=dict(l=10, r=10, t=40, b=10)
    )
    fig_med.update_yaxes(tickformat=",") # Formato de miles en el eje Y
    fig_med.update_xaxes(tickangle=45, type='category') # Categoría para evitar que Plotly agrupe IDs numéricos
    st.plotly_chart(fig_med, use_container_width=True)

