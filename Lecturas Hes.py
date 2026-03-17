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

# ESTILO CSS COMPLETO
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
            margin-top: -60px !important; 
            max-width: 200px !important;
            margin-left: auto;
            margin-right: auto;
            display: block;
        }
        .block-container {
            padding-top: 1.8rem !important;
            padding-bottom: 0rem !important;
        }
        .marco-tablero {
            background-color: #111111;
            border: 1px solid #333;
            border-radius: 8px;
            padding: 20px;
            margin-bottom: 20px;
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
        .stApp { background-color: #000000 !important; color: white; }
        section[data-testid="stSidebar"] { background-color: #111111 !important; }
        .map-legend {
            display: flex;
            justify-content: center;
            flex-wrap: wrap;
            gap: 20px;
            padding: 15px;
            margin-top: 10px;
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

URL_LOGO_MIAA = "https://raw.githubusercontent.com/Miaa-Aguascalientes/Lecturas-Hes/c45d926ef0e34215c237cd3c7f71f7b97bf9a784/LogoMIAA-BpcVaQaq.svg"

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
        st.sidebar.error(f"Error consulta Postgres: {e}")
        return pd.DataFrame()

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

# --- SIDEBAR ---
with st.sidebar:
    st.image(URL_LOGO_MIAA, use_container_width=True)
    if st.button("♻️ Actualizar Datos", use_container_width=True):
        st.cache_data.clear()
        st.cache_resource.clear()
        st.rerun()
    st.divider()

    with st.expander("📅 RANGO DE FECHAS", expanded=True):
        opcion_rango = st.selectbox("Rango", ["Este mes", "Última semana", "Mes pasado", "Últimos 6 meses", "Este año", "Año pasado", "Personalizado"], index=0)
        if opcion_rango == "Este mes": dr = (inicio_mes_actual, ahora)
        elif opcion_rango == "Última semana": dr = (ahora - pd.Timedelta(days=7), ahora)
        elif opcion_rango == "Mes pasado": dr = (inicio_mes_pasado, ultimo_dia_mes_pasado)
        elif opcion_rango == "Últimos 6 meses": dr = (ahora - pd.DateOffset(months=6), ahora)
        elif opcion_rango == "Este año": dr = (inicio_año_actual, ahora)
        elif opcion_rango == "Año pasado": dr = (inicio_año_pasado, fin_año_pasado)
        else: dr = (inicio_mes_actual, ahora)
        try:
            fecha_rango = st.date_input("Periodo", value=dr, max_value=ahora, format="DD/MM/YYYY", label_visibility="collapsed")
        except: st.stop()

    if len(fecha_rango) == 2:
        df_hes = pd.read_sql(f"SELECT * FROM HES WHERE Fecha BETWEEN '{fecha_rango[0]}' AND '{fecha_rango[1]}'", mysql_engine)
        with st.expander("🔍 FILTROS DE BÚSQUEDA", expanded=False):
            mapeo = {"ClienteID_API": "Cliente", "Metodoid_API": "Metodo", "Medidor": "Medidor", "Predio": "Predio", "Colonia": "Colonia", "Giro": "Giro", "Sector": "Sector"}
            filtros_activos = {}
            for col_real, nombre in mapeo.items():
                if col_real in df_hes.columns:
                    raw = df_hes[col_real].unique()
                    opciones = sorted([str(int(float(x))) if str(x).replace('.0', '').isdigit() else str(x) for x in raw if pd.notnull(x) and str(x).strip() not in ['0', '0.0', '']])
                    c_tit, c_sel = st.columns([1, 2])
                    with c_tit: st.markdown(f"<p style='margin-top:8px; font-weight:bold; font-size:14px;'>{nombre}</p>", unsafe_allow_html=True)
                    with c_sel: seleccion = st.multiselect("", options=opciones, key=f"f_{col_real}", label_visibility="collapsed")
                    filtros_activos[col_real] = seleccion
                    if seleccion: df_hes = df_hes[df_hes[col_real].astype(str).str.replace('.0', '', regex=False).isin(seleccion)]

        with st.expander("🏆 RANKING TOP 10", expanded=True):
            if not df_hes.empty:
                rk = df_hes.groupby('Medidor')['Consumo_diario'].sum().sort_values(ascending=False).head(10).reset_index()
                max_c = rk['Consumo_diario'].max() or 1
                st.markdown("<div style='margin-top:15px;'></div>", unsafe_allow_html=True)
                for _, row in rk.iterrows():
                    try: m_limpio = str(int(float(row['Medidor'])))
                    except: m_limpio = str(row['Medidor'])
                    rc1, rc2, rc3 = st.columns([1.2, 0.7, 1.1])
                    rc1.markdown(f"<p style='font-size: 16px; font-weight: 800; color: #81D4FA; margin-bottom: 12px;'>{m_limpio}</p>", unsafe_allow_html=True)
                    rc2.markdown(f"<p style='font-size: 16px; font-weight: 800; color: white; text-align: right; margin-bottom: 12px;'>{row['Consumo_diario']:,.0f}</p>", unsafe_allow_html=True)
                    pct = (row['Consumo_diario'] / max_c) * 100
                    rc3.markdown(f'''<div style="display: flex; align-items: center; height: 24px; margin-bottom: 12px;"><div style="width: 100%; background-color: #262626; height: 16px; border-radius: 4px; overflow: hidden;"><div style="width: {pct}%; background-color: #FF0000; height: 16px; border-radius: 4px;"></div></div></div>''', unsafe_allow_html=True)
                st.markdown("<div style='margin-bottom: 20px;'></div>", unsafe_allow_html=True)
            else: st.write("Sin datos")
        st.markdown('<div style="background-color: #B22222; padding: 10px; border-radius: 5px; text-align: center; margin-top: 20px; font-weight: bold;">⚠️ INFORME ALARMAS</div>', unsafe_allow_html=True)
    else: st.stop()

# PROCESAMIENTO
agg = {c: f for c, f in {'Consumo_diario': 'sum', 'Lectura': 'last', 'Latitud': 'first', 'Longitud': 'first', 'Nivel': 'first', 'ClienteID_API': 'first', 'Nombre': 'first', 'Predio': 'first', 'Domicilio': 'first', 'Colonia': 'first', 'Giro': 'first', 'Sector': 'first', 'Metodoid_API': 'first', 'Primer_instalacion': 'first', 'Fecha': 'last'}.items() if c in df_hes.columns}
df_mapa = df_hes.groupby('Medidor').agg(agg).reset_index()
df_coords = df_mapa[(df_mapa['Latitud'] != 0) & (df_mapa['Longitud'] != 0) & (df_mapa['Latitud'].notnull())]
lat_c, lon_c, zoom = (df_coords['Latitud'].mean(), df_coords['Longitud'].mean(), 14) if not df_coords.empty and (filtros_activos.get("Colonia") or filtros_activos.get("Sector")) else (21.8853, -102.2916, 12)

# --- DASHBOARD PRINCIPAL ---
st.markdown('<div class="titulo-superior">Medidores inteligentes - Tablero de consumos</div>', unsafe_allow_html=True)

st.markdown('<div class="marco-tablero">', unsafe_allow_html=True)
m1, m2, m3, m4 = st.columns(4)
m1.metric("📟 N° de medidores", f"{len(df_mapa):,}")
m2.metric("💧 Consumo total", f"{df_hes['Consumo_diario'].sum():,.1f} m³")
m3.metric("📈 Promedio diario", f"{df_hes['Consumo_diario'].mean():.2f} m³")
m4.metric("📋 Total lecturas", f"{len(df_hes):,}")

st.markdown("<br>", unsafe_allow_html=True)
col_map, col_der = st.columns([3, 1.2])

with col_map:
    m = folium.Map(location=[lat_c, lon_c], zoom_start=zoom, tiles="CartoDB dark_matter")
    Fullscreen(position="topright", force_separate_button=True).add_to(m)
    fg_s, fg_m = folium.FeatureGroup(name="Sectores"), folium.FeatureGroup(name="Medidores")
    
    if not df_sec.empty:
        for _, r in df_sec.iterrows():
            folium.GeoJson(json.loads(r['geojson_data']), style_function=lambda x: {'fillColor': '#00d4ff', 'color': '#00d4ff', 'weight': 1, 'fillOpacity': 0.1}).add_to(fg_s)

    for _, r in df_mapa.iterrows():
        if pd.notnull(r['Latitud']) and pd.notnull(r['Longitud']):
            color_hex, etiqueta = get_color_logic(r.get('Nivel'), r.get('Consumo_diario', 0))
            
            # --- TU TOOLTIP HTML ORIGINAL RESTAURADO ---
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
            folium.CircleMarker([r['Latitud'], r['Longitud']], radius=3, color=color_hex, fill=True, fill_opacity=0.9, tooltip=folium.Tooltip(tooltip_html, sticky=True)).add_to(fg_m)

    fg_s.add_to(m); fg_m.add_to(m)
    folium.LayerControl(collapsed=False).add_to(m)
    folium_static(m, width=900, height=550)

    st.markdown("""<div class="map-legend"><div class="legend-item"><div class="legend-color" style="background-color: #00FF00;"></div>REGULAR</div><div class="legend-item"><div class="legend-color" style="background-color: #32CD32;"></div>NORMAL</div><div class="legend-item"><div class="legend-color" style="background-color: #FF8C00;"></div>BAJO</div><div class="legend-item"><div class="legend-color" style="background-color: #FFFFFF; border: 1px solid #555;"></div>CERO</div><div class="legend-item"><div class="legend-color" style="background-color: #FF0000;"></div>MUY ALTO</div><div class="legend-item"><div class="legend-color" style="background-color: #B22222;"></div>ALTO</div></div>""", unsafe_allow_html=True)

with col_der:
    st.write("🟢 **Histórico Reciente**")
    st.dataframe(df_hes[['Fecha', 'Lectura', 'Consumo_diario']].tail(15).sort_values(by='Fecha', ascending=False), hide_index=True, use_container_width=True)

st.markdown('</div>', unsafe_allow_html=True)

# GRÁFICOS
st.divider()
if not df_hes.empty:
    df_d = df_hes.groupby('Fecha')['Consumo_diario'].sum().reset_index()
    fig1 = px.bar(df_d, x='Fecha', y='Consumo_diario', text_auto=',.2f', title="Consumo Total por Día", color_discrete_sequence=['#00d4ff'])
    fig1.update_layout(paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)', font_color="white", height=350)
    st.plotly_chart(fig1, use_container_width=True)

    df_t = df_mapa.sort_values(by='Consumo_diario', ascending=False)
    fig2 = px.bar(df_t, x='Medidor', y='Consumo_diario', title="Consumo por Medidor", color_discrete_sequence=['#00d4ff'])
    fig2.update_layout(paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)', font_color="white", height=350)
    fig2.update_xaxes(type='category')
    st.plotly_chart(fig2, use_container_width=True)
