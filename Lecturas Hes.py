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
import io
import base64
import matplotlib.pyplot as plt

# 1. CONFIGURACIÓN
st.set_page_config(
    page_title="MIAA - Tablero de Consumos",
    page_icon="https://www.miaa.mx/favicon.ico", 
    layout="wide"  
)

# ESTILO CSS (Incluye el diseño de las barras y el mapa)
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
        iframe[title="streamlit_folium.folium_static"] {
            border: 3px solid #444444 !important;
            border-radius: 10px;
            box-shadow: 0px 4px 15px rgba(0, 0, 0, 0.5);
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
        st.error(f"Error MySQL: {e}")
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

# FUNCIÓN PARA GENERAR GRÁFICO BASE64 (DENTRO DEL TOOLTIP)
def generar_grafico_base64(df_medidor):
    if df_medidor.empty: return ""
    df_plot = df_medidor.sort_values('Fecha')
    fig, ax = plt.subplots(figsize=(4, 2), dpi=80)
    fig.patch.set_facecolor('#f8f9fa')
    ax.set_facecolor('#f8f9fa')
    ax.plot(df_plot['Fecha'], df_plot['Consumo_diario'], color='#007bff', marker='o', markersize=4, linewidth=2)
    ax.fill_between(df_plot['Fecha'], df_plot['Consumo_diario'], color='#007bff', alpha=0.1)
    ax.tick_params(labelsize=8)
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    plt.xticks(rotation=45)
    plt.tight_layout()
    buf = io.BytesIO()
    plt.savefig(buf, format='png', bbox_inches='tight')
    plt.close(fig)
    return f"data:image/png;base64,{base64.b64encode(buf.getbuffer()).decode('ascii')}"

# CARGA DE DATOS
mysql_engine = get_mysql_engine()
df_sec = get_sectores_cached()

ahora = pd.Timestamp.now()
inicio_mes_actual = ahora.replace(day=1)
ultimo_dia_mes_pasado = inicio_mes_actual - pd.Timedelta(days=1)
inicio_mes_pasado = ultimo_dia_mes_pasado.replace(day=1)

with st.sidebar:
    st.image(URL_LOGO_MIAA, use_container_width=True)
    if st.button("♻️ Actualizar Datos", use_container_width=True):
        st.cache_data.clear(); st.cache_resource.clear(); st.rerun()
    st.divider()

    with st.expander("📅 RANGO DE FECHAS", expanded=True):
        opcion_rango = st.selectbox("Rango", ["Este mes", "Última semana", "Mes pasado", "Personalizado"], index=0)
        default_range = (inicio_mes_actual, ahora)
        if opcion_rango == "Última semana": default_range = (ahora - pd.Timedelta(days=7), ahora)
        elif opcion_rango == "Mes pasado": default_range = (inicio_mes_pasado, ultimo_dia_mes_pasado)
        
        try:
            fecha_rango = st.date_input("Periodo", value=default_range, max_value=ahora, format="DD/MM/YYYY", label_visibility="collapsed")
        except: st.stop()
    
    if len(fecha_rango) == 2:
        df_hes = pd.read_sql(f"SELECT * FROM HES WHERE Fecha BETWEEN '{fecha_rango[0]}' AND '{fecha_rango[1]}'", mysql_engine)
        
        with st.expander("🔍 FILTROS DE BÚSQUEDA", expanded=False):
            mapeo_nombres = {"ClienteID_API": "Cliente", "Metodoid_API": "Metodo", "Medidor": "Medidor", "Predio": "Predio", "Colonia": "Colonia", "Giro": "Giro", "Sector": "Sector"}
            filtros_activos = {}
            for col_real, nombre_amigable in mapeo_nombres.items():
                if col_real in df_hes.columns:
                    opciones = sorted([str(int(float(x))) if str(x).replace('.0', '').isdigit() else str(x) for x in df_hes[col_real].unique() if pd.notnull(x) and str(x).strip() not in ['0', '0.0', '']])
                    col_tit, col_sel = st.columns([1, 2])
                    with col_tit: st.markdown(f"<p style='margin-top:8px; font-weight:bold; font-size:14px;'>{nombre_amigable}</p>", unsafe_allow_html=True)
                    with col_sel: seleccion = st.multiselect("", options=opciones, key=f"f_{col_real}", label_visibility="collapsed")
                    filtros_activos[col_real] = seleccion
                    if seleccion: df_hes = df_hes[df_hes[col_real].astype(str).str.replace('.0', '', regex=False).isin(seleccion)]

        # SECCIÓN RANKING (RESTAURADA)
        with st.expander("🏆 RANKING TOP 10", expanded=True):
            if not df_hes.empty:
                ranking_data = df_hes.groupby('Medidor')['Consumo_diario'].sum().sort_values(ascending=False).head(10).reset_index()
                max_c = ranking_data['Consumo_diario'].max() if not ranking_data.empty else 1
                for _, row in ranking_data.iterrows():
                    try: med_limpio = str(int(float(row['Medidor'])))
                    except: med_limpio = str(row['Medidor'])
                    rc1, rc2, rc3 = st.columns([1.2, 0.7, 1.1])
                    rc1.markdown(f"<p style='font-size: 16px; font-weight: 800; color: #81D4FA; margin-bottom: 12px;'>{med_limpio}</p>", unsafe_allow_html=True)
                    rc2.markdown(f"<p style='font-size: 16px; font-weight: 800; color: white; text-align: right; margin-bottom: 12px;'>{row['Consumo_diario']:,.0f}</p>", unsafe_allow_html=True)
                    pct = (row['Consumo_diario'] / max_c) * 100
                    rc3.markdown(f'''<div style="display: flex; align-items: center; height: 24px; margin-bottom: 12px;"><div style="width: 100%; background-color: #262626; height: 16px; border-radius: 4px; overflow: hidden;"><div style="width: {pct}%; background-color: #FF0000; height: 16px; border-radius: 4px;"></div></div></div>''', unsafe_allow_html=True)
            else: st.write("Sin datos")
        st.markdown('<div style="background-color: #B22222; padding: 10px; border-radius: 5px; text-align: center; margin-top: 20px; font-weight: bold; letter-spacing: 1px;">⚠️ INFORME ALARMAS</div>', unsafe_allow_html=True)
    else: st.stop()

# PROCESAMIENTO
mapeo_columnas = {'Consumo_diario': 'sum', 'Lectura': 'last', 'Latitud': 'first', 'Longitud': 'first', 'Nivel': 'first', 'ClienteID_API': 'first', 'Nombre': 'first', 'Predio': 'first', 'Domicilio': 'first', 'Colonia': 'first', 'Giro': 'first', 'Sector': 'first', 'Metodoid_API': 'first', 'Primer_instalacion': 'first', 'Fecha': 'last'}
agg_segura = {col: func for col, func in mapeo_columnas.items() if col in df_hes.columns}
df_mapa = df_hes.groupby('Medidor').agg(agg_segura).reset_index()
df_v = df_mapa[(df_mapa['Latitud'] != 0) & (df_mapa['Latitud'].notnull())]

lat_c, lon_c, zoom = (df_v['Latitud'].mean(), df_v['Longitud'].mean(), 14) if not df_v.empty else (21.8853, -102.2916, 12)

# DASHBOARD
st.markdown('<div class="titulo-superior">Medidores inteligentes - Tablero de consumos</div>', unsafe_allow_html=True)
m1, m2, m3, m4 = st.columns(4)
m1.metric("📟 N° de medidores", f"{len(df_mapa):,}")
m2.metric("💧 Consumo total", f"{df_hes['Consumo_diario'].sum():,.1f} m³")
m3.metric("📈 Promedio diario", f"{df_hes['Consumo_diario'].mean():.2f} m³")
m4.metric("📋 Total lecturas", f"{len(df_hes):,}")

col_map, col_der = st.columns([3, 1.2])

with col_map:
    m = folium.Map(location=[lat_c, lon_c], zoom_start=zoom, tiles="CartoDB dark_matter")
    Fullscreen(position="topright", force_separate_button=True).add_to(m)
    
    fg_sectores = folium.FeatureGroup(name="Sectores Hidráulicos (QGIS)", show=True)
    fg_medidores = folium.FeatureGroup(name="Medidores Inteligentes", show=True)

    if not df_sec.empty:
        for _, row in df_sec.iterrows():
            folium.GeoJson(json.loads(row['geojson_data']), style_function=lambda x: {'fillColor': '#00d4ff', 'color': '#00d4ff', 'weight': 1, 'fillOpacity': 0.1}).add_to(fg_sectores)

    for _, r in df_mapa.iterrows():
        if pd.notnull(r['Latitud']) and pd.notnull(r['Longitud']):
            color_hex, etiqueta = get_color_logic(r.get('Nivel'), r.get('Consumo_diario', 0))
            img_b64 = generar_grafico_base64(df_hes[df_hes['Medidor'] == r['Medidor']])
            
            tooltip_html = f"""
            <div style='font-family: Arial; font-size: 12px; color: #333; width: 300px; padding: 10px;'>
                <h5 style='margin:0; color: #007bff; border-bottom: 1px solid #ccc;'>{r['Medidor']}</h5>
                <b>Cliente:</b> {r.get('ClienteID_API', 'N/A')}<br>
                <b>Nombre:</b> {r.get('Nombre', 'N/A')}<br>
                <b>Consumo:</b> {r.get('Consumo_diario', 0):,.2f} m³<br>
                <div style='text-align:center; background:{color_hex}22; border:1px solid {color_hex}; margin-top:5px;'>
                    <b style='color:{color_hex};'>{etiqueta}</b>
                </div>
                <img src='{img_b64}' style='width:100%; margin-top:10px; border-radius:5px;'>
            </div>
            """
            folium.CircleMarker(location=[r['Latitud'], r['Longitud']], radius=4, color=color_hex, fill=True, fill_opacity=0.9, tooltip=folium.Tooltip(tooltip_html)).add_to(fg_medidores)

    fg_sectores.add_to(m); fg_medidores.add_to(m)
    folium.LayerControl(position='topright', collapsed=False).add_to(m)
    folium_static(m, width=1000, height=650)

with col_der:
    st.write("🟢 **Histórico Reciente**")
    st.dataframe(df_hes[['Fecha', 'Lectura', 'Consumo_diario']].tail(15).sort_values(by='Fecha', ascending=False), hide_index=True, use_container_width=True)

# --- GRÁFICOS INFERIORES (RESTAURADOS) ---
st.divider()
if not df_hes.empty:
    df_diario = df_hes.groupby('Fecha')['Consumo_diario'].sum().reset_index()
    fig_diario = px.bar(df_diario, x='Fecha', y='Consumo_diario', text_auto=',.2f', color_discrete_sequence=['#00d4ff'])
    fig_diario.update_layout(title="Consumo Total por Día", paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)', font_color="white", height=350)
    st.plotly_chart(fig_diario, use_container_width=True)

    df_todos_med = df_mapa.sort_values(by='Consumo_diario', ascending=False)
    fig_med = px.bar(df_todos_med, x='Medidor', y='Consumo_diario', color_discrete_sequence=['#00d4ff'])
    fig_med.update_layout(title="Consumo por Medidor (Registros Totales)", paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)', font_color="white", height=350)
    fig_med.update_xaxes(type='category')
    st.plotly_chart(fig_med, use_container_width=True)
