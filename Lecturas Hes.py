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

# ESTILO CSS PARA COMPACTAR ESPACIADO VERTICAL
st.markdown("""
    <style>
        .stApp { background-color: #000000 !important; color: white; }
        section[data-testid="stSidebar"] { background-color: #111111 !important; }
        
        /* 1. Reducir espacio entre bloques verticales en el sidebar */
        [data-testid="stSidebarUserContent"] div[data-testid="stVerticalBlock"] > div {
            padding-bottom: 0px !important;
            padding-top: 0px !important;
            margin-bottom: -5px !important;
        }

        /* 2. Ajustar alineación de la etiqueta del filtro */
        [data-testid="stWidgetLabel"] p {
            font-size: 14px !important;
            margin-bottom: 0px !important;
        }

        /* 3. Quitar márgenes excedentes de los selectores */
        .stMultiSelect {
            margin-bottom: 0px !important;
        }
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

# --- LÓGICA DE FECHAS EN ESPAÑOL ---
# --- LÓGICA DE FECHAS DINÁMICAS ---
ahora = pd.Timestamp.now()
inicio_mes_actual = ahora.replace(day=1)

# Cálculos de Mes Pasado
ultimo_dia_mes_pasado = inicio_mes_actual - pd.Timedelta(days=1)
inicio_mes_pasado = ultimo_dia_mes_pasado.replace(day=1)

# Cálculos de Años
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

    # Selección de rango rápido con todas las opciones solicitadas
    st.write("**📅 Selecciona un rango**")
    opcion_rango = st.selectbox(
        "Rango predefinido",
        [
            "Este mes", 
            "Última semana",
            "Mes pasado", 
            "Últimos 6 meses",
            "Este año", 
            "Año pasado", 
            "Personalizado"
        ],
        index=0, # Inicia por defecto en "Este mes"
        label_visibility="collapsed"
    )

    # Lógica de asignación de fechas basada en la selección
    if opcion_rango == "Este mes":
        default_range = (inicio_mes_actual, ahora)
    elif opcion_rango == "Última semana":
        default_range = (ahora - pd.Timedelta(days=7), ahora)
    elif opcion_rango == "Mes pasado":
        default_range = (inicio_mes_pasado, ultimo_dia_mes_pasado)
    elif opcion_rango == "Últimos 6 meses":
        default_range = (ahora - pd.DateOffset(months=6), ahora)
    elif opcion_rango == "Este año":
        default_range = (inicio_año_actual, ahora)
    elif opcion_rango == "Año pasado":
        default_range = (inicio_año_pasado, fin_año_pasado)
    else:
        # Rango base para la opción personalizada
        default_range = (inicio_mes_actual, ahora)

    # Calendario amigable en español
    try:
        fecha_rango = st.date_input(
            "Periodo de consulta",
            value=default_range,
            max_value=ahora,
            format="DD/MM/YYYY",
            label_visibility="collapsed"
        )
    except:
        st.stop()
    
    if len(fecha_rango) == 2:
        df_hes = pd.read_sql(f"SELECT * FROM HES WHERE Fecha BETWEEN '{fecha_rango[0]}' AND '{fecha_rango[1]}'", mysql_engine)
        
        # Filtros compactos horizontales con alineación mejorada
        st.markdown("<br>", unsafe_allow_html=True)
        filtros_sidebar = ["ClientID_API", "Metodoid_API", "Medidor", "Predio", "Colonia", "Giro", "Sector"]
        filtros_activos = {}
        
        for col in filtros_sidebar:
            if col in df_hes.columns:
                opciones = sorted(df_hes[col].unique().astype(str).tolist())
                c1, c2 = st.columns([1, 2])
                with c1:
                    # Margen de 10px para que el texto no "flote" arriba de la caja
                    st.markdown(f"<p style='margin-top:10px; font-size: 14px;'>{col}</p>", unsafe_allow_html=True)
                with c2:
                    seleccion = st.multiselect("", options=opciones, key=f"f_{col}", label_visibility="collapsed")
                
                filtros_activos[col] = seleccion
                if seleccion:
                    df_hes = df_hes[df_hes[col].astype(str).isin(seleccion)]

        st.divider()
        
        # --- RANKING TOP CONSUMO (ESTILO ORIGINAL) ---
        st.write("**Ranking Top 10 Consumo**")
        if not df_hes.empty:
            ranking_data = df_hes.groupby('Medidor')['Consumo_diario'].sum().sort_values(ascending=False).head(10).reset_index()
            max_c = ranking_data['Consumo_diario'].max() if not ranking_data.empty else 1
            
            for _, row in ranking_data.iterrows():
                rc1, rc2 = st.columns([1, 1])
                rc1.markdown(f"<span style='color: #81D4FA; font-size: 12px;'>{row['Medidor']}</span>", unsafe_allow_html=True)
                
                pct = (row['Consumo_diario'] / max_c) * 100
                rc2.markdown(f"""
                    <div style="display: flex; align-items: center; justify-content: flex-end;">
                        <span style="font-size: 11px; margin-right: 5px;">{row['Consumo_diario']:,.0f}</span>
                        <div style="width: 40px; background-color: #333; height: 8px; border-radius: 2px;">
                            <div style="width: {pct}%; background-color: #FF0000; height: 8px; border-radius: 2px;"></div>
                        </div>
                    </div>
                    """, unsafe_allow_html=True)

        st.markdown('<div style="background-color: #444; padding: 10px; border-radius: 5px; text-align: center; margin: 15px 0;">⚠️ <b>Informe alarmas</b></div>', unsafe_allow_html=True)
    else:
        st.stop()

# --- PROCESAMIENTO ---
mapeo_columnas = {
    'Consumo_diario': 'sum', 
    'Lectura': 'last', 
    'Latitud': 'first', 
    'Longitud': 'first',
    'Nivel': 'first', 
    'ClientID_API': 'first', # Mantiene el ID del cliente tras el agrupamiento
    'Nombre': 'first', 
    'Predio': 'first',
    'Domicilio': 'first', 
    'Colonia': 'first', 
    'Giro': 'first', 
    'Sector': 'first',
    'Metodoid_API': 'first', 
    'Primer_instalacion': 'first', 
    'Fecha': 'last'
}

# Filtramos solo las columnas que realmente existen en el DataFrame actual
agg_segura = {col: func for col, func in mapeo_columnas.items() if col in df_hes.columns}
df_mapa = df_hes.groupby('Medidor').agg(agg_segura).reset_index()

# LÓGICA DE ZOOM DINÁMICO
df_valid_coords = df_mapa[(df_mapa['Latitud'] != 0) & (df_mapa['Longitud'] != 0) & (df_mapa['Latitud'].notnull())]

if not df_valid_coords.empty and (filtros_activos.get("Colonia") or filtros_activos.get("Sector")):
    lat_centro, lon_centro, zoom_inicial = df_valid_coords['Latitud'].mean(), df_valid_coords['Longitud'].mean(), 14
else:
    lat_centro, lon_centro, zoom_inicial = 21.8853, -102.2916, 12

# 5. DASHBOARD - VISUALIZACIÓN
st.title("Medidores inteligentes - Tablero de consumos")

# Métricas principales
m1, m2, m3, m4 = st.columns(4)
m1.metric("N° de medidores", f"{len(df_mapa):,}")
m2.metric("Consumo acumulado m3", f"{df_hes['Consumo_diario'].sum():,.1f}" if 'Consumo_diario' in df_hes.columns else "0")
m3.metric("Promedio diario m3", f"{df_hes['Consumo_diario'].mean():.2f}" if 'Consumo_diario' in df_hes.columns else "0")
m4.metric("Lecturas", f"{len(df_hes):,}")

col_map, col_der = st.columns([3, 1.2])

with col_map:
    # Creación del mapa base
    m = folium.Map(location=[lat_centro, lon_centro], zoom_start=zoom_inicial, tiles="CartoDB dark_matter")
    
    # Capa de Sectores Hidrométricos (GeoJSON)
    if not df_sec.empty:
        for _, row in df_sec.iterrows():
            geojson_obj = json.loads(row['geojson_data'])
            folium.GeoJson(
                geojson_obj,
                style_function=lambda x: {'fillColor': '#00d4ff', 'color': '#00d4ff', 'weight': 1, 'fillOpacity': 0.1},
                highlight_function=lambda x: {'fillColor': '#ffff00', 'color': '#ffff00', 'weight': 3, 'fillOpacity': 0.4},
                tooltip=folium.Tooltip(f"Sector: {row['sector']}", sticky=True)
            ).add_to(m)

    # Capa de Marcadores de Medidores con Popup Detallado
    for _, r in df_mapa.iterrows():
        if pd.notnull(r['Latitud']) and pd.notnull(r['Longitud']):
            color_hex, etiqueta = get_color_logic(r.get('Nivel'), r.get('Consumo_diario', 0))
            
            # --- CORRECCIÓN PARA EL NÚMERO DE CLIENTE ---
            # Forzamos a que si el dato existe en el DataFrame, se convierta en texto y no se pierda
            val_cliente = r.get('ClientID_API')
            txt_cliente = str(int(float(val_cliente))) if pd.notnull(val_cliente) and str(val_cliente).strip() != "" else "N/A"
            
            # Construcción del Popup con estilo profesional
            pop_html = f"""
            <div style='font-family: Arial, sans-serif; font-size: 12px; width: 300px; color: #333; line-height: 1.4;'>
                <h5 style='margin:0 0 8px 0; color: #007bff; border-bottom: 1px solid #ccc; padding-bottom: 3px;'>Detalle del Medidor</h5>
                <b>Cliente:</b> {txt_cliente} - <b>Serie:</b> {r['Medidor']}<br>
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
                radius=4, 
                color=color_hex, 
                fill=True, 
                fill_opacity=0.9,
                popup=folium.Popup(pop_html, max_width=350)
            ).add_to(m)
    
    # Renderizar mapa
    folium_static(m, width=900, height=550)

with col_der:
    st.write("🟢 **Histórico Reciente**")
    if not df_hes.empty:
        st.dataframe(
            df_hes[['Fecha', 'Lectura', 'Consumo_diario']].tail(15).sort_values(by='Fecha', ascending=False), 
            hide_index=True,
            use_container_width=True
        )
    else:
        st.info("No hay lecturas para el periodo seleccionado.")

if st.button("🔄 Reiniciar Tablero", use_container_width=True):
    reiniciar_tablero()











