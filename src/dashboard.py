import streamlit as st
import pandas as pd
import psycopg2
import plotly.express as px
import os

# ========================================== #
# 1. Ajustes base del Dashboard
# ========================================== #
st.set_page_config(page_title="Dashboard Antifraude", page_icon="🛡️", layout="wide")

# Credenciales Inyectadas (Si se corre usando variables de entorno o usa defect)
DB_HOST = os.getenv("POSTGRES_HOST", "postgres")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")
DB_NAME = os.getenv("POSTGRES_DB", "fraud_detection_db")
DB_USER = os.getenv("POSTGRES_USER", "fraud_admin")
DB_PASS = os.getenv("POSTGRES_PASSWORD", "super_secure_fraud_pwd_2026")

# ========================================== #
# 2. Extractor de Datos (Con caché para rendimiento)
# ========================================== #
# Refresca la capa térmica de caché pasados 5 segundos (comportamiento quasi-realtime)
@st.cache_data(ttl=5)
def load_data():
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASS
        )
        # Extraemos ordenado desde la BD para aligerar Pandas
        query = "SELECT * FROM analyzed_transactions ORDER BY timestamp DESC"
        df = pd.read_sql_query(query, conn)
        conn.close()
        
        if not df.empty:
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            
        return df
    except Exception as e:
        st.error(f"Fallo de conexión a la base de datos PostgreSQL: {e}")
        return pd.DataFrame()

# ========================================== #
# 3. Estructura Visual Principal
# ========================================== #
st.title("🛡️ Panel de Control Antifraude - Operativa B2B")

df = load_data()

# Bloqueo elegante si aún no hay datos parseados
if df.empty:
    st.warning("⚠️ Base de datos vacía o inalcanzable. Esperando a que el sistema de Spark Streaming procese transacciones...")
else:
    # --- BARRA LATERAL (Filtros Globales) --- #
    st.sidebar.markdown("## 🔧 Central de Mandos")
    
    # Inyector de volcado manual de caché
    if st.sidebar.button("🔄 Forzar Sincronización Realtime"):
        st.cache_data.clear()
        st.rerun()

    st.sidebar.markdown("---")
    st.sidebar.markdown("### Selecciona Paramatrización")
    pares_divisas = ["TODOS"] + list(df['currency_pair'].unique())
    divisa_target = st.sidebar.selectbox("Mercado FOREX:", pares_divisas)

    # Lógica de filtrado
    if divisa_target != "TODOS":
        df_view = df[df['currency_pair'] == divisa_target]
    else:
        df_view = df

    if df_view.empty:
         st.warning("No hay transacciones interceptadas para ese par de divisas.")
    else:
        # --- FILA SUPERIOR: KPIs --- #
        # Cálculo de contadores
        total_tx = len(df_view)
        volumen_total = df_view['amount'].sum()
        
        # Spark exportó: 1 (Normal) y -1 (Fraude / Outlier)
        df_view['Clasificacion'] = df_view['prediction_ml'].apply(lambda x: 'Fraude' if x == -1 else 'Normal')
        tx_fraudes = len(df_view[df_view['prediction_ml'] == -1])
        tasa_fraude = (tx_fraudes / total_tx) * 100

        c1, c2, c3 = st.columns(3)
        c1.metric("📌 Tx Procesadas (IA)", f"{total_tx:,}")
        c2.metric("💰 Volumen Estudiado", f"${volumen_total:,.2f}")
        
        # Color dinámico para la alarma si supera un umbral
        alerta_fraude = f"{tasa_fraude:,.2f}%"
        if tasa_fraude > 10.0:
            c3.error(f"🚨 Tasa Fraude Detectada: **{alerta_fraude}**")
        else:
            c3.metric("🚨 Tasa Fraude Detectada", alerta_fraude)

        st.markdown("---")

        # --- SEGUNDA FILA: GRÁFICOS INTERACTIVOS (Plotly) --- #
        col_plot, col_matrix = st.columns([2.5, 1])

        with col_plot:
            st.subheader("Radar de Transacciones en Tiempo Real (Outliers)")
            fig = px.scatter(
                df_view,
                x="timestamp", 
                y="amount", 
                color="Clasificacion",
                # Colores psicológicos para peligro/calma
                color_discrete_map={"Normal": "#5dade2", "Fraude": "#e74c3c"},
                hover_name="transaction_id",
                hover_data=["sender_id", "currency_pair"]
            )
            fig.update_layout(
                xaxis_title="Eje Lineal Temp (UTC)", 
                yaxis_title="Cuantía de Movimiento", 
                margin=dict(l=0, r=0, b=0, t=30),
                legend_title="Dictamen Algorítmico"
            )
            st.plotly_chart(fig, use_container_width=True)

        with col_matrix:
            st.subheader("Benchmarking / Test")
            st.markdown("<small>Contraste de la Inteligencia Artificial frente a la bandera del inyector de datos.</small>", unsafe_allow_html=True)
            
            # Evaluación: isolation forest (-1 fraude) vs variable sintética guardada silenciosamente
            # Esto es un clásico de Data Science (Machine Learning Validation)
            tp = len(df_view[(df_view['prediction_ml'] == -1) & (df_view['is_synthetic_fraud'] == True)])
            fp = len(df_view[(df_view['prediction_ml'] == -1) & (df_view['is_synthetic_fraud'] == False)])
            tn = len(df_view[(df_view['prediction_ml'] == 1) & (df_view['is_synthetic_fraud'] == False)])
            fn = len(df_view[(df_view['prediction_ml'] == 1) & (df_view['is_synthetic_fraud'] == True)])
            
            st.info(f"🟢 **Verdaderos Positivos** (Cazados): **{tp}**")
            st.warning(f"🟡 **Falsos Positivos** (Inocentes): **{fp}**")
            st.success(f"🔵 **Verdaderos Negativos** (Normales): **{tn}**")
            st.error(f"🔴 **Falsos Negativos** (K.O - Escaparon): **{fn}**")

        st.markdown("---")
        
        # --- TABLA INFERIOR: FILTRO DE INVESTIGADORES --- #
        st.subheader("📋 Consola Táctica de Alertas Severas")
        st.write("Vista acotada de las últimas 20 operativas marcadas como anómalas para intervención humana.")
        
        # Filtramos la tabla al personal de compliance
        df_alertas_top = df_view[df_view['prediction_ml'] == -1].head(20).copy()
        
        if not df_alertas_top.empty:
            columnas_bonitas = ['timestamp', 'transaction_id', 'sender_id', 'amount', 'currency_pair', 'merchant_category']
            # Reorganizamos y limpiamos índices base
            st.dataframe(df_alertas_top[columnas_bonitas].reset_index(drop=True), use_container_width=True)
        else:
            st.success("🎉 Sistema en verde. Ninguna intervención requerida por tu departamento actualmente.")
