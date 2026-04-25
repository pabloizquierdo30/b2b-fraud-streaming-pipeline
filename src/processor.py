import os

# Parche imprescindible para ejecutar Spark 3.x sobre Java 17+ nativo de Debian 12
os.environ["JDK_JAVA_OPTIONS"] = (
    "--add-opens=java.base/java.lang=ALL-UNNAMED "
    "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED "
    "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED "
    "--add-opens=java.base/java.io=ALL-UNNAMED "
    "--add-opens=java.base/java.net=ALL-UNNAMED "
    "--add-opens=java.base/java.nio=ALL-UNNAMED "
    "--add-opens=java.base/java.util=ALL-UNNAMED "
    "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED "
    "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED "
    "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED "
    "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED "
    "--add-opens=java.base/sun.security.action=ALL-UNNAMED "
    "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED "
    "--add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED"
)

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, BooleanType
import pandas as pd
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler

# ========================================== #
# Configuración de Base de Datos (PostgreSQL)
# ========================================== #
DB_USER = os.getenv("POSTGRES_USER", "fraud_admin")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD", "super_secure_fraud_pwd_2026")
DB_DB = os.getenv("POSTGRES_DB", "fraud_detection_db")
# Usamos el contenedor docker 'postgres' y su puerto interno 5432
DB_URL = f"jdbc:postgresql://postgres:5432/{DB_DB}"

# ========================================== #
# Buffer de Memoria (Sliding Window ML)
# ========================================== #
global_data_buffer = []

def process_micro_batch(batch_df, batch_id):
    """
    Función que recibe un micro-batch (DataFrame de Spark) y su Identificador.
    Aplica el modelo de IA con contexto estadístico y guarda el resultado en BD.
    """
    global global_data_buffer
    
    # Si el batch está vacío (sin eventos nuevos de Kafka), lo pasamos por alto
    if batch_df.count() == 0:
        return
        
    print("============================================")
    print(f"📦 Procesando micro-batch ID: {batch_id} con {batch_df.count()} transacciones")
    print("============================================")

    # 1. Transformación a Pandas de los datos actuales
    pdf = batch_df.toPandas()
    
    # 2. Acumulación (Sliding Window). Límite de 1000 registros para no explotar la RAM
    global_data_buffer.extend(pdf.to_dict('records'))
    global_data_buffer = global_data_buffer[-1000:]
    
    # 3. DataFrame de Contexto Total
    ctx_df = pd.DataFrame(global_data_buffer)
    
    # 4. Feature Engineering sobre todo el contexto
    # (Al hacerlo sobre el contexto evitamos offsets de variables categóricas)
    ctx_df['currency_numeric'] = ctx_df['currency_pair'].astype('category').cat.codes
    X_context = ctx_df[['amount', 'currency_numeric']].fillna(0)
    
    # 5. Escalado de Características (Senior Fix)
    scaler = StandardScaler()
    X_context_scaled = scaler.fit_transform(X_context)
    
    # 6. Entrenamiento con Contexto Estadístico (IsolationForest)
    model = IsolationForest(contamination=0.05, random_state=42)
    model.fit(X_context_scaled)
    
    # 7. Predicción aislada: filtramos sólo las características del batch entrante
    batch_size = len(pdf)
    X_current_scaled = X_context_scaled[-batch_size:]
    
    # -1 para fraude, 1 para normal
    pdf['prediction_ml'] = model.predict(X_current_scaled)
    
    # 5. Volver a transformar a Spark DataFrame
    # Recuperamos la sesión activa desde el dataframe original
    spark = batch_df.sparkSession
    result_df = spark.createDataFrame(pdf)
    
    # 6. Almacenamiento Persistente en PostgreSQL
    try:
        result_df.write \
            .format("jdbc") \
            .option("url", DB_URL) \
            .option("dbtable", "analyzed_transactions") \
            .option("user", DB_USER) \
            .option("password", DB_PASSWORD) \
            .option("driver", "org.postgresql.Driver") \
            .mode("append") \
            .save()
            
        print(f"✅ Micro-batch {batch_id} insertado con éxito en PostgreSQL (analyzed_transactions).")
    except Exception as e:
        print(f"❌ Error al guardar en PostgreSQL el batch {batch_id}: {str(e)}")


def main():
    print("🚀 Levantando PySpark Structured Streaming Processor...")
    import pyspark
    spark_version = pyspark.__version__
    kafka_pkg = f"org.apache.spark:spark-sql-kafka-0-10_2.12:{spark_version}"
    
    # 1. Inicializar Spark Session
    # Se añade de manera dinámica el paquete oficial de Kafka para Spark y el driver JDBC de PostgreSQL
    spark = SparkSession.builder \
        .appName("B2B_Fraud_Detection_Streaming") \
        .config("spark.jars.packages", f"{kafka_pkg},org.postgresql:postgresql:42.6.0") \
        .getOrCreate()
        
    # Reducimos los logs de Spark para evitar que inunden la terminal
    spark.sparkContext.setLogLevel("WARN")

    # 2. Definición estricta del Esquema JSON entrante
    json_schema = StructType([
        StructField("transaction_id", StringType(), True),
        StructField("sender_id", StringType(), True),
        StructField("receiver_id", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("currency_pair", StringType(), True),
        StructField("merchant_category", StringType(), True),
        StructField("timestamp", StringType(), True),  # Guardamos como string, Postgres se encarga del casteo final
        StructField("is_synthetic_fraud", BooleanType(), True)
    ])

    # 3. Conexión de Lectura (Source: Kafka)
    df_stream = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "b2b_transactions") \
        .option("startingOffsets", "latest") \
        .load()

    # 4. Parseo de Kafka (Bytes) a Columnas JSON estructuradas
    parsed_df = df_stream.selectExpr("CAST(value AS STRING) as json_payload") \
        .select(F.from_json("json_payload", json_schema).alias("data")) \
        .select("data.*")

    # 5. Pipeline de Procesamiento por Lotes y Sink Postgres
    # Utilizamos checkpointLocation temporal en /tmp para persistir el estado de streaming de forma resiliente
    query = parsed_df \
        .writeStream \
        .foreachBatch(process_micro_batch) \
        .option("checkpointLocation", "/tmp/spark_checkpoint_fraud") \
        .start()

    print("📡 Streaming IA a la escucha. ¡Inicia el generador para ver el análisis de fraudes!")
    
    # Mantenemos el listener bloqueante abierto
    query.awaitTermination()

if __name__ == "__main__":
    main()
