# Se usa python:3.9-slim-bullseye en lugar de slim para asegurar Java 11
FROM python:3.9-slim-bullseye

WORKDIR /app

# Instalamos Java (indispensable para que PySpark pueda interactuar con el ecosistema de Spark/Kafka)
# y limpiamos el caché de apt para evitar que la imagen engorde innecesariamente.
RUN apt-get update && \
    apt-get install -y default-jre procps && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Definimos JAVA_HOME (Dependerá de la arquitectura, usualmente en Debian slim recae en este path)
ENV JAVA_HOME="/usr/lib/jvm/default-java"

RUN pip install --no-cache-dir pyspark==3.4.1 kafka-python pandas scikit-learn psycopg2-binary streamlit plotly

# El comando por defecto asegura que el contenedor no se apague y podamos entrar más tarde para ejecutar nuestros scripts
CMD ["sleep", "infinity"]
