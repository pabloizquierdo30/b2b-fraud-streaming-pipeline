# 🛡️ B2B Real-Time Fraud Detection Pipeline

> Un ecosistema de streaming analítico end-to-end diseñado para interceptar anomalías transaccionales en pagos transfronterizos B2B en tiempo real.

![Python](https://img.shields.io/badge/Python-3.9-blue?style=flat&logo=python&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?style=flat&logo=docker&logoColor=white)
![Apache Kafka](https://img.shields.io/badge/Apache_Kafka-Confluent-231F20?style=flat&logo=apachekafka)
![Apache Spark](https://img.shields.io/badge/PySpark-Structured_Streaming-E25A1C?style=flat&logo=apachespark&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-4169E1?style=flat&logo=postgresql&logoColor=white)
![Streamlit](https://img.shields.io/badge/Streamlit-Dashboard-FF4B4B?style=flat&logo=streamlit&logoColor=white)

---

## 💼 El Caso de Negocio (Business Context)

En el actual panorama financiero global, las compañías especializadas tanto en la gestión de **pagos transfronterizos internacionales** como en la **cobertura de riesgo de divisas (FX)** enfrentan el desafío de auditar operaciones altamente volátiles y multimodales. Los enfoques de validación en modo lote (*Batch*) ejecutados a final del día dejan una ventana abierta inaceptable frente a amenazas cada vez más rápidas, generando un grave riesgo regulatorio y perjuicio a tesorería.

Este proyecto modeliza una solución nativa para compañías FinTech líderes del sector que busquen escalabilidad y latencia milimétrica. Gracias al despliegue de una arquitectura de Big Data orientada a eventos asíncronos y enriquecida con Machine Learning no supervisado, el sistema monitoriza las operativas corporativas al vuelo. El clúster identifica desviaciones de volumen o arbitraje ilícito en moneda exótica con rigor estadístico, proveyendo al equipo de *Compliance* de una capa de protección invisible, escalable y de acción inmediata.

---

## 🏗️ Arquitectura y Flujo de Datos (Architecture & Data Flow)

El pipeline descansa sobre cinco componentes distribuidos bajo una red local micro-segmentada de Docker:

*   **🏭 Generador (`producer.py`)**: Simulación determinista de flujos B2B entre corporaciones, programada para inyectar anomalías algorítmicas controladas (montos atípicos, divisas raras) simulando a un actor malicioso.
*   **📡 Ingesta de Eventos (Apache Kafka)**: Cluster configurado (Confluent/Zookeeper) sirviendo como un bus de mensajes de alta disponibilidad.
*   **🧠 Procesamiento IA en Streaming (`processor.py`)**: Core analítico ejecutando **PySpark Structured Streaming**. Para evitar el "Síndrome de Micro-batch Blindness", integra al vuelo un modelo **Isolation Forest (Scikit-Learn)** alimentado con *Sliding Window Buffers* y preprocesado (StandardScaler), garantizando análisis robusto frente al contexto general.
*   **🗄️ Persistencia de Auditoría (PostgreSQL)**: Servidor relacional sincronizado como sumidero JDBC para centralizar e historificar cada lote predecido y sus dictámenes algorítmicos.
*   **📊 Plataforma Táctica (`dashboard.py`)**: Interfaz web interactiva construida en **Streamlit y Plotly**. Ofrece telemetría instantánea y una consola de alertas para los equipos humanos de seguridad. 

---

## 📂 Estructura del Proyecto

```text
fraud-detection-project/
├── .gitignore             # Reglas de exclusión para seguridad y archivos temporales
├── docker-compose.yml     # Definición de infraestructura unificada (Spark, Kafka, etc.)
├── Dockerfile             # Configuración inmutable (Python 3.9 + Java 11)
├── README.md              # Documentación del proyecto
└── src/
    ├── dashboard.py       # Front-End y Analítica de Datos (Streamlit)
    ├── processor.py       # Core Inteligencia Artificial y Streaming (PySpark)
    └── producer.py        # Simulación y empaquetador Kafka

* Nota: El archivo .env de credenciales se omite del repositorio por seguridad.
```

---

## 🚀 Quick Start (Instalación y Ejecución)

El entorno se ha empaquetado para ejecutarse en cualquier sistema operativo host sin fricciones relativas a dependencias de JVM o librerías dinámicas, a través de Docker.

**1. Despliegue de la Infraestructura Central:**
Arranca todos los motores subyacentes con un único comando:
```bash
docker compose up -d --build
```

**2. Arranque del Panel de Control (Dashboard):**
Entraremos en la máquina virtual cliente (app-container) y montaremos la interfaz web ininterrumpida.
```bash
docker exec -it app-container bash
streamlit run src/dashboard.py
```
> 👉 *Navega a http://localhost:8501 en tu navegador habitual.*

**3. Arranque del Entorno Financiero MOCK (Generador de Transacciones):**
En una *nueva* pestaña de tu terminal, enciende el flujo de simulaciones:
```bash
docker exec -it app-container bash
python src/producer.py
```

**4. Intercepción Biometral IA (Core Streaming):**
En una *última* pestaña adicional, despliega el analizador de PySpark para auditar la tubería de eventos:
```bash
docker exec -it app-container bash
python src/processor.py
```

*Verás en vivo cómo los micro-batches se interceptan, se estudian y se insertan a la base de PostgreSQL, poblando el Dashboard de métricas validadas.*

---

## 🔮 Futuras Optimizaciones (Future Work)

El diseño de este *Proof of Concept* es altamente modular, habiendo previsto los siguientes *upgrades* orientados a DataOps y MLOps:

*   **Implementación de CI/CD & Unit Testing:** Creación de *workflows* en GitHub Actions ejecutando flujos de tests para la validación del transformador Pandas-UDF sobre PySpark.
*   **Despliegue Cloud-Native:** Migración de los volúmenes físicos hacia Managed Streaming for Apache Kafka (MSK) en AWS, acoplando el análisis a AWS EMR o Databricks.
*   **Supervisión Semántica (Deep Learning):** Reemplazar Isolation Forest por algoritmos semi-supervisados (Autoencoders Neural Networks) para rastrear intersecuencias fraudulentas cronológicas de manera superior.

---
📝 *Desarrollado como prototipo técnico para el sector FinTech y Operaciones internacionales B2B.*
