import json
import time
import random
import uuid
from datetime import datetime, timezone
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

# ========================================== #
# Configuración Principal
# ========================================== #
KAFKA_BROKER = 'kafka:9092'
TOPIC_NAME = 'b2b_transactions'

# Constantes para la simulación B2B
NORMAL_CURRENCY_PAIRS = ['EUR/USD', 'GBP/EUR', 'USD/JPY', 'EUR/GBP', 'AUD/USD', 'USD/CAD']
HIGH_RISK_CURRENCY_PAIRS = ['USD/RUB', 'EUR/TRY', 'USD/VEF', 'EUR/ARS']
MERCHANT_CATEGORIES = [
    'Software Services', 'Manufacturing', 'Logistics', 
    'Consulting', 'Real Estate', 'Wholesale Export'
]

def create_kafka_producer():
    """
    Intenta establecer una conexión con Kafka aplicando reintentos dinámicos.
    Útil al arrancar en Docker Compose porque Kafka puede tardar en levantarse.
    """
    producer = None
    retries = 10
    
    print("==============================================")
    print("🚀 Iniciando Productor B2B (Data Simulation)")
    print("==============================================")

    while retries > 0:
        try:
            print(f"⌛ Intentando conectar al broker de Kafka en {KAFKA_BROKER} (Intentos restantes: {retries})...")
            producer = KafkaProducer(
                bootstrap_servers=[KAFKA_BROKER],
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            print("✅ ¡Conexión a Kafka establecida con éxito!")
            break
        except NoBrokersAvailable:
            retries -= 1
            print("⏳ Kafka aún no arranca. Reintentando en 5 segundos...")
            time.sleep(5)
            
    if not producer:
        print("❌ Error Crítico: No se pudo conectar a Kafka tras múltiples intentos. Abortando.")
        exit(1)
        
    return producer

def generate_transaction():
    """
    Motor de inyección de negocio:
    Genera transacciones financieras, introduciendo un 5% de fraude determinista (reglas de negocio duras).
    """
    is_fraud = random.random() < 0.05  # 5% de probabilidad
    
    transaction_id = str(uuid.uuid4())
    sender_id = f"CMP-{random.randint(10000, 99999)}"
    receiver_id = f"CMP-{random.randint(10000, 99999)}"
    timestamp = datetime.now(timezone.utc).isoformat()
    merchant_category = random.choice(MERCHANT_CATEGORIES)
    
    if is_fraud:
        # Lógica de anomalía comercial: montos masivos o divisas de alto riesgo
        if random.random() < 0.5:
            # Fraude por capital masivo inesperado
            amount = round(random.uniform(500000.0, 5000000.0), 2)
            currency_pair = random.choice(NORMAL_CURRENCY_PAIRS)
        else:
            # Fraude por ruta exótica/sancionada
            amount = round(random.uniform(5000.0, 50000.0), 2)
            currency_pair = random.choice(HIGH_RISK_CURRENCY_PAIRS)
    else:
        # Transacción de negocio estándar
        amount = round(random.uniform(500.0, 50000.0), 2)
        currency_pair = random.choice(NORMAL_CURRENCY_PAIRS)

    transaction = {
        "transaction_id": transaction_id,
        "sender_id": sender_id,
        "receiver_id": receiver_id,
        "amount": amount,
        "currency_pair": currency_pair,
        "merchant_category": merchant_category,
        "timestamp": timestamp,
        "is_synthetic_fraud": is_fraud  # Label para la validación del modelo
    }
    
    return transaction

def main():
    producer = create_kafka_producer()
    
    print(f"📡 Comenzando la emisión de streaming al topic: '{TOPIC_NAME}'...\n")
    
    try:
        while True:
            tx = generate_transaction()
            
            # Publicación asíncrona pero forzamos buffer flush para el streaming en tiempo real
            producer.send(TOPIC_NAME, value=tx)
            producer.flush()
            
            # Logger visual de operaciones B2B
            if tx['is_synthetic_fraud']:
                # Rojo en bash: \033[91m ... \033[0m
                print(f"\033[91m🚨 [FRAUDE DETECTADO]\033[0m  TX: {tx['transaction_id'][:8]} | Envío: {tx['sender_id']} | Monto: {tx['amount']:>9,.2f} | Par: {tx['currency_pair']}")
            else:
                # Verde en bash: \033[92m ... \033[0m
                print(f"\033[92m✅ [NORMAL]\033[0m           TX: {tx['transaction_id'][:8]} | Envío: {tx['sender_id']} | Monto: {tx['amount']:>9,.2f} | Par: {tx['currency_pair']}")
                
            # Ratio de ingesta entre 0.5s y 1.5s
            time.sleep(random.uniform(0.5, 1.5))
            
    except KeyboardInterrupt:
        print("\n🛑 Deteniendo el motor B2B de transacciones...")
    finally:
        if producer is not None:
            producer.close()
            print("🔌 Conexión con Kafka cerrada.")

if __name__ == "__main__":
    main()
