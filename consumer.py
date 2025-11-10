from kafka import KafkaConsumer
from const import *
import sys

broker = f"127.0.0.1:9092"

try:
    topics_input = input("Quais grupos você quer ouvir (separados por vírgula)? ")
    topic_list = [topic.strip() for topic in topics_input.split(',')]
    
    if not topic_list or all(t == '' for t in topic_list):
        raise ValueError("Nenhum tópico fornecido.")
        
except (KeyboardInterrupt, EOFError):
    print("\nSaindo.")
    sys.exit(1)
except Exception as e:
    print(f"Erro: {e}")
    print("Insira pelo menos um nome de tópico.")
    sys.exit(1)

try:
    consumer = KafkaConsumer(
        bootstrap_servers=[broker],
        value_deserializer=lambda v: v.decode('utf-8'),
        auto_offset_reset='latest',
        group_id=None 
    )
except Exception as e:
    print(f"Erro ao conectar ao Kafka: {e}")
    sys.exit(1)

consumer.subscribe(topics=topic_list)

print(f"\n--- Ouvindo os grupos: {', '.join(topic_list)} ---")
print("(Pressione Ctrl+C para sair)\n")

try:
    for msg in consumer:
        print(f"({msg.topic}) {msg.value}")
        
except KeyboardInterrupt:
    print("\nDesconectando...")
finally:
    consumer.close()
    print("Consumidor fechado.")
