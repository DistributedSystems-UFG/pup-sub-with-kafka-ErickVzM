from kafka import KafkaProducer
from const import *
import sys

broker = f"{BROKER_ADDR}:{BROKER_PORT}"
try:
    producer = KafkaProducer(
        bootstrap_servers=[broker],
        value_serializer=lambda v: v.encode('utf-8')
    )
except Exception as e:
    print(f"Erro ao conectar ao Kafka: {e}")
    sys.exit(1)

try:
    username = input("Qual o seu nome de usuário? ")
    topic_name = input(f"Olá {username}, para qual grupo quer enviar mensagens? ")
except KeyboardInterrupt:
    print("\nSaindo.")
    sys.exit()

print(f"\n--- Conectado ao grupo '{topic_name}' como '{username}' ---")
print("Digite 'sair' a qualquer momento para fechar.")

try:
    while True:
        message = input("> ")

        if message.lower() == 'sair':
            break
        chat_message = f"[{username}]: {message}"

        print(f"Enviando para '{topic_name}' -> {chat_message}")
        producer.send(topic_name, value=chat_message)
        producer.flush() 

except KeyboardInterrupt:
    print("\nDesconectando...")
finally:
    producer.close()
    print("Produtor fechado.")
