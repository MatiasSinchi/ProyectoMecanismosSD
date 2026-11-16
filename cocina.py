import json
import time

from uuid import uuid4
from confluent_kafka import Consumer, KafkaError, KafkaException
from kafka_config import publicar_evento


consumidor = Consumer({"bootstrap.servers": "localhost:9092","group.id": "servicio-cocina","auto.offset.reset": "earliest","enable.auto.commit": False})

consumidor.subscribe(["campusbite.pedidos"])

print("Cocina esperando pedidos...")


try:
    while True:
        mensaje = consumidor.poll(1.0)

        if mensaje is None:
            continue

        if mensaje.error():
            # Asistido por IA: el topic aun no existe hasta que la API publique el primer pedido
            if mensaje.error().code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
                continue
            raise KafkaException(mensaje.error())

        evento = json.loads(mensaje.value().decode("utf-8"))
        pedido_id = evento["pedidoId"]

        print(f"Preparando pedido {pedido_id}...")
        time.sleep(3)

        evento_listo = {
            "eventId": str(uuid4()),
            "pedidoId": pedido_id,
            "tipo": "PedidoListo",
            "version": evento["version"] + 1,
            "estado": "LISTO"
        }

        publicar_evento(
            "campusbite.pedidos-listos",
            evento_listo
        )

        consumidor.commit(
            message=mensaje,
            asynchronous=False
        )

        print(f"Pedido {pedido_id} terminado y publicado")

except KeyboardInterrupt:
    print("\nCocina detenida")

finally:
    consumidor.close()