import json

from confluent_kafka import Consumer, KafkaError, KafkaException


consumidor = Consumer({"bootstrap.servers": "localhost:9092","group.id": "servicio-caja","auto.offset.reset": "earliest","enable.auto.commit": False})

consumidor.subscribe(["campusbite.pedidos"])

print("Caja esperando pedidos...")


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

        evento = json.loads(
            mensaje.value().decode("utf-8")
        )

        pedido_id = evento["pedidoId"]
        producto = evento["producto"]
        cantidad = evento["cantidad"]

        print(
            f"Factura generada para pedido {pedido_id}: "
            f"{cantidad} x {producto}"
        )

        consumidor.commit(
            message=mensaje,
            asynchronous=False
        )

except KeyboardInterrupt:
    print("\nCaja detenida")

finally:
    consumidor.close()