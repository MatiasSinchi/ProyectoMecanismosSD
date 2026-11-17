import json

from confluent_kafka import KafkaException, Producer


# Asistido por IA: conectar solo por IPv4, Kafka no escucha en ::1 y el intento por IPv6 llena la consola de errores
productor = Producer({"bootstrap.servers": "localhost:9092","broker.address.family": "v4","message.timeout.ms": 2000})

# asistido por ia: la configuración inicial del productor Kafka.
def publicar_evento(topic: str, evento: dict):
    resultado = {"error": None}

    def confirmar_entrega(error, mensaje):
        if error is not None:
            resultado["error"] = error

    productor.produce(
        topic=topic,
        key=str(evento["pedidoId"]),
        value=json.dumps(evento).encode("utf-8"),
        callback=confirmar_entrega
    )

    mensajes_pendientes = productor.flush(2)

    if mensajes_pendientes > 0:
        raise KafkaException("Kafka no confirmó el mensaje a tiempo")

    if resultado["error"] is not None:
        raise KafkaException(resultado["error"])