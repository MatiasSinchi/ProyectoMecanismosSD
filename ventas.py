import json

from collections import defaultdict
from confluent_kafka import Consumer, KafkaError, KafkaException


# Asistido por IA: estructura del servicio de ventas y control de eventos repetidos.
consumidor = Consumer({"bootstrap.servers": "localhost:9092","group.id": "servicio-ventas","auto.offset.reset": "earliest","enable.auto.commit": False})

consumidor.subscribe(["campusbite.pedidos"])

pedidos_procesados = 0
unidades_totales = 0
unidades_por_producto = defaultdict(int) #producto -> unidades vendidas
eventos_procesados = set() #eventId ya contados, para no sumar dos veces el mismo evento


def mostrar_resumen():
    print(f"Pedidos procesados: {pedidos_procesados}")
    print(f"Unidades vendidas: {unidades_totales}")
    for producto, unidades in unidades_por_producto.items():
        print(f"  - {producto}: {unidades}")


print("Ventas esperando pedidos...")


try:
    while True:
        mensaje = consumidor.poll(1.0)

        if mensaje is None:
            continue

        if mensaje.error():
            #el topic aun no existe hasta que la API publique el primer pedido
            if mensaje.error().code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
                continue
            raise KafkaException(mensaje.error())

        evento = json.loads(mensaje.value().decode("utf-8"))
        event_id = evento["eventId"]

        if event_id in eventos_procesados:
            print(f"Evento repetido {event_id} ignorado")
        else:
            eventos_procesados.add(event_id)
            pedidos_procesados += 1
            unidades_totales += evento["cantidad"]
            unidades_por_producto[evento["producto"]] += evento["cantidad"]

            print(f"Venta registrada para pedido {evento['pedidoId']}: {evento['cantidad']} x {evento['producto']}")
            mostrar_resumen()

        #se confirma despues de procesar, asi un mensaje no se pierde si el servicio cae antes
        consumidor.commit(
            message=mensaje,
            asynchronous=False
        )

except KeyboardInterrupt:
    print("\nVentas detenida")
    mostrar_resumen()

finally:
    consumidor.close()
