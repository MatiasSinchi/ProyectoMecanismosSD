# ADR-002: Apache Kafka para la comunicación entre servicios

- **Estado:** Aceptado
- **Fecha:** 2026-11-15
- **Decisores:** equipo CampusBite

## Contexto

Cuando se crea un pedido, tres servicios tienen que enterarse: **Cocina** (prepararlo), **Caja** (facturarlo) y **Ventas** (contarlo). Cuando la Cocina termina, el **Servicio de Pedidos** tiene que actualizar el estado.

Requisitos:

- **Desacoplamiento:** la API no debería conocer a cada servicio ni esperar a que todos respondan. Agregar un servicio nuevo, como pasó con Ventas, no debería obligar a cambiar la API.
- **Difusión:** cada servicio debe recibir **todos** los pedidos, no repartírselos.
- **Tolerancia a caídas:** si un servicio está apagado, sus pedidos no se pierden y los procesa al volver.
- **Ritmos distintos:** la Cocina tarda (3 s por pedido) y la Caja es instantánea; uno no debe frenar al otro.
- **Orden:** los eventos de un mismo pedido deben procesarse en orden.

Una llamada REST directa de la API a cada servicio no cumple estos requisitos. La API tendría que esperar a los tres y, si uno está caído, el pedido fallaría o se perdería para ese servicio.

## Decisión

Los servicios se comunican de forma **asíncrona por eventos** usando **Apache Kafka**.

**Topics**

| Topic | Evento | Productor | Consumidores |
|---|---|---|---|
| `campusbite.pedidos` | `PedidoCreado` | Servicio de Pedidos | Cocina, Caja, Ventas |
| `campusbite.pedidos-listos` | `PedidoListo` | Cocina | Servicio de Pedidos |

**Configuración y uso**

- **Un grupo de consumidores por servicio:**
  - `servicio-cocina`, `servicio-caja`, `servicio-ventas` y `servicio-pedidos`;
  - Kafka entrega cada evento a todos los grupos y guarda un offset distinto para cada uno.
- **Clave del mensaje = `pedidoId`.** Los eventos de un mismo pedido van a la misma partición y se leen en orden.
- **Commit manual (`enable.auto.commit: False`):** cada consumidor confirma el offset después de procesar el mensaje. Esto da entrega *at-least-once*, compensada con consumidores idempotentes por `eventId` ([ADR-003](ADR-003-seleccion-mecanismos.md)).
- **`auto.offset.reset: earliest`:** un grupo nuevo empieza desde el primer mensaje del topic y no pierde pedidos anteriores a su primer arranque.
- **Confirmación del productor:** `publicar_evento` espera hasta 2 s la confirmación del broker. Si no llega, la API responde 503 y no guarda el pedido.
- **Eventos en JSON** con `eventId`, `pedidoId`, `tipo` y `version`.
- **Infraestructura:**
  - imagen `apache/kafka:4.4.0` en modo KRaft, sin ZooKeeper, con un solo nodo levantado con `docker-compose`;
  - cliente `confluent-kafka` para Python.

## Alternativas consideradas

| Alternativa | Ventajas | Por qué se descartó |
|---|---|---|
| **Llamadas REST entre servicios** | Simple y sin infraestructura extra | Acopla los servicios: si la Cocina está caída, el pedido falla. La API tendría que conocer cada servicio y reintentar por su cuenta |
| **RabbitMQ** | Broker maduro, enrutamiento flexible con *exchanges*, fácil de operar | Borra el mensaje cuando se consume. Para difundir hay que crear una cola por servicio, y un servicio nuevo no puede releer pedidos anteriores. Kafka guarda los eventos y cada grupo lleva su offset, que es justo el modelo de "cada servicio a su ritmo" |
| **Redis Pub/Sub** | Muy ligero y rápido | No guarda los mensajes: si un suscriptor está caído, los pierde. No cumple la tolerancia a caídas |
| **Redis Streams** | Guarda mensajes y tiene grupos de consumidores | Es una opción válida, pero Kafka es el estándar del curso y de la industria para *event streaming*. Además, la persistencia de Redis depende de su configuración de disco |
| **Una tabla en la base de datos que los servicios consultan** (*polling*) | No requiere broker | Requiere una base de datos compartida, genera consultas constantes y hay que implementar a mano qué leyó cada servicio |

## Consecuencias

**Positivas**

- **Desacoplamiento:** la API no conoce a Cocina, Caja ni Ventas. Agregar Ventas no requirió tocar `main.py`.
- **Tolerancia a fallos comprobada:** con la Cocina apagada se siguieron aceptando pedidos, Caja y Ventas los procesaron, y al volver la Cocina recuperó el pendiente ([prueba-recuperacion-kafka.md](../pruebas/prueba-recuperacion-kafka.md)).
- **Cada servicio va a su ritmo:** la Cocina lenta no retrasa la factura.
- **Se puede reprocesar:** como Kafka guarda los eventos, un servicio nuevo o reiniciado puede leer el historial.
- **Se puede observar:** `kafka-consumer-groups.sh` muestra el *lag* de cada servicio.

**Negativas o riesgos**

- **Consistencia eventual:** después del `POST` el pedido está en `RECIBIDO` y pasa a `LISTO` segundos después; el cliente debe consultarlo.
- **Duplicados por *at-least-once*:** hay que hacer idempotentes a los consumidores (`eventId`).
- **Más infraestructura:** hay que levantar y mantener el broker, y si Kafka está caído no se pueden crear pedidos (503).
- **Un solo nodo y una partición:** no hay replicación ni paralelismo dentro de un grupo. En producción harían falta 3 brokers con `replication.factor=3` y varias particiones.
- **Sin esquema formal de eventos:** el JSON no se valida con Avro o JSON Schema. Un cambio en el formato podría romper a los consumidores sin aviso.
- **Topics creados automáticamente:** se crean con el primer mensaje, con la configuración por defecto. Por eso los consumidores toleran `UNKNOWN_TOPIC_OR_PART` al arrancar.

## Referencias

- [Arquitectura de CampusBite](../arquitectura.md)
- [ADR-001: API REST](ADR-001-rest.md)
- [Prueba de recuperación con Kafka](../pruebas/prueba-recuperacion-kafka.md)
- Código: `kafka_config.py`, `main.py`, `cocina.py`, `caja.py`, `ventas.py`, `docker-compose.yml`
