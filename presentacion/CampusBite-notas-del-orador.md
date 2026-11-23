# CampusBite: notas del orador

Guion para cada diapositiva de `CampusBite-presentacion.pptx`. Las mismas notas están en el panel de notas de PowerPoint.

## 1. Portada

Buenos días. Somos Matias Sinchi y Alex Paucar y vamos a presentar CampusBite, un sistema distribuido de pedidos para la cafetería del campus. Lo construimos con FastAPI para la API y Apache Kafka para la comunicación entre servicios. En la presentación vamos a ver la arquitectura, las tres decisiones de arquitectura (ADR), el código más importante y las pruebas que hicimos para comprobar que el sistema tolera fallos.

## 2. El problema: un pedido, tres servicios

El estudiante hace un pedido y quiere saber cuándo está listo. Pero detrás hay tres áreas que tienen que enterarse: la cocina lo prepara, la caja lo factura y ventas lleva las estadísticas. Estos son los cuatro requisitos que guiaron todo el diseño. El más importante para la materia es el tercero y el cuarto: si un servicio se cae no se pueden perder pedidos, y si algo se repite no se pueden duplicar pedidos ni facturas.

## 3. Arquitectura general

Este es el sistema completo. A la izquierda, el cliente habla con el Servicio de Pedidos, que es main.py con FastAPI, por REST: es síncrono porque el cliente necesita su respuesta en el momento. La API publica un evento PedidoCreado en el topic campusbite.pedidos. De ahí lo leen tres servicios independientes: Caja, Ventas y Cocina, cada uno con su propio grupo de consumidores. Cuando la Cocina termina, publica PedidoListo en un segundo topic, y un hilo dentro de la API lo consume y cambia el estado a LISTO. Lo importante: la API no conoce a ninguno de los tres servicios. Solo conoce a Kafka. Por eso agregar Ventas no nos obligó a tocar main.py.

## 4. Flujo de un pedido

Seguimos un pedido de principio a fin. Uno: el cliente hace POST /pedidos con el header Idempotency-Key. Dos: la API publica PedidoCreado y espera que Kafka confirme. Tres: recién entonces responde 201 con estado RECIBIDO. Cuatro: los tres grupos reciben su copia del evento en paralelo; la caja factura, ventas suma y la cocina tarda tres segundos. Cinco: la cocina publica PedidoListo. Seis: la API lo recibe y el estado pasa a LISTO. Abajo están los dos eventos. Fíjense en el eventId, que sirve para detectar duplicados, y en que la clave del mensaje es el pedidoId, para que los eventos de un mismo pedido lleguen en orden.

## 5. ADR-001: REST con FastAPI en el borde

La primera decisión fue cómo habla el cliente con el sistema. Necesitamos dos operaciones de petición-respuesta: crear el pedido y consultar su estado. El cliente puede ser cualquier navegador o app, y la red del campus puede fallar, así que el cliente puede reintentar. Elegimos REST con JSON implementado en FastAPI, con códigos HTTP con significado: 201 creado, 200 si es un reintento, 422 datos inválidos, 404 no existe, y 503 si Kafka no confirma, que le dice al cliente que puede reintentar. Descartamos gRPC porque el navegador no lo usa directo, GraphQL porque tenemos un solo recurso, WebSocket porque complica la creación, y que el cliente publique directo en Kafka porque expondría el broker. La consecuencia negativa principal es que el cliente tiene que consultar el estado periódicamente, porque REST no notifica.

## 6. ADR-002: Apache Kafka entre servicios

La segunda decisión fue cómo se comunican los servicios internos. Cuando se crea un pedido, tres servicios tienen que enterarse, y cada uno debe recibir todos los pedidos, no repartírselos. Además, si uno está apagado no puede perder pedidos, y la cocina es lenta mientras la caja es instantánea. Con llamadas REST directas, la API tendría que esperar a los tres y si uno estaba caído el pedido fallaba. Por eso usamos Kafka: dos topics, un group.id por servicio para que cada uno reciba todo con su propio offset, la clave del mensaje es el pedidoId para mantener el orden, y commit manual. RabbitMQ lo descartamos porque borra el mensaje al consumirlo y un servicio nuevo no puede releer el historial; Redis Pub/Sub porque no guarda los mensajes. Lo positivo se comprobó: agregar Ventas no requirió tocar main.py. Lo negativo es la consistencia eventual y que puede haber duplicados.

## 7. ADR-003: ¿perder o duplicar?

El tercer ADR registra los mecanismos de sistemas distribuidos que elegimos. El más importante es la semántica de entrega. Hay tres opciones. At-most-once: confirmar el mensaje antes de procesarlo; si el servicio se cae a mitad, el pedido se pierde. Exactly-once: con transacciones de Kafka, pero es complejo y solo cubre lo que pasa dentro de Kafka; no impide imprimir dos veces una factura. Elegimos at-least-once: commit manual después de procesar. En una cafetería es peor perder un pedido que recibirlo dos veces. Y el duplicado lo neutralizamos con el eventId: cada consumidor recuerda qué eventos ya procesó. Juntos, at-least-once más consumidores idempotentes dan un efecto práctico de exactamente una vez.

## 8. ADR-003: mecanismos aplicados

Esta tabla resume los diez mecanismos del ADR-003: qué problema resuelve cada uno y en qué archivo está. Algunos que vale la pena destacar: los UUID v4, que cualquier nodo genera sin coordinarse con un contador central, y que reemplazaron al contador secuencial de la versión inicial. La consistencia eventual: preferimos que el estado se ponga al día en segundos antes que usar two-phase commit, que bloquearía a todos si uno se cae. Y la confirmación del productor: la API solo responde 201 si Kafka confirmó el evento; si no, responde 503 y no guarda el pedido.

## 9. Código: POST /pedidos con Idempotency-Key

Este es el corazón de main.py. El endpoint exige el header Idempotency-Key y toma un candado propio de esa clave, así dos peticiones simultáneas con la misma clave se atienden de una en una. Dentro, si la clave ya existe: si el cuerpo es distinto responde 422, y si es igual devuelve el pedido original con 200, sin publicar otro evento. Si la clave es nueva, publica PedidoCreado en Kafka. Si Kafka falla, responde 503 y no guarda nada. Solo después de que Kafka confirma se guardan el pedido y la clave. Ese orden importa: si guardáramos la clave antes de publicar, un reintento devolvería un pedido que nunca llegó a la cocina.

## 10. Código: productor con confirmación de entrega

kafka_config.py tiene el productor que usan la API y la cocina. Tres detalles. Primero, la clave del mensaje es el pedidoId: Kafka manda los mensajes con la misma clave a la misma partición, así que los eventos de un pedido llegan en orden. Segundo, no es fire-and-forget: flush espera hasta dos segundos la confirmación del broker, y si no llega o hay error lanza KafkaException, que la API convierte en 503. Así nunca respondemos 201 por un evento perdido. Tercero, broker.address.family v4: fue un problema real que encontramos, el cliente intentaba conectarse por IPv6 a ::1, donde Kafka no escucha, y llenaba la consola de errores.

## 11. Código: consumidor idempotente (cocina.py)

Los tres consumidores siguen el mismo patrón; este es el de la cocina. En la configuración: un group.id propio, servicio-cocina, para recibir todos los pedidos; enable.auto.commit en False, para confirmar a mano; y earliest para no perder pedidos anteriores al primer arranque. En el bucle: si el topic todavía no existe, lo ignora en lugar de caerse, así los servicios arrancan en cualquier orden. Si el eventId ya se procesó, lo ignora pero igual hace commit, para que Kafka no lo vuelva a entregar. Si es nuevo, prepara el pedido, publica PedidoListo, marca el eventId y recién al final hace commit. Ese orden es lo que da at-least-once.

## 12. Despliegue y ejecución

Así se levanta el sistema. Kafka corre en un contenedor de Docker con la imagen oficial apache/kafka 4.4.0, en modo KRaft, es decir, sin ZooKeeper, con un solo nodo. Después cada servicio es un proceso independiente en su propia terminal: la API con uvicorn y los tres consumidores con python. Todos se conectan al broker en localhost 9092. Gracias a la tolerancia a topic inexistente, se pueden iniciar en cualquier orden. Swagger queda en /docs y lo usamos para las pruebas y la demo.

## 13. Prueba 1: la Cocina se cae

La primera prueba comprueba la tolerancia a fallos. Con todo funcionando creamos el pedido A y llegó a LISTO. Después detuvimos la Cocina con Control C y creamos el pedido B. La API respondió 201 igual, y Caja y Ventas facturaron y contaron el pedido B aunque la Cocina estaba caída; el estado se quedó en RECIBIDO porque nadie lo había preparado. Al reiniciar la Cocina, Kafka le entregó el pedido B desde su último offset confirmado, lo preparó y pasó a LISTO. Al final todos los grupos quedaron con LAG cero. Como prueba extra publicamos dos veces el mismo evento: Ventas lo ignoró, pero Cocina y Caja lo procesaron dos veces. Eso nos llevó a agregar el control de eventId también en esos dos servicios.

## 14. Prueba 2: diez peticiones simultáneas

La segunda prueba fue la más reveladora. Los reintentos uno a uno funcionaban bien: misma clave, mismo pedido, ningún evento nuevo en Kafka. Pero cuando mandamos diez peticiones al mismo tiempo con la misma clave, se crearon diez pedidos y diez facturas. Era una condición de carrera: FastAPI ejecuta las funciones síncronas en un pool de hilos, y las diez pasaron la comprobación antes de que la primera guardara la clave, porque publicar en Kafka tarda unos milisegundos. La corrección tuvo tres partes: un candado por clave, comparar el cuerpo para responder 422 si la clave se reusa con otro pedido, y responder 200 en los reintentos. Al repetir la prueba: un solo pedido, un 201 y nueve 200, un evento y una factura. Lo que sigue pendiente es que las claves viven en memoria y se pierden al reiniciar la API.

## 15. Limitaciones y mejoras futuras

Somos conscientes de las limitaciones, y todas están documentadas en la arquitectura y los ADR. La principal es que el estado vive en memoria: pedidos, claves y eventId se pierden al reiniciar; en producción irían en una base de datos o Redis. Tenemos un solo broker y una partición, sin replicación; en producción serían tres brokers con factor de replicación tres. El candado de idempotencia es local al proceso, así que con varias instancias de la API haría falta un SET NX en Redis. Hay un hueco entre publicar y responder que resolvería el patrón Transactional Outbox. Y para no hacer polling, el estado podría notificarse por WebSocket o Server-Sent Events.

## 16. Uso de IA en el proyecto

Usamos Claude Code durante el proyecto, y cada consulta quedó registrada en la bitácora; en el código, los fragmentos asistidos tienen el comentario 'Asistido por IA'. La regla fue no dar nada por bueno sin ejecutarlo. Y eso fue clave, porque las pruebas encontraron problemas que no se veían leyendo el código: los duplicados en Cocina y Caja, y la condición de carrera del Idempotency-Key. La IA también se equivocó: intentó abrir Docker Desktop cuando usamos Colima, uno de sus scripts de prueba se colgó, y el caso del header vacío no probaba lo que decía. Lo que no delegamos: las decisiones de qué implementar, los commits, que hicimos a mano después de revisar cada cambio, y la revisión del código del compañero.

## 17. Conclusiones

Para cerrar, cuatro aprendizajes. Uno: un sistema distribuido falla por partes, y el diseño tiene que asumirlo; Kafka permitió que la caída de la cocina no detuviera a nadie más. Dos: no perder mensajes y no duplicarlos tiran en sentidos opuestos; confirmar después de procesar evita perder, pero obliga a ignorar los repetidos. Tres: un mecanismo que funciona en la prueba simple puede fallar con concurrencia, como nos pasó con las diez peticiones. Cuatro: la IA acelera, pero solo sirve si se entiende y se verifica. El video con la demo está en el enlace. Muchas gracias, ¿preguntas?
