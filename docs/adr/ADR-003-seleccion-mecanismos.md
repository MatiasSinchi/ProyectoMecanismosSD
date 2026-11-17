# ADR-003: Selección de mecanismos de sistemas distribuidos

- **Estado:** Aceptado
- **Fecha:** 2026-11-16
- **Decisores:** equipo CampusBite

## Contexto

Con REST en el borde ([ADR-001](ADR-001-rest.md)) y Kafka entre servicios ([ADR-002](ADR-002-kafka.md)), CampusBite se enfrenta a los problemas típicos de un sistema distribuido:

1. **La red y los procesos fallan.** Un cliente puede reintentar un `POST` sin saber si el primero llegó. Un consumidor puede caerse a mitad de procesar un mensaje.
2. **No hay un reloj ni un contador central.** Varios nodos generan identificadores al mismo tiempo.
3. **Los servicios no comparten memoria.** Cada uno ve el estado del pedido en un momento distinto.
4. **Los mensajes pueden llegar más de una vez o desordenados.**

Este ADR registra qué mecanismo se eligió para cada problema y por qué.

## Decisión

### 1. Comunicación híbrida: síncrona en el borde, asíncrona dentro

| | Elegido | Alternativas |
|---|---|---|
| Cliente → sistema | REST síncrono | — |
| Entre servicios | Eventos asíncronos (publicación-suscripción con Kafka) | Llamadas REST encadenadas |

**Por qué:** el cliente necesita respuesta inmediata, pero los servicios internos no deben depender de que los demás estén vivos.

### 2. Semántica de entrega: *at-least-once*

| Opción | Cómo | Riesgo |
|---|---|---|
| *At-most-once* | Commit antes de procesar (o `enable.auto.commit`) | Si el servicio cae a mitad, **se pierde** el pedido |
| **At-least-once** ✅ | `enable.auto.commit: False` y commit **después** de procesar | El mismo mensaje puede **procesarse dos veces** |
| *Exactly-once* | Productor idempotente con transacciones de Kafka y `read_committed` | Complejo. Solo cubre lo que pasa dentro de Kafka; no evita que se imprima dos veces una factura |

**Por qué:** en una cafetería es peor perder un pedido que recibirlo dos veces, y el duplicado se puede neutralizar con el mecanismo 4.

### 3. Idempotencia en la API: header `Idempotency-Key`

El cliente genera una clave única por intento lógico de pedido y la envía en cada reintento. El servidor guarda `clave → pedidoId` y, si la clave se repite:

- con el mismo cuerpo, devuelve el pedido original con **200**, sin crear otro ni publicar otro evento;
- con otro cuerpo, responde **422**.

Cada clave tiene su propio candado (`threading.Lock`), así que dos peticiones simultáneas con la misma clave se atienden de una en una y solo la primera crea el pedido. Las peticiones con claves distintas no se bloquean entre sí.

| Alternativa | Por qué se descartó |
|---|---|
| Detectar duplicados por contenido (mismo producto y cantidad) | Dos pedidos legítimos iguales se confundirían |
| No hacer nada | Un reintento por mala red crearía un segundo pedido y una segunda factura |

Es el mismo patrón que usan APIs de pago como Stripe. Ver [prueba-idempotency-key.md](../pruebas/prueba-idempotency-key.md).

### 4. Consumidores idempotentes: `eventId`

Cada evento lleva un `eventId` (UUID) único. Cocina, Caja y Ventas guardan los `eventId` ya procesados. Si llega uno repetido, lo ignoran pero **igual lo confirman**, para que Kafka no lo vuelva a entregar. El `eventId` se marca como procesado solo cuando terminó bien.

El Servicio de Pedidos no necesita este control: poner el estado en `LISTO` dos veces da el mismo resultado (es una operación idempotente por naturaleza).

**Por qué:** es lo que hace seguro el *at-least-once* del mecanismo 2. Junto con él da un efecto práctico de "exactamente una vez", sin las transacciones de Kafka.

### 5. Identificadores globales: UUID v4

| Opción | Problema |
|---|---|
| Entero secuencial (`len(pedidos) + 1`, la versión inicial) | Requiere un contador central; con varias instancias de la API se repetirían IDs. Además es predecible: cualquiera puede consultar el pedido de otro |
| ID autoincremental de base de datos | Acopla la creación a una base de datos única |
| Snowflake (tiempo + nodo + secuencia) | Requiere asignar un número a cada nodo; no hace falta a esta escala |
| **UUID v4** ✅ | Cada nodo lo genera solo, sin coordinarse, con probabilidad de choque despreciable |

Se usa para `pedidoId` y para `eventId`.

### 6. Orden: clave de partición = `pedidoId`

Kafka solo garantiza el orden dentro de una partición. Si el `pedidoId` es la clave del mensaje, todos los eventos de un mismo pedido van a la misma partición y se leen en orden, aunque en el futuro el topic tenga varias particiones.

### 7. Difusión con un grupo de consumidores por servicio

Cada servicio tiene su propio `group.id`, así que recibe **todos** los eventos y lleva su propio offset. Si varios servicios compartieran un grupo, Kafka les **repartiría** los pedidos, y Caja facturaría solo algunos.

Varias instancias de **un mismo servicio** sí deberían compartir grupo, para repartirse la carga.

### 8. Consistencia eventual

El estado del pedido no se actualiza al mismo tiempo en todos los servicios. Tras el `POST` el pedido está en `RECIBIDO`, y pasa a `LISTO` cuando llega `PedidoListo`.

| Alternativa | Por qué se descartó |
|---|---|
| Consistencia fuerte con *two-phase commit* entre servicios | Bloquea a todos si uno está caído y reduce la disponibilidad. Para un pedido de cafetería basta con que el estado se ponga al día en segundos |

### 9. Confirmación del productor y degradación controlada

`publicar_evento` espera la confirmación del broker (hasta 2 s). Si no llega, la API responde **503** y **no guarda** el pedido, así la API nunca tiene un pedido que los demás servicios desconocen.

| Alternativa | Por qué se descartó |
|---|---|
| Publicar sin esperar confirmación (*fire-and-forget*) | La API diría "201 creado" aunque el evento se hubiera perdido |
| Patrón *Transactional Outbox* (guardar el evento en una base de datos y publicarlo después) | Es la solución robusta, pero requiere una base de datos. Queda como mejora futura |

### 10. Arranque en cualquier orden

Los consumidores ignoran `UNKNOWN_TOPIC_OR_PART` mientras el topic todavía no existe, en lugar de caerse. Así los servicios se pueden iniciar en cualquier orden.

## Resumen

| Problema | Mecanismo elegido | Dónde |
|---|---|---|
| Acoplamiento entre servicios | Publicación-suscripción asíncrona | Kafka |
| Pérdida de mensajes | *At-least-once* (commit manual después de procesar) | Los 4 consumidores |
| Reintentos del cliente | `Idempotency-Key` | `main.py` |
| Mensajes duplicados | Consumidores idempotentes (`eventId`) | `cocina.py`, `caja.py`, `ventas.py` |
| IDs sin coordinación | UUID v4 | `main.py`, `cocina.py` |
| Orden de eventos | Clave de partición = `pedidoId` | `kafka_config.py` |
| Cada servicio recibe todo | Un `group.id` por servicio | Los 4 consumidores |
| Disponibilidad frente a consistencia | Consistencia eventual | Estado del pedido |
| Broker caído | Confirmación del productor + 503 | `kafka_config.py`, `main.py` |
| Orden de arranque | Tolerancia a topic inexistente | Los 4 consumidores |

## Consecuencias

**Positivas**

- Ningún pedido se pierde por la caída de un consumidor, y los duplicados no generan doble cocina, doble factura ni doble venta.
- Los reintentos del cliente son seguros.
- Se puede escalar: los UUID y la clave por `pedidoId` siguen sirviendo con más instancias y particiones.
- Cada mecanismo tiene una prueba documentada en `docs/pruebas/`.

**Negativas o riesgos**

- **El estado de los mecanismos de idempotencia vive en memoria:** las claves de `Idempotency-Key` y los `eventId` se pierden al reiniciar.
  - **Consumidores:** queda sin cubrir el caso de una caída entre procesar y confirmar.
  - **API:** un reintento después de reiniciar crearía otro pedido.

  La solución sería guardarlos en Redis o en una base de datos, con expiración.
- **El candado por clave solo funciona dentro de un proceso:** con varias instancias de la API haría falta una reserva atómica compartida, por ejemplo `SET NX` en Redis o una inserción única en base de datos.
- **Hueco entre publicar y responder:** si Kafka confirma después de los 2 s de espera, la API responde 503 pero el evento sí existe. El *Transactional Outbox* lo resolvería.
- **El campo `version` no se usa** para descartar eventos viejos o desordenados.

## Referencias

- [Arquitectura de CampusBite](../arquitectura.md)
- [ADR-001: API REST](ADR-001-rest.md)
- [ADR-002: Apache Kafka](ADR-002-kafka.md)
- [Prueba de recuperación con Kafka](../pruebas/prueba-recuperacion-kafka.md)
- [Prueba de Idempotency-Key](../pruebas/prueba-idempotency-key.md)
