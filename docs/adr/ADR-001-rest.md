# ADR-001: API REST para la comunicación cliente–servicio

- **Estado:** Aceptado
- **Fecha:** 2026-11-15
- **Decisores:** equipo CampusBite

## Contexto

Los clientes de CampusBite (estudiantes, desde una app o una web) necesitan dos operaciones:

1. **Crear un pedido** y recibir de inmediato su identificador.
2. **Consultar el estado** de un pedido (`RECIBIDO` o `LISTO`).

Las dos son interacciones de petición-respuesta: el cliente espera una respuesta en el momento. Además:

- El cliente puede ser cualquier navegador o app, así que el protocolo debe ser estándar y fácil de consumir.
- Los clientes y la red móvil del campus pueden fallar, así que el cliente puede **reintentar** una petición sin saber si la primera llegó.
- El equipo trabaja en Python y necesita probar y documentar la API rápido para el curso.

## Decisión

El Servicio de Pedidos (`main.py`) expone una **API REST sobre HTTP con JSON**, implementada con **FastAPI**:

| Método | Ruta | Uso | Códigos |
|---|---|---|---|
| `GET` | `/salud` | Comprobar que el servicio responde | 200 |
| `POST` | `/pedidos` | Crear un pedido | 201, 200, 422, 503 |
| `GET` | `/pedidos/{pedido_id}/estado` | Consultar el estado | 200, 404 |

Criterios de diseño:

- **Recursos y verbos HTTP:** el pedido es el recurso. `POST` lo crea y `GET` lo lee, sin efectos secundarios.
- **Códigos de estado con significado:**
  - `201 Created` al crear y `200 OK` si es un reintento con la misma `Idempotency-Key`;
  - `404 Not Found` si el pedido no existe;
  - `422 Unprocessable Content` si los datos son inválidos;
  - `503 Service Unavailable` si Kafka no confirma el evento. Así el cliente sabe que el fallo es temporal y puede reintentar.
- **Validación en el borde** con Pydantic: `producto` de 1 a 100 caracteres sin espacios sobrantes y `cantidad` de 1 a 100.
- **Idempotencia del `POST`** mediante el header `Idempotency-Key`. `POST` no es idempotente por definición en HTTP, así que el cliente envía una clave única por intento lógico y el servidor devuelve el pedido original si la clave se repite. Ver [ADR-003](ADR-003-seleccion-mecanismos.md).
- **Identificadores UUID** en lugar de enteros secuenciales. No se pueden adivinar y no dependen de un contador central.
- **Documentación automática** con OpenAPI y Swagger en `/docs`.

REST se usa **solo en el borde**, entre el cliente y el sistema. La comunicación entre servicios internos va por Kafka ([ADR-002](ADR-002-kafka.md)).

## Alternativas consideradas

| Alternativa | Ventajas | Por qué se descartó |
|---|---|---|
| **gRPC** | Contratos tipados con Protobuf, binario y rápido, *streaming* | Los navegadores no lo usan directamente (requiere gRPC-Web y un proxy). Es más difícil de probar con `curl` o Swagger y su rendimiento extra no hace falta para una cafetería |
| **GraphQL** | El cliente pide solo los campos que necesita y hay un único endpoint | El modelo tiene un solo recurso con dos operaciones, así que no aprovecha su flexibilidad. Además complica el uso de códigos HTTP y la caché |
| **WebSocket o Server-Sent Events** | El servidor puede avisar al cliente cuando el pedido está `LISTO`, sin que tenga que consultar | Mantiene conexiones abiertas y complica la creación del pedido. Queda como mejora futura para notificar el estado |
| **Que el cliente publique directo en Kafka** | Un salto menos | Expone el broker a internet, no da respuesta inmediata al cliente y obliga a cada cliente a usar una librería de Kafka |
| **Flask o Django REST Framework** en vez de FastAPI | Más conocidos | FastAPI trae validación con Pydantic, OpenAPI y Swagger automáticos sin configuración extra |

## Consecuencias

**Positivas**

- Cualquier cliente HTTP puede usar la API: navegador, app, `curl` o Postman.
- Swagger en `/docs` sirve de documentación y de herramienta de prueba para el equipo y la demo.
- Los códigos HTTP comunican el resultado sin cuerpo extra. El 503 en particular indica que conviene reintentar.
- Con `Idempotency-Key`, reintentar un `POST` es seguro.

**Negativas o riesgos**

- **El cliente debe consultar el estado periódicamente** (*polling*) para saber cuándo el pedido está `LISTO`, porque REST no tiene notificaciones.
- **El acoplamiento es temporal:** si el Servicio de Pedidos está caído, el cliente no puede pedir. Para evitarlo habría que tener varias instancias detrás de un balanceador.
- **Las claves de idempotencia están en memoria:** se pierden al reiniciar y no se comparten entre instancias. Escalar horizontalmente requeriría guardarlas en un almacenamiento común (Redis o una base de datos).
- **La API responde con lo que tiene en memoria:** el estado se actualiza de forma asíncrona desde Kafka, así que justo después de crear el pedido siempre se ve `RECIBIDO` (consistencia eventual).

## Referencias

- [Arquitectura de CampusBite](../arquitectura.md)
- [Prueba de Idempotency-Key](../pruebas/prueba-idempotency-key.md)
- Código: `main.py`
