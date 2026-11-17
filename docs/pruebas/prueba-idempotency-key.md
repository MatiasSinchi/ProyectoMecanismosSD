# Prueba de Idempotency-Key

**Fecha:** 2026-11-16
**Objetivo:** comprobar que reintentar `POST /pedidos` con el mismo header `Idempotency-Key` no crea pedidos duplicados ni publica eventos duplicados en Kafka, y encontrar los límites del mecanismo.

## Cómo funciona

El cliente genera una clave única por pedido y la envía en el header `Idempotency-Key`. En `main.py`:

1. Si la clave ya está en `claves_idempotencia`, se devuelve el pedido guardado y **no se publica ningún evento**.
2. Si no está, se crea el pedido, se publica `PedidoCreado` en Kafka y, **solo si Kafka confirma**, se guarda la relación `clave → pedidoId`.

## Entorno

- Kafka `apache/kafka:4.4.0` recién creado con `docker-compose up -d`, con el topic vacío.
- API (`uvicorn main:app`) y `caja.py` en ejecución.
- Peticiones con `curl`.

**Cómo se verificó:** para saber si se publicó un evento, se contaron los mensajes del topic `campusbite.pedidos` antes y después de cada caso:

```bash
docker exec campusbite-kafka /opt/kafka/bin/kafka-get-offsets.sh \
  --bootstrap-server localhost:9092 --topic campusbite.pedidos
```

## Casos y resultados

| # | Caso | Esperado | Obtenido | Eventos en Kafka | |
|---|------|----------|----------|------------------|---|
| 1 | Primera petición con la clave `pedido-cafe-001` | 201, pedido nuevo | 201, id `11590b89…` | 0 → 1 | ✅ |
| 2 | Dos reintentos con la misma clave y el mismo cuerpo | El mismo pedido, sin evento nuevo | Mismo id `11590b89…` en ambos | 1 → 1 | ✅ |
| 3 | Misma compra (2 x cafe) con otra clave, `pedido-cafe-002` | Pedido nuevo: son dos compras legítimas | 201, id `697852cb…` | 1 → 2 | ✅ |
| 4 | Clave `pedido-cafe-001` con otro cuerpo (5 x empanada) | Error 409 o 422: la clave ya se usó con otro contenido | 201 con el pedido **original** (2 x cafe), sin error | 2 → 2 | ⚠️ |
| 5 | Sin header `Idempotency-Key` | 422 | 422 `Field required` | — | ✅ |
| 6 | Header con valor vacío | 422 | 422 `String should have at least 1 character` | — | ✅ |
| 7 | 10 peticiones **simultáneas** con la misma clave | Un único pedido | **10 pedidos distintos** y 10 facturas en Caja | 2 → 12 | ❌ |
| 8 | Kafka caído y luego reintento con la misma clave | 503; al volver Kafka, un único pedido | 503; el reintento crea el pedido y el siguiente devuelve el mismo id | 12 → 13 | ✅ |
| 9 | Reintento con la misma clave después de reiniciar la API | El mismo pedido | **Pedido nuevo** (`2dbd592c…` → `19accf99…`) | +1 | ❌ |

Salidas completas: [idempotency-key.txt](capturas/idempotency-key.txt) y [idempotency-key-caja.txt](capturas/idempotency-key-caja.txt). En la captura, el caso 6 aparece después del 7 porque se repitió al final: en el primer intento `curl` no envió el header vacío.

## Análisis

### Lo que funciona (casos 1, 2, 3, 5, 6 y 8)

- **Los reintentos secuenciales son seguros:** la misma clave devuelve el mismo pedido y no publica un segundo evento, así que Cocina, Caja y Ventas no se enteran de ningún duplicado.
- **Dos compras iguales con claves distintas se tratan como pedidos distintos.** Es lo correcto: la clave identifica el *intento*, no el contenido.
- **Si Kafka falla, la clave no se guarda (caso 8).** Así el cliente puede reintentar con la misma clave cuando Kafka vuelve. Si la clave se guardara antes de publicar, el reintento devolvería un pedido que nunca llegó a Kafka.

### Problemas encontrados

**Caso 7: condición de carrera (❌, el más grave)**

`crear_pedido` es una función síncrona, y FastAPI la ejecuta en un *pool* de hilos, así que varias peticiones corren en paralelo. La comprobación y el guardado de la clave no son atómicos:

```
Hilo A: ¿clave en claves_idempotencia? → no
Hilo B: ¿clave en claves_idempotencia? → no    ← A todavía no la guardó
Hilo A: publica en Kafka (tarda unos ms) y guarda la clave
Hilo B: publica en Kafka y guarda la clave       → segundo pedido
```

Como la publicación en Kafka tarda, la ventana es amplia y las 10 peticiones pasaron la comprobación. Es el escenario típico de un cliente que reintenta por *timeout* mientras la primera petición sigue en curso.

**Solución propuesta:** reservar la clave de forma atómica antes de publicar, con un `threading.Lock` o, en producción, con una inserción única en Redis o en la base de datos.

**Caso 9: las claves están en memoria (❌)**

`claves_idempotencia` es un diccionario en memoria, así que al reiniciar la API se pierde. Un cliente que reintenta justo después de un reinicio crea un pedido duplicado. Con varias instancias de la API pasaría lo mismo, porque cada una tendría su propio diccionario.

**Solución propuesta:** guardar las claves en un almacenamiento compartido y persistente (Redis o base de datos) con expiración, por ejemplo de 24 h.

**Caso 4: no se compara el cuerpo (⚠️)**

Reutilizar una clave con otro contenido devuelve el pedido original con 201, sin avisar. El cliente cree que pidió 5 empanadas y en realidad tiene 2 cafés.

**Solución propuesta:** guardar un hash del cuerpo junto a la clave y responder **422** si no coincide. Es lo que hacen APIs como Stripe.

**Detalle menor:** el reintento responde **201 Created**, aunque no se creó nada nuevo. Sería más preciso responder **200 OK**.

## Conclusión

`Idempotency-Key` cumple su objetivo en el caso principal: **reintentos secuenciales de un cliente**, como los que provoca una red inestable. Además se comporta bien cuando Kafka falla.

En cambio, la prueba demostró que **no protege frente a peticiones concurrentes ni frente a reinicios de la API**. Son los riesgos ya anotados en el [ADR-003](../adr/ADR-003-seleccion-mecanismos.md), ahora confirmados con evidencia. El caso 7 es el más importante de corregir, porque un cliente con *timeout* corto lo provoca fácilmente.
