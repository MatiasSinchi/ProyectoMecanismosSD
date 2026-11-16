# Prueba de recuperación con Kafka

**Fecha:** 2026-11-16
**Objetivo:** comprobar que, si el servicio de Cocina se cae, los pedidos creados mientras está apagado no se pierden. Caja y Ventas los procesan igual y Cocina los recupera al volver a iniciarse.

## Entorno

- Kafka `apache/kafka:4.4.0` levantado con `docker-compose up -d` (Colima), sin topics previos.
- Python 3.15 con `confluent-kafka` 2.16.1.
- Servicios: `main.py` (API), `cocina.py`, `caja.py` y `ventas.py`, cada uno en su terminal.

Cada servicio consume con su propio `group.id` (`servicio-cocina`, `servicio-caja`, `servicio-ventas`), así que Kafka guarda un offset distinto para cada uno. Todos tienen `enable.auto.commit: False` y confirman el mensaje recién después de procesarlo.

## Pasos y resultados

| # | Paso | Resultado esperado | Resultado obtenido |
|---|------|--------------------|--------------------|
| 1 | Iniciar API, Cocina, Caja y Ventas, y crear el pedido A (2 x cafe) | Los tres consumidores lo procesan y el estado llega a `LISTO` | ✅ Estado `LISTO`, factura y venta registradas |
| 2 | Detener Cocina con Control + C | Cocina se cierra mostrando "Cocina detenida" | ✅ |
| 3 | Crear el pedido B (3 x empanada) con Cocina apagada | La API responde 201 | ✅ 201, estado `RECIBIDO` |
| 4 | Verificar Caja y Ventas | Facturan y registran B aunque Cocina esté caída | ✅ "Factura generada… 3 x empanada" y "Venta registrada… 3 x empanada" |
| 4b | Consultar el estado de B | Sigue en `RECIBIDO` porque nadie lo ha preparado | ✅ `RECIBIDO` |
| 5 | Volver a iniciar Cocina | Arranca desde el último offset confirmado | ✅ |
| 6 | Verificar que Cocina recupere el pedido pendiente | Prepara B y el estado pasa a `LISTO` | ✅ "Preparando pedido 0705735b…" y estado `LISTO` |

### Prueba extra: evento repetido

Se publicó dos veces en `campusbite.pedidos` el mismo evento, con `eventId = evento-duplicado-1`, para simular una entrega duplicada de Kafka.

- **Ventas:** lo contó una sola vez y la segunda vez mostró `Evento repetido evento-duplicado-1 ignorado`. ✅
- **Cocina y Caja:** lo procesaron dos veces, porque no controlan el `eventId`. Queda pendiente aplicarles el mismo control.

### Prueba extra: cierre con Control + C en Ventas

Al detener Ventas se mostró el resumen final y se cerró el consumidor:

```
Ventas detenida
Pedidos procesados: 3
Unidades vendidas: 6
  - cafe: 3
  - empanada: 3
```

### Offsets al finalizar

Todos los grupos terminaron sin mensajes pendientes (`LAG = 0`):

```
GROUP             TOPIC                      CURRENT-OFFSET  LOG-END-OFFSET  LAG
servicio-caja     campusbite.pedidos         4               4               0
servicio-cocina   campusbite.pedidos         4               4               0
servicio-ventas   campusbite.pedidos         4               4               0
servicio-pedidos  campusbite.pedidos-listos  4               4               0
```

Comando usado:

```bash
docker exec campusbite-kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --describe --all-groups
```

## Capturas

Salida de cada servicio durante la prueba:

- [api.txt](capturas/api.txt): peticiones a la API y cambios de estado
- [cocina-1.txt](capturas/cocina-1.txt): Cocina antes de detenerla
- [cocina-2.txt](capturas/cocina-2.txt): Cocina después de reiniciarla, recuperando el pedido B
- [caja.txt](capturas/caja.txt): facturas generadas
- [ventas.txt](capturas/ventas.txt): ventas, control de duplicados y resumen final

## Conclusión

Kafka conserva los mensajes y el offset confirmado de cada grupo de consumidores. Por eso, que un servicio se caiga no afecta a los demás, y al volver procesa lo que quedó pendiente. Como el commit se hace después de procesar, la entrega es *al menos una vez* (at-least-once). Eso puede producir duplicados, y Ventas los descarta con el `eventId`.

**Limitación:** los `eventId` vistos y los totales de Ventas se guardan en memoria. Si se reinicia Ventas, los contadores empiezan de cero y no reprocesa los eventos ya confirmados.
