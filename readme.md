# CampusBite

Sistema distribuido de pedidos para la cafetería, con FastAPI y Kafka.

```
POST /pedidos → [campusbite.pedidos] → cocina.py → [campusbite.pedidos-listos] → main.py (estado LISTO)
                                     ↘ caja.py (factura)
                                     ↘ ventas.py (estadísticas de ventas)
```

La arquitectura completa (componentes, eventos, flujo y decisiones) está en [docs/arquitectura.md](docs/arquitectura.md).

## Instalación

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
```

En macOS con Python 3.14 o superior, `confluent-kafka` no trae wheel y necesita `librdkafka`:

```bash
brew install librdkafka
C_INCLUDE_PATH=/opt/homebrew/include LIBRARY_PATH=/opt/homebrew/lib python -m pip install -r requirements.txt
```

## Ejecución

1. Levantar Kafka (con Docker Desktop o Colima):

```bash
docker compose up -d   # o docker-compose up -d
```

2. Iniciar la API (con el entorno virtual activado):

```bash
python -m uvicorn main:app --reload
```

La documentación Swagger queda en http://127.0.0.1:8000/docs

3. Iniciar cada servicio en una terminal separada, con el entorno virtual activado:

```bash
source .venv/bin/activate
python cocina.py
```

```bash
source .venv/bin/activate
python caja.py
```

```bash
source .venv/bin/activate
python ventas.py
```

| Servicio | Grupo de Kafka | Lee de | Qué hace |
|----------|----------------|--------|----------|
| `cocina.py` | `servicio-cocina` | `campusbite.pedidos` | Prepara el pedido (3 s) y publica `PedidoListo` en `campusbite.pedidos-listos` |
| `caja.py` | `servicio-caja` | `campusbite.pedidos` | Genera la factura del pedido |
| `ventas.py` | `servicio-ventas` | `campusbite.pedidos` | Cuenta pedidos y unidades vendidas (en total y por producto) e ignora eventos repetidos por `eventId` |

Cada servicio tiene su propio grupo, así que todos reciben todos los pedidos. Se pueden iniciar en cualquier orden, incluso antes de crear el primer pedido. Se detienen con **Control + C**; Ventas muestra además un resumen final.

Si un servicio está apagado, Kafka le guarda los pedidos y los procesa al volver a iniciarse. Ver [docs/pruebas/prueba-recuperacion-kafka.md](docs/pruebas/prueba-recuperacion-kafka.md).

## Probar el flujo

```bash
curl -X POST http://127.0.0.1:8000/pedidos \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: pedido-1" \
  -d '{"producto": "empanada", "cantidad": 2}'

curl http://127.0.0.1:8000/pedidos/<id>/estado
```

El estado pasa de `RECIBIDO` a `LISTO` unos segundos después, cuando la cocina termina el pedido.

## Detener

```bash
docker compose down   # o docker-compose down
```
