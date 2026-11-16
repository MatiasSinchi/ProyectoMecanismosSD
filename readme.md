# CampusBite

Sistema distribuido de pedidos para la cafetería, con FastAPI y Kafka.

```
POST /pedidos → [campusbite.pedidos] → cocina.py → [campusbite.pedidos-listos] → main.py (estado LISTO)
                                     ↘ caja.py (factura)
```

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

2. En terminales separadas, con el entorno virtual activado:

```bash
python -m uvicorn main:app --reload   # API de pedidos
python cocina.py                      # servicio de cocina
python caja.py                        # servicio de caja
```

La documentación Swagger queda en http://127.0.0.1:8000/docs

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
