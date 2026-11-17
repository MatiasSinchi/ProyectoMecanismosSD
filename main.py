from fastapi import FastAPI, HTTPException, Header, Response
from pydantic import BaseModel, Field, field_validator
#con kafka
from uuid import uuid4
from confluent_kafka import Consumer,KafkaException
from kafka_config import publicar_evento
#pedido listo evento
import json
from threading import Thread, Lock
from contextlib import asynccontextmanager

class PedidoEntrada(BaseModel):
    producto: str = Field(min_length=1, max_length=100)
    cantidad: int = Field(gt=0, le=100)

    @field_validator("producto")
    @classmethod
    def validar_producto(cls, producto:str):
        producto = producto.strip()
        
        if not producto:
            raise ValueError("El nombre del producto no puede estar vacio")
        
        return producto
    
@asynccontextmanager
async def lifespan(app: FastAPI):
    hilo = Thread(
        target=escuchar_pedidos_listos,
        daemon=True
    )
    hilo.start()

    yield
    
app = FastAPI(title="CampusBite", description="sistma distribuido de pedidos para la cafeteria",lifespan=lifespan)
#prueba de swwagger con fastapi
@app.get("/salud", tags=["Salud"])
def consultar_salud():
    return {"salud": "ok"}
            #clave y valor
            
pedidos = {}
claves_idempotencia = {} #este diccionario relacionara cada clave con el pedido que ya fue creado

# Asistido por IA: un candado por clave para que dos peticiones simultaneas con la misma clave no creen dos pedidos.
candado_general = Lock() #protege el diccionario de candados
candados_por_clave = {}

def obtener_candado(clave: str):
    with candado_general:
        if clave not in candados_por_clave:
            candados_por_clave[clave] = Lock()
        return candados_por_clave[clave]

#consumimos los eventos PedidoListo.
def escuchar_pedidos_listos():
    consumidor_estado = Consumer({"bootstrap.servers": "localhost:9092","group.id": "servicio-pedidos","auto.offset.reset": "earliest","enable.auto.commit": False})
    consumidor_estado.subscribe(["campusbite.pedidos-listos"])
    
    print("Servicio de Pedidos esperando eventos PedidoListo...")

    while True:
        mensaje = consumidor_estado.poll(1.0)

        if mensaje is None:
            continue

        if mensaje.error():
            print(f"Error al consumir: {mensaje.error()}")
            continue

        evento = json.loads(
            mensaje.value().decode("utf-8")
        )

        pedido_id = evento["pedidoId"]

        if pedido_id in pedidos:
            pedidos[pedido_id]["estado"] = evento["estado"]
            print(f"Pedido {pedido_id} actualizado a LISTO")

        consumidor_estado.commit(
            message=mensaje,
            asynchronous=False
        )

#queremos crear nuestro pedido 
# Asistido por IA: implementación inicial de Idempotency-Key.
@app.post("/pedidos", status_code=201, tags=["Pedidos"]) #status code 201 es para indicar que se creo un recurso
def crear_pedido(pedido: PedidoEntrada, response: Response, idempotency_key :str = Header(..., alias="Idempotency-Key",min_length=1)): #obliga a que el JSON tenga producto y cantidad.
    #solo una peticion por clave a la vez: las demas esperan y luego encuentran el pedido ya creado
    with obtener_candado(idempotency_key):
        return crear_pedido_unico(pedido, response, idempotency_key)


def crear_pedido_unico(pedido: PedidoEntrada, response: Response, idempotency_key: str):
    if idempotency_key in claves_idempotencia:
        pedido_id = claves_idempotencia[idempotency_key]
        pedido_guardado = pedidos[pedido_id]

        # Asistido por IA: la clave ya se uso con otro contenido, no se devuelve un pedido distinto al solicitado
        if pedido_guardado["producto"] != pedido.producto or pedido_guardado["cantidad"] != pedido.cantidad:
            raise HTTPException(
                status_code=422,
                detail="La Idempotency-Key ya se uso con otro pedido"
            )

        response.status_code = 200 #no se creo nada nuevo, se devuelve el pedido existente
        return pedido_guardado
    pedido_id = str(uuid4())
        
    nuevo_pedido = {
        "id": pedido_id,
        "producto": pedido.producto,
        "cantidad": pedido.cantidad,
        "estado": "RECIBIDO"
    }

    evento = {
        "eventId": str(uuid4()),
        "pedidoId": pedido_id,
        "tipo": "PedidoCreado",
        "version": 1,
        "producto": pedido.producto,
        "cantidad": pedido.cantidad
    }

    try:
        publicar_evento("campusbite.pedidos", evento)
    except KafkaException:
        raise HTTPException(
            status_code=503,
            detail="Kafka no está disponible"
        )

    pedidos[pedido_id] = nuevo_pedido
    claves_idempotencia[idempotency_key] = pedido_id

    return nuevo_pedido


#queremos consultar sobre nuestro pedido
@app.get("/pedidos/{pedido_id}/estado", tags=["Pedidos"]) #el {pedido_id} es el numero de pedido asignado al cliente
def  consultar_estado(pedido_id: str):
    if pedido_id not in pedidos:
        raise HTTPException(
            status_code=404, # codigo 404 not found
            detail="pedido no encontrado"
        )
    pedido = pedidos[pedido_id]
    
    return{
        "id":pedido["id"],
        "producto":pedido["producto"],
        "estado":pedido["estado"]
    }
    
    