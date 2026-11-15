from fastapi import FastAPI, HTTPException, Header
from pydantic import BaseModel, Field, field_validator
#con kafka
from uuid import uuid4
from confluent_kafka import KafkaException
from kafka_config import publicar_evento

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

app = FastAPI(title="CampusBite", description="Es una prueba de swagger con fastapi")
#prueba de swwagger con fastapi
@app.get("/salud", tags=["Salud"])
def consultar_salud():
    return {"salud": "ok"}
            #clave y valor
            
pedidos = {}
claves_indempondencia = {} #este diccionario relacionara cada clave con el pedido que ya fue creado


#queremos crear nuestro pedido 
# Asistido por IA: implementación inicial de Idempotency-Key.
@app.post("/pedidos", status_code=201, tags=["Pedidos"]) #status code 201 es para indicar que se creo un recurso
def crear_pedido(pedido: PedidoEntrada, idempotency_key :str = Header(..., alias="Idempotency-Key",min_length=1)): #obliga a que el JSON tenga producto y cantidad.
    if idempotency_key in claves_indempondencia:
        pedido_id = claves_indempondencia[idempotency_key]
        return pedidos[pedido_id]
    pedido_id = len(pedidos) + 1 # le asignamos un numero de turno al cliente, si esta vacio es 0, con la formula 0+1=1 se le asigno el turno 1
    
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
    claves_indempondencia[idempotency_key] = pedido_id

    return nuevo_pedido


#queremos consultar sobre nuestro pedido
@app.get("/pedidos/{pedido_id}/estado", tags=["Pedidos"]) #el {pedido_id} es el numero de pedido asignado al cliente
def  consultar_estado(pedido_id: int): 
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
    
    