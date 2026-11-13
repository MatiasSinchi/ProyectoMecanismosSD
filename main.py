from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

class PedidoEntrada(BaseModel):
    producto: str
    cantidad: int

app = FastAPI(title="CampusBite", description="Es una prueba de swagger con fastapi")
#prueba de swwagger con fastapi
@app.get("/salud", tags=["Salud"])
def consultar_salud():
    return {"salud": "ok"}
            #clave y valor
            
pedidos = {}

#queremos crear nuestro pedido
@app.post("/pedidos", status_code=201, tags=["Pedidos"]) #status code 201 es para indicar que se creo un recurso
def crear_pedido(pedido: PedidoEntrada): #obliga a que el JSON tenga producto y cantidad.
    pedido_id = len(pedidos) + 1 # le asignamos un numero de turno al cliente, si esta vacio es 0, con la formula 0+1=1 se le asigno el turno 1
    
    nuevo_pedido = {
        "id": pedido_id,
        "producto": pedido.producto,
        "cantidad": pedido.cantidad,
        "estado": "RECIBIDO"
    }
    pedidos[pedido_id] = nuevo_pedido
    return nuevo_pedido

#queremos consultar sobre nuestro pedido
@app.get("/pedidos/{pedido_id}", tags=["Pedidos"]) #el {pedido_id} es el numero de pedido asignado al cliente
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
    
    