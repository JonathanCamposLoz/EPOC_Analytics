from typing import Optional

from fastapi import FastAPI, Query, Path, Body
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel


from src.pollution import Pollution
from src.weather import Weather


app = FastAPI()

origins = ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins = origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class Coordenadas(BaseModel):
    latitud: str
    longitud: str


@app.post("/api/EPOC/information")
def download_information(
                        coordenadas: Coordenadas = Body(..., 
                                                        description='It is necesary latitud and longitud to get information')):
    
    print(dict(coordenadas))
    answer = 'ok'
    return {"answer": answer}

'''
p = Pollution()
p.get_dataPollution('bogota')


w = Weather()
w.get_dataWeather('bogota')

'''


