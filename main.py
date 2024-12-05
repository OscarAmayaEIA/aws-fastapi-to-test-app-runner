import csv
import datetime
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from pydantic import BaseModel
from pathlib import Path

app = FastAPI()
CSV_FILE = "sensor_data.csv"

# Crear el archivo CSV si no existe
if not Path(CSV_FILE).exists():
    with open(CSV_FILE, mode="w", newline="") as file:
        writer = csv.writer(file)
        # Encabezados del archivo CSV
        writer.writerow(["Device_ID", "Topic", "Value", "Fecha_Recepcion"])

# Modelo de datos para la API
class SensorData(BaseModel):
    device_id: str
    topic: str
    value: str
@app.get("/api/download/")
async def download_csv():
    """
    Descargar el archivo CSV.
    """
    return FileResponse(CSV_FILE, media_type='application/octet-stream', filename="sensor_data.csv")

@app.post("/api/dots/")
async def receive_sensor_data(data: SensorData):
    """
    Recibir datos del sensor y guardarlos en un archivo CSV.
    """
    try:
        with open(CSV_FILE, mode="a", newline="") as file:
            writer = csv.writer(file)
            # Registrar la fecha de recepción en el servidor
            aux=data.value.split(";")
            if len(aux)==2:
                aux_=aux[0].split(":")
                data.value=aux_[1]
                # fecha_recepcion=aux[1]
            else:
                pass
            fecha_recepcion = datetime.datetime.now().isoformat()
            # Escribir en el archivo CSV
            writer.writerow([data.device_id, data.topic, data.value, fecha_recepcion])
        return {"message": "Datos recibidos y guardados con éxito."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al guardar los datos: {e}")

@app.get("/api/dots/")
async def get_sensor_data():
    """
    Obtener todos los datos del sensor guardados en el archivo CSV como JSON ordenado por variable.
    """
    try:
        with open(CSV_FILE, mode="r") as file:
            reader = csv.DictReader(file)
            data = {}
            for row in reader:
                # Separar los datos por el nombre del topic
                try:
                    topic = row["Topic"].split("/")[1]
                except IndexError:
                    topic = row["Topic"]

                if topic not in data:
                    data[topic] = []
                data[topic].append({
                    "Device_ID": row["Device_ID"],
                    "Value": row["Value"],
                    "Fecha_Recepcion": row["Fecha_Recepcion"],
                })

        return data
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al leer los datos: {e}")

if __name__ == "__main__":
    import uvicorn  
    uvicorn.run(app, host="0.0.0.0", port=8000)

