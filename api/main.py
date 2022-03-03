# -*- coding: utf-8 -*-
__coauthor__ = "Oscar López"
__author__ = "Pedro Ortiz"
__copyright__ = "Copyright 2021, Literata"
__credits__ = ["Pedro Ortiz", "Oscar López"]
__license__ = "GPL"
__version__ = "1.0.0"
__email__ = ["pedrortiz@gmail.com", "lpz.oscr@gmail.com"]
__status__ = "Development"

import sys
sys.path.append("..")
from src.utils import *
from api.aux import *
from api.tools import *
from fastapi import FastAPI, UploadFile, File
from pydantic import BaseModel, Field
from typing import List, Optional
import uvicorn
from copy import deepcopy
from datetime import date, timedelta, datetime as dt
from concurrent.futures import ThreadPoolExecutor
import time

############################################################
# MODELOS DE DATOS
############################################################


class Entity(BaseModel):
    """Clase para las entidades nombradas."""

    text: str
    label: str


class EntitiesResponse(BaseModel):
    """Clase para la respuesta de la lista de licitantes"""

    # Lo de abajo es un ejemplo que hay que modificar para el caso específico.
    filename: str
    entities: List[Entity]

    class Config:
        schema_extra = {
            "example": {
                "filename": "ex_doc.pdf",
                "entities": [
                    {"text": "TRASLADOS MYG INTERNACIONAL S.A. DE CV", "label": "ORG"},
                    {"text": "ROSTRO CONSULTORES, S.C.", "label": "ORG"},
                ],
            }
        }


############################################################
# ENDPOINTS
############################################################

app = FastAPI(title="Extracción de Licitantes")

@app.post("/ner_extraction", tags=["Extraction"], response_model=List[EntitiesResponse])
async def extract_entities(
    files: List[UploadFile] = File(...),
    autoencoder: bool = False,
):
    """Extractor de nombres de empresas que se encuentran dentro un archivo PDF.

    ## Argumentos:
    - Listado de uno o más documentos en formato PDF.

    ## Respuesta
    JSON con los siguientes campos:
    - Filename: Nombre del archivo.
    - Entities: Lista de las entidades presentes en el PDF. Pueden incluye el
    contenido dentro de `text` y el tipo de entidad dentro de `label`. Se tiene
    el label ORG para organizaciones y el label PER para personas.

    ## Ejemplo de uso con Python
    ```python
    import requests

    url = "http://192.168.151.58:55659/ner_extraction/"
    full_name = 'dirección/completa/tu_documento.pdf'
    files = {'files': open(full_name, 'rb')}
    response = requests.post(url, files = files)
    print('Respuesta literata: ',response.status_code)

    >> 'Respuesta literata : 200'
    ```
    """
    start = time.time()
    results = []
    for file in files:
        pdf_bytes_0 = deepcopy(file.file.read())
        if autoencoder:
            futures = []
            with ThreadPoolExecutor() as executor:
                futures.append(
                    executor.submit(post_to_autoencoder, deepcopy(pdf_bytes_0))
                )
                futures.append(
                    executor.submit(literata_transcript, deepcopy(pdf_bytes_0))
                )
            pdf_clean_bytes = futures[0].result()
            txt0 = futures[1].result()
            # pdf_clean_bytes = post_to_autoencoder(pdf_bytes_0)
            # txt0 = literata_transcript(pdf_bytes_0)
            print(txt0)
            txt1 = literata_transcript(pdf_clean_bytes)
            print(txt1)
            txt = txt0 + "\n" + txt1
        else:
            txt = literata_transcript(pdf_bytes_0)
        # print(txt)
        txt_filename = file.filename.split(".")[0]
        set_google_key("../auth/literata_prod_datalake.json")
        try:
            upload_txt_to_dl(
                txt,
                txt_filename,
                bucket="uniclick-dl-literata-union",
                folder_bucket="api_extraccion_licitantes/transcripciones_actas",
            )
        except Exception as e:
            print(e)
            print("Texto no subido al bucket uniclick-dl-literata-union")
            pass
        licitantes = extract_ents(fix_lines_str(txt), raw=True)
        result = {"filename": file.filename, "entities": licitantes}
        results.append(result)
    upload_docs_to_mongo(results, data_base="compranet", collection="licitantes")
    print(time.time() - start)
    return results


############################################################
# EXECUTE SERVER
############################################################

if __name__ == "__main__":
    uvicorn.run(
        "main:app", host="0.0.0.0", port=55650, reload=True, debug=True, workers=5
    )
