# -*- coding: utf-8 -*-
__author__ = 'Oscar López'
__copyright__ = 'Copyright 2021, Literata'
__credits__ = ['Oscar López']
__license__ = 'GPL'
__version__ = '1.0.0'
__email__ = 'lpz.oscr@gmail.com'
__status__ = 'Development'

import sys
sys.path.append('..')
# from src.utils import *
from api.aux import *
from fastapi import FastAPI, UploadFile, File, Query
from fastapi.responses import StreamingResponse
# from api.security import *
# from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from pydantic import BaseModel, Field
from typing import List, Optional
import uvicorn
from copy import deepcopy
import numpy as np
from tensorflow.keras.models import load_model
# from pdf_cleaner.utils import *
from pdf2image import convert_from_path, convert_from_bytes
import matplotlib.pyplot as plt
from PIL import Image
from pathlib import Path
import os
from io import BytesIO

os.environ['CUDA_VISIBLE_DEVICES'] = '-1'


AUTOENCODER = load_model('../../pdf_cleaner/data/models/elch.h5')


def get_pages_batch(pdf_bytes):
    batch_ = []
    for page in convert_from_bytes(pdf_bytes):
        rgb_arr = np.array(page)
        batch_.append(np.dot(rgb_arr[...,:3], [0.2989, 0.5870, 0.1140])/255)
    return batch_

def clean_pdf(pdf_bytes):
    input_pages = get_pages_batch(pdf_bytes)
    predictions = []
    for page in input_pages:
        predictions.append(AUTOENCODER.predict(np.expand_dims(page, axis=0)))

    pred_pages = []
    for p in predictions:
        img = Image.fromarray((p[0][:, :, 0] * 255).astype(np.uint8))
        pred_pages.append(img)

    output_path = Path('./data/tmp')
    output_path.mkdir(exist_ok=True, parents=True)

    pred_pages[0].save('./data/tmp/output_prediction.pdf', 'PDF',
                       resolution=100.0,
                       save_all=True,
                       append_images=pred_pages[1:])
    with open('./data/tmp/output_prediction.pdf', 'rb') as f:
        pdf_pred_bytes = f.read()

    return pdf_pred_bytes


############################################################
# ENDPOINTS
############################################################

app = FastAPI(title='Autoencoder Limpieza')

@app.post('/autoencoder_clean', tags=['Extraction'])
async def autoencoder_clean(filename: str,
                            file: UploadFile = File(...),
                            # token: str=Depends(OAUTH2_SCHEME)
                            ):
    pdf_pred_bytes = clean_pdf(file.file.read())
    response = StreamingResponse(BytesIO(pdf_pred_bytes),
                                 media_type='application/pdf')
    content_disposition = f"attachment; filename={filename}.pdf"
    response.headers['Content-Disposition'] = content_disposition
    return response


############################################################
# EXECUTE SERVER
############################################################

if __name__ == '__main__':

    uvicorn.run('cleaner:app',
                host='0.0.0.0',
                port=56651,
                reload=True,
                debug=True,
                workers=5)
