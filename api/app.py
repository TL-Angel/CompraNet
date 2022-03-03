from cmath import e
import sys

sys.path.append("../")
from requests_html import HTMLSession
import re
from datetime import date, timedelta, datetime as dt
from zipfile import ZipFile
from io import BytesIO
import os
import pandas as pd
import numpy as np
import json
import random
import time
import bson
import base64
from bson.binary import Binary
import requests
import pymongo
from pymongo import MongoClient
from src.utilsv2 import *
from copy import deepcopy
from pymongo import ReplaceOne
from connections.datalakeconn import *
from tools import *
from aux import write_logfile, write_report_actas


def app(n_days=1):
    reporte_actas = {}
    start = time.time()
    URL_EXPEDIENTES = "https://www.gob.mx/compranet/documentos/datos-abiertos-250375"
    today = dt.today() - timedelta(days=0, hours=6, minutes=0)
    today = today.replace(hour=0, minute=0, second=0, microsecond=0)
    current_year = str(today.year)
    month = str(today.month)
    expedientes = DownloadExpedientes(None)
    expedientes.tmp_data = r"../data/tmp/"
    list_expedientes = expedientes.get_expedientes_publicados(
        URL_EXPEDIENTES, current_year
    )
    for excel in list_expedientes:
        actas = {
            "actas_filtradas": [],
            "actas_nuevas_mongo_licitaciones_publicas": [],
            "actas_nuevas_mongo_licitantes": [],
            "actas_nuevas_mongo_licitantes_y_no_licitaciones_publicas": [],
            "total_actas_a_descargar": [],
            "actas_no_subidas_al_dl": [],
            "actas_no_descargadas": [],
            "actas_no_procesadas_literata": [],
            "actas_procesadas_literata": [],
            "actas_descargadas": [],
            "actas_subidas_al_dl": [],
        }
        print("Empezando proceso de extracción para {}".format(excel))
        expedientes_anuales = ExpedientesPublicados(path_file=excel)
        print('lectura excel ok)')
        expedientes_anuales.reporte_actas = actas
        expedientes_anuales.reading_expedientes_publicados()
        expedientes_anuales.get_opportunity_id()
        expedientes_anuales.filter_n_day = n_days
        expedientes_anuales.filter_by_last_update()
        # ------ mongo data base ---------
        mongo_uri = read_mongo("../auth/uri_robina.txt")
        mongodb = MongoConnection(expedientes_anuales, mongo_uri=mongo_uri)
        mongodb.prepare_data()
        projections = ["expediente", "opportunity", "count"]
        data_base = "compranet"
        collection = "licitaciones_publicas"
        mongodb.start_connection(data_base, collection)
        mongodb.send_request_licitaciones()
        data_base = "compranet"
        collection = "licitantes"
        mongodb.start_connection(data_base, collection)
        columns = {
            "Codigo del expediente": "expediente",
            "opportunityId": "opportunity",
        }
        mongodb.send_request_licitantes(columns, data_base, collection)
        downloaded = DownloadExpedientes(mongodb.child)
        downloaded.tmp_data = r"../data/tmp/data"
        downloaded.download_data_expediente_publicados(current_year, month)
        reporte_actas[
            excel.replace("../data/tmp/", "")
        ] = downloaded.child.reporte_actas
    write_report_actas(reporte_actas)
    print(time.time() - start)


if __name__ == "__main__":
    try:
        print(sys.argv)
        n_days = int(sys.argv[1])
        app(n_days)
    except KeyboardInterrupt as e:
        print("Interrupted")
        fileName = "app.py"
        result_log = "KeyboardInterrupt error {}".format(e)
        write_logfile(fileName, result_log)
        try:
            sys.exit(0)
        except SystemExit as e:
            fileName = "app.py"
            result_log = "SystemExit error: {}".format(e)
            write_logfile(fileName, result_log)
            os._exit(0)
