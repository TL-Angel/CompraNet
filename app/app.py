
from cmath import e
import sys
sys.path.append("../")
from connections.datalakeconn import *
from scraper.tools import *
from scraper.aux import *
from scraper.SQLServer import *
from scraper.aux import write_logfile, write_report_actas
from requests_html import HTMLSession
import re
from datetime import date, timedelta, datetime as dt
from zipfile import ZipFile
from io import BytesIO
import os
import pandas as pd
import numpy as np
import time
from copy import deepcopy


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
        expedientes_anuales.prepare_data()
        expedientes_anuales.insertar_fecha_creacion()
        expedientes_anuales.insertar_fecha_modificacion_reg()
        expedientes_anuales.preparacion_estados()
        expedientes_anuales.mapear_id_estados()
        expedientes_anuales.data_frame_filtered = expedientes_anuales.data_frame_filtered.fillna('')
        print('DF filtradas:\n')
        print(expedientes_anuales.data_frame_filtered)
        # Las de abajo deben de ser las licitaciones filtradas - las licitaciones de la db
        expedientes_anuales.new_licitaciones = filter_new_licitaciones(
            expedientes_anuales.data_frame_filtered
            )
        expedientes_anuales.new_licitaciones.to_excel('../data/tmp/lici_news_{}.xlsx'.format(str(dt.today().replace(microsecond=0)).replace(":", "-")))
        # De igual manera debo de preguntar si las 
        print( 'New licitaciones:\n', expedientes_anuales.new_licitaciones)
        # Ahora pregunto que las licitaciones filtradas cuales ya están en DL
        expedientes_anuales.licitaciones_no_dl = filter_licitaciones_no_dl(expedientes_anuales.data_frame_filtered , expedientes_anuales.fecha, expedientes_anuales.data_frame_filtered.columns.tolist())
        print('\n', 'datos en DL:')
        print(expedientes_anuales.licitaciones_no_dl) 
        expedientes_anuales.licitaciones_no_dl.to_excel('../data/tmp/lici_NO_DL_{}.xlsx'.format(str(dt.today().replace(microsecond=0)).replace(":", "-")))
        #---------------------------
        # Mandar new licitaciones a DB Licitaciones
        #---------------------------
        sql = Conection('DWH')
        sql.InsertData(expedientes_anuales.new_licitaciones, TableName='Licitacion', FieldList=expedientes_anuales.new_licitaciones.columns.tolist())
        #---- Descargar actas -----------
        downloaded = DownloadExpedientes(expedientes_anuales)
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
