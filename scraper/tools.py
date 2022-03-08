import sys
sys.path.append("../")
from scraper.aux import *
from scraper.DBServer import *
from connections.datalakeconn import *
from src.utils import *
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
from copy import deepcopy
from pymongo import ReplaceOne


#############################################
# CONGIF ZONE
#############################################
LOG_PENDIENTES = "../data/logs/logs_actas_pendientes.txt"
XPATH_ACTA_PROPOSICIONES = '//tr[.//td[contains(text(),"Acta de presentac")]]//a/@onclick'
XPATH_ACTA_ACLARACIONES = '//tr[.//td[contains(text(),"Acta(s)")]]//a/@onclick'
TIPO_ACTA = 'proposiciones'
MAPEO_DF_DWH = {
    'Codigo del expediente': 'Codigo',
    'Numero del procedimiento': 'NumProc',
    'Caracter del procedimiento': 'CaracterProc',
    'Forma del procedimiento': 'FormaProc' ,
    'Articulo de excepcion': 'ArticuloExcepcion',
    'REFERENCIA_EXPEDIENTE': 'RefExpediente',
    'Titulo del expediente': 'TituloExpediente',
    'Plantilla del expediente': 'PlantillaExpediente', 
    'Descripcion del Anuncio': 'DescAnuncio', 
    'Clave de la UC': 'ClaveUC',
    'Nombre de la UC': 'NombreUC', 
    'Operador': 'Operador', 
    'Correo electronico': 'CorreoOperador',
    'Entidad federativa': 'IdEstado', 
    'Tipo de contratacion': 'TipoContratacion', 
    'Publicacion del anuncio': 'FechaPublicacion',
    'Vigencia del anuncio': 'Vigencia', 
    'Clave COG': 'ClaveCOG', 
    'Fecha de creacion':'FechaCreacion',
    'Fecha de ultima modificacion': 'FechaModificacion', 
    'Direccion del anuncio':'URLAnuncio',
    'opportunityId':'OpportunityId'
}
#############################################
# Clases para el proceso
#############################################

# ----------------


class ExpedientesPublicados(object):
    """
    Clase para el procesamiento de los expendientes descargados

    path_file = dirección del archivo
    """

    def __init__(self, path_file=None):
        self.path_file = path_file
        self.data_frame = None
        self.mongo_object = None
        self.filter_n_day = None

    def reading_expedientes_publicados(self):
        if ".xlsx" in self.path_file:
            self.data_frame = pd.read_excel(self.path_file)
        elif ".csv" in self.path_file:
            self.data_frame = pd.read_csv(self.path_file)
        else:
            print("Archivo no valido")
            return None
        self.data_frame["Fecha de creación"] = self.data_frame[
            "Fecha de creación"
        ].apply(dt.strptime, args=("%Y/%m/%d%H:%M",))
        self.data_frame["Fecha de última modificacion"] = self.data_frame[
            "Fecha de última modificacion"
        ].apply(dt.strptime, args=("%Y/%m/%d %H:%M",))

    def prepare_data(self):
        self.data_frame_filtered.columns = [
            remover_acentos(x) for x in list(self.data_frame_filtered.columns)
        ]
        self.data_frame_filtered = self.data_str(
            self.data_frame_filtered)
        self.data_frame_filtered = self.mapping_names_dwh( self.data_frame_filtered, MAPEO_DF_DWH)
        self.data_frame_filtered['ActaPublicada'] = 0
        self.data_frame_filtered['UrlActaDL'] = ''
        self.data_frame_filtered['NombreArchivoActa'] = ''
        self.data_frame_filtered['OpportunityId'] = self.data_frame_filtered['OpportunityId'].apply(int)
        self.data_frame_filtered['Codigo'] = self.data_frame_filtered['Codigo'].apply(str) 
        self.data_frame_filtered['ClaveCOG'] = self.data_frame_filtered['ClaveCOG'].apply(str)
    def filter_by_last_update(self):
        if self.filter_n_day is None:
            n_day = 1
        else:
            n_day = self.filter_n_day
        fecha = dt.today() - timedelta(days=n_day)
        self.fecha = fecha.replace(hour=0, minute=0, second=0, microsecond=0)
        self.data_frame_filtered = self.data_frame[
            self.data_frame["Fecha de última modificacion"] >= (self.fecha)
        ]
        print(self.fecha)
        print(
            "Catidad actas filtradas por ultima actualizacion: {}".format(
                len(self.data_frame_filtered)
            )
        )
        self.reporte_actas["actas_filtradas"].append(
            len(self.data_frame_filtered))
        self.reporte_actas["actas_filtradas"].append(
            self.data_frame_filtered["Dirección del anuncio"]
        )

    def data_str(self, df):
        for x in list(df.columns):
            if ("Fecha" in str(x)):
                try:
                    df.loc[:, x] = df.loc[:, x].apply(str)
                    df.loc[:, x] = df.loc[:, x].apply(
                        string_time, args=("%Y-%m-%d %H:%M:%S",)
                    )
                    df.loc[:, x] = df.loc[:, x].apply(
                        lambda x: x.replace(
                            microsecond=0,
                        )
                    )
                except Exception as e:
                    #print(e)
                    df.loc[:, x] = df.loc[:, x].apply(str)
                    df.loc[:, x] = df.loc[:, x].apply(
                        string_time, args=("%Y/%m/%d %H:%M",)
                    )
                    df.loc[:, x] = df.loc[:, x].apply(
                        lambda x: x.replace(
                            microsecond=0,
                        )
                    )
                # else:
                #    df.loc[:,x] = df.loc[:,x].apply(str)
                #    df.loc[:,x] = df.loc[:,x].apply(string_time, args=('%Y-%m-%d %H:%M:%S.%f',))
                #    df.loc[:,x] = df.loc[:,x].apply(lambda x: x.replace(microsecond=0,))
            #else:
            #    df.loc[:, x] = df.loc[:, x].apply(str)
            if ("Vigencia" in str(x)) or ("Publicacion" in str(x)):
                try:
                    df.loc[:, x] = df.loc[:, x].apply(str)
                    df.loc[:, x] = df.loc[:, x].apply(
                        string_time, args=("%Y-%m-%d %H:%M:%S",)
                    )
                    df.loc[:, x] = df.loc[:, x].apply(
                        lambda x: x.replace(
                            microsecond=0,
                        )
                    )
                except Exception as e:
                    #print(e)
                    df.loc[:, x] = df.loc[:, x].apply(str)
                    df.loc[:, x] = df.loc[:, x].apply(
                        string_time, args=("%Y-%m-%d %H:%M",)
                    )
                    df.loc[:, x] = df.loc[:, x].apply(
                        lambda x: x.replace(
                            microsecond=0,
                        )
                    )
        return df
    def mapping_names_dwh(self, df, mapeo):
        """Métopdo para cambiar los nombres del excel a los campos del DWH
        """
        return  df.rename(columns=mapeo)
    def preparacion_estados(self):
        """Método para preparar los nombre de estados.
        """
        self.data_frame_filtered['IdEstado'] = self.data_frame_filtered['IdEstado'].apply(str.upper)
        self.data_frame_filtered['IdEstado'] = self.data_frame_filtered['IdEstado'].apply(cleaning_by_line_v3)
        
    def mapear_id_estados(self):
        sql = Conection('ESTADO')
        df_id_estados = sql.GetIdEstados()
        self.data_frame_filtered['IdEstado'] = self.data_frame_filtered['IdEstado'].map(df_id_estados.set_index('Estado')['IdEstado'])
        self.data_frame_filtered['IdEstado'] = self.data_frame_filtered['IdEstado'].apply(str)
    def insertar_fecha_creacion(self):
        """Método para agregar la fecha de creación a los datos filtrados
        """
        self.data_frame_filtered['FechaCreacionReg'] = dt.today().replace(microsecond=0)
    def insertar_fecha_modificacion_reg(self):
        """Método para agregar la fecha de creación a los datos filtrados
        """
        self.data_frame_filtered['FechaModificacionReg'] = dt.today().replace(microsecond=0)
    def get_list_links(self):
        return self.data_frame["Dirección del anuncio"].to_list()

    def regex_opportunity_id(text):
        pattern = "(?<=opportunityId=)\d+"
        return re.findall(pattern, text)[0]

    def get_opportunity_id(self):
        try:
            self.data_frame["opportunityId"] = self.data_frame[
                "Dirección del anuncio"
            ].apply(ExpedientesPublicados.regex_opportunity_id)
        except Exception as e:
            print('Error in regex para id: ', e)
            pass


# ------------
class DownloadExpedientes(object):
    """Clase para gestionar la descarga de los archivos de compranet, tanto la lista (excel) de licitaciones,
    como los scrapes para descargar las actas de junta de aclaraciones.

    Args:
        object (Clase): El argumento es la clase que tiene la información de las licitaciones a descargar.
    """

    def __init__(
        self,
        child,
        url_expedientes="https://www.gob.mx/compranet/documentos/datos-abiertos-250375",
        year=None,
        month=None,
        path_datalake_folder=None,
    ):
        self.child = child
        self.year = year
        self.month = month
        self.url_expedientes = url_expedientes
        self.path_datalake_folder = path_datalake_folder
        self.tmp_data = r"../data/tmp/data"
        self.blob_name = "Actas_Junta_Aclaraciones"

    def download_data_expediente_publicados(self, year, month):
        """Método para gestionar la descarga de la lista de expedientes de la junta de actas
        de aclaraciones (excel).

        Args:
            year (str): Año de la lista de actas de junta de aclarciones
            month (str): Mes de la lista de actas de junta de aclarciones

        Returns:
            str: Nombre del archivo descargado
        """
        self.year = year
        self.month = month
        # FILTRAR POR AÑO Y MES PARA LUEGO DESCARGAR LAS ACTAS
        salida = []
        try:
            if (len(self.child.licitaciones_no_dl) == 0) & (
                len(self.child.new_licitaciones) == 0
            ):
                print("Sin datos que actualizar")
                return None
            elif len(self.child.licitaciones_no_dl) == 0:
                data = self.child.new_licitaciones
            elif len(self.child.new_licitaciones) == 0:
                data = self.child.licitaciones_no_dl
            else:
                data = pd.concat(
                    [
                        self.child.new_licitaciones,
                        self.child.licitaciones_no_dl,
                    ],
                    ignore_index=True,
                )
            data = data.drop_duplicates()
            print("Cantidad total de licitaciones a descargar: ", len(data))
            result_log = "Cantidad total de licitaciones a descargar: {}".format(
                str(len(data))
            )
            # ------------------------------------------------------------
            self.child.reporte_actas["total_actas_a_descargar"].append(
                len(data))
            self.write_logfile("Licitaciones", result_log)
            for id_exp, id_opt, ActaPublicada, UrlActaDL, NombreArchivoActa in data.loc[
                :, ["Codigo", "OpportunityId", "ActaPublicada", "UrlActaDL", "NombreArchivoActa"]
            ].values:
                try:
                    salida.append(self.get_file(id_opt, id_exp,ActaPublicada, UrlActaDL, NombreArchivoActa, ""))
                except Exception as e:
                    print("Error per request: {}".format(e))
                    result_log = "Error al solicitar descarga de acta: {}".format(
                        e)
                    id_file = "opportunity:{}_expediente:{}".format(
                        id_opt, id_exp)
                    self.write_logfile(id_file, result_log)
            return salida
        except Exception as e:
            print("Error al filtrar actas: {}".format(e))
            result_log = "Error al filtrar actas: {}".format(e)
            self.write_logfile("filtrado de actas", result_log)

    def download_links(self, links, contador, url_base, session, fileName, ActaPublicada, UrlActaDL, NombreArchivoActa, oppId, expId):
        if len(links) == 3:
            print("contador: {0}, item: {1}".format(contador, links[1]))
            #downloadFile = smartproxy(url_base + links[1], session)
            downloadFile = session.get(url_base + links[1])
            if downloadFile.status_code == 200:
                print(":::  obteniendo Acta  .......................................")
                ext_split = downloadFile.headers["Content-Disposition"].split(
                    ".")
                ext = ext_split[len(ext_split) - 1]
                file_name = fileName + "_" + str(contador) + "." + ext
                file_name_noext = fileName + "_" + str(contador) 
                try:
                    self.write_file(downloadFile, fileName, contador, ext)
                    print("Acta descargada")
                    result_log = "Acta descargada"
                    self.write_logfile(file_name, result_log)
                    self.child.reporte_actas["actas_descargadas"].append(
                        file_name)
                    # hacer un if para preguntar si este doc en especifico está en el mongo de licitaciones_publicas
                    # actualizar ActaPublicada si la descarga es exitosa CAMBIAR ActaPublicada
                    res = update_actas_descargadas( str(1), 'ActaPublicada', 'dbo.Licitacion', [ str(expId), str(oppId)])
                    print(res)
                except Exception as e:
                    print("Acta no descargada, error: {}".format(e))
                    result_log = "Acta no descargada. Error: {}".format(e)
                    self.write_logfile(fileName, result_log)
                    self.child.reporte_actas["actas_no_descargadas"].append(
                        file_name)
                ################## DATA LAKE #############################
                try:
                    #    CREAR UN NUEVO LOG QUE TENGA LA LISTA DE ARCHIVOS QUE NO FUERON MANDADOS AL DL
                    response = upload_file_to_dl(
                        str(self.blob_name), str(self.tmp_data), str(file_name), str(self.year), str(self.month)
                    )
                    print("Archivo subido a data lake?? ", response)
                    if response:
                        result_log = "Respuesta dl: {}".format(response)
                        self.write_logfile(file_name, result_log)
                        self.child.reporte_actas["actas_subidas_al_dl"].append(
                            file_name)
                        #Actualizar UrlActaDL, NombreArchivoActa si el acta fue subida al DL
                        blob_name = f"{self.blob_name}/{ self.year}/{self.month}/{file_name}"
                        nombre_acta = str(file_name_noext)
                        res = update_actas_subidas_dl( [blob_name, nombre_acta], [ 'UrlActaDL', 'NombreArchivoActa'], 'dbo.Licitacion', [ str(expId), str(oppId)])
                        print(res)
                        print('Codigo: ', str(expId))
                    else:
                        result_log = "Respuesta dl: {}".format(response)
                        self.write_logfile(file_name, result_log)
                        blob_name = f"{self.blob_name}/{ self.year}/{self.month}/{file_name}"
                        full_name = r"{0}/{1}".format(self.tmp_data, file_name)
                        self.write_logfile(
                            full_name, ' path_dl={}'.format(blob_name), LOG_PENDIENTES
                        )
                        self.child.reporte_actas["actas_no_subidas_al_dl"].append(
                            file_name)

                except Exception as e:
                    print("Acta no subida al data lake")
                    result_log = "Acta no subida al data lake, error: "
                    self.write_logfile(file_name, result_log + e)
                    self.write_logfile(
                        file_name, result_log + e, LOG_PENDIENTES)
                    print(e)
                    self.child.reporte_actas["actas_no_subidas_al_dl"].append(
                        file_name)
                #############################################################
                # COMIENZA PROCESO PARA ENVIAR A LITERATA
                #############################################################
                # enviado = self.send_file(fileName, str(contador), ext)
                # if enviado.status_code == 200:
                #     print("Acta procesada en literata: ok")
                #     result_log = "Acta Procesada en Literata: ok. Respuesta server literarta: {}".format(
                #         enviado.status_code
                #     )
                #     self.write_logfile(file_name, result_log)
                #     self.child.reporte_actas["actas_procesadas_literata"].append(
                #         file_name
                #     )
                #     return enviado
                # else:
                #     print("Status code: {}".format(enviado))
                #     result_log = "No se pudo insertar registro a la db. Respuesta server literarta: {}".format(
                #         enviado.status_code
                #     )
                #     self.write_logfile(file_name, result_log)
                #     self.child.reporte_actas["actas_no_procesadas_literata"].append(
                #         file_name
                #     )
                #     return None
                ###############################################################################
            else:
                self.child.reporte_actas["actas_no_descargadas"].append(
                    fileName)

    def write_file(self, downloadFile, fileName, contador, ext):
        """Método para escribir el archivo obtenido del scraper para actas de junta de aclaraciones

        Args:
            downloadFile (object): Respuesta del scraper para descargar el archivo
            fileName (str): Nombre del archivo
            contador (str): Contador para el archivo
            ext (str): Extensión del archivo
        """
        with open(
            r"{0}/{1}_{2}.{3}".format(self.tmp_data,
                                      fileName, contador, ext), "wb"
        ) as f:
            f.write(downloadFile.content)
        print(r"{0}/{1}_{2}.{3}".format(self.tmp_data, fileName, contador, ext))

    def send_file(self, fileName, contador, ext):
        """Método para enviar el archivo descargado al end point de 'ner_extraction' (literata)

        Args:
            fileName (str): Nombre del archivo
            contador (int): Contador del archivo, 1 sí es la primera vez que se descarga
            ext (str): Extensión del archivo

        Returns:
            object: Respuesta de la petición hecha al servicio 'ner_extraction'
        """
        url = "http://192.168.151.58:55659/ner_extraction/"
        full_name = r"{0}/{1}_{2}.{3}".format(
            self.tmp_data, fileName, contador, ext)
        try:
            files = {"files": open(full_name, "rb")}
            response = requests.post(url, files=files)
            print("Respuesta literata: ", response.status_code)
            return response
        except Exception as e:
            print("Error - archivo no cargado a literata: {}".format(e))

    def write_logfile(self, fileName, result_log, log_name="../data/logs/logs.txt"):
        """Método para escribir un acción al log file

        Args:
            fileName (str): Nombre del archivo o acción
            result_log (str): Descripción de la acción
        """

        with open(log_name, "a") as file:
            file.write(
                "\n{0},{1},{2}".format(
                    dt.now().strftime("%Y%m%d %H:%M:%S"), fileName, result_log
                )
            )

    def get_file(self, oppId, expId, ActaPublicada, UrlActaDL, NombreArchivoActa, db):
        """Método para descargar las actas de junta de aclaraciones.

        Args:
            oppId (str): Opportunity Id que tiene asignado el archivo
            expId (str): Código del expediente de la licitación
            db (str): Nombre de la db

        Returns:
            object: Respuesta de la descarga de cada archivo.
        """
        yearmonth = self.year + self.month
        fileName = "{0}_{1}_{2}_{3}".format(TIPO_ACTA, oppId, expId, yearmonth)
        with HTMLSession() as session:
            url_base = "https://compranet.hacienda.gob.mx"
            url_ = "https://compranet.hacienda.gob.mx/esop/guest/go/opportunity/detail?opportunityId={0}".format(
                oppId
            )
            try:
                response=session.get(url_)
                #response = smartproxy(url_, session)
                print("response: ", response.status_code)
                if response.status_code == 200:
                    print(
                        ":::  Respuesta correcta ........................................."
                    )
                    row_actas = response.html.xpath(
                        XPATH_ACTA_PROPOSICIONES
                    )
                    if row_actas != []:
                        contador = 1
                        print(
                            ":::  Acta localizada  ......................................."
                        )
                        result_log = "Acta localizada."
                        self.write_logfile(fileName, result_log)
                        output = []
                        for item in row_actas:
                            links = item.split("'")
                            output.append(
                                self.download_links(
                                    links, contador, url_base, session, fileName, ActaPublicada, UrlActaDL, NombreArchivoActa, oppId, expId
                                )
                            )
                            contador += 1
                        return output
                    else:
                        result_log = "No se encontraron Actas. longitud row_actas: {}".format(
                            str(len(row_actas))
                        )
                        self.write_logfile(fileName, result_log)
                        print(result_log)
                        self.child.reporte_actas["actas_no_descargadas"].append(
                            fileName
                        )
                else:
                    result_log = "Acta no descargada. Status_code: {}".format(
                        response.status_code
                    )
                    self.write_logfile(fileName, result_log)
                    print(result_log)
                    self.child.reporte_actas["actas_no_descargadas"].append(
                        fileName)
            except Exception as e:
                result_log = "No se descargo acta. Error: {}".format(e)
                self.write_logfile(fileName, result_log)
                print(result_log)
                self.child.reporte_actas["actas_no_descargadas"].append(
                    fileName)

    def get_expedientes_publicados(self, URL_EXPEDIENTES, current_year):
        """Método para gestionar la descarga de la lista de expedientes de la junta de actas.

        Args:
            URL_EXPEDIENTES (str): url de la lista de expedientes desde compranet
            current_year (str): Año de creación de la lista de expediendetes a descargar

        Returns:
            list: Lista con los nombres de los archivos descargados.
        """
        year_1 = str(int(current_year) - 1)
        year_2 = str(int(current_year) - 2)
        years = [current_year]  # , year_1, year_2]
        list_path = []
        for year in years:
            with HTMLSession() as session:
                # Obtenemos la pagina de descargas de contratos y expedientes
                response=session.get(URL_EXPEDIENTES)
                #response = smartproxy(URL_EXPEDIENTES, session, sticky=False)
                # Validamos la respuesta del servidor
                if response.status_code == 200:
                    msgError = "Conexión existosa.!"
                    # Obtenemos el archivo con los contratos del año
                    url_file = response.html.xpath(
                        '//strong//a[contains(@href,"ExpedientesPublicados{0}")]/@href'.format(
                            year
                        )
                    )
                    print(url_file)
                    if len(url_file) >= 1:
                        print("downloading,....")
                        res = session.get(url_file[0])
                        #res = smartproxy(url_file[0], session, sticky=False)
                        if res.status_code == 200:
                            result_log = (
                                "Url expediente encontrado. Status code: {}.".format(
                                    res.status_code
                                )
                            )
                            self.write_logfile(
                                "ExpedientesPublicados{0}".format(
                                    year), result_log
                            )
                            # Create a ZipFile Object in memory
                            with ZipFile(BytesIO(res.content)) as zipObj:
                                # Get a list of all archived file names from the zip
                                listOfFileNames = zipObj.namelist()
                                # Iterate over the file names
                                for fileName in listOfFileNames:
                                    # Check filename endswith csv
                                    print(fileName)
                                    if fileName.startswith(
                                        "ExpedientesPublicados{0}".format(year)
                                    ):
                                        # Extract a single file from zip
                                        zipObj.extract(fileName, self.tmp_data)
                                        list_path.append(
                                            self.tmp_data + fileName)
                                        result_log = "Expediente descargado: {}".format(
                                            fileName
                                        )
                                        self.write_logfile(
                                            fileName, result_log)
                else:
                    msgError = response.status_code
                    result_log = (
                        "Error al descargar expediente. Status code: {}.".format(
                            msgError
                        )
                    )
                    self.write_logfile(fileName, result_log)
        return list_path


# --------------
class MongoConnection(object):
    def __init__(self, child, mongo_uri=None):
        self.child = child
        self.mongo_uri = mongo_uri
        # Making a Connection with MongoClient
        self.client = MongoClient(self.mongo_uri)

    def start_connection(self, data_base, collection):
        # database
        self.db = self.client[data_base]
        # collection
        self.collection = self.db[collection]

    def prepare_data(self):
        self.child.data_frame_filtered.columns = [
            remover_acentos(x) for x in list(self.child.data_frame_filtered.columns)
        ]
        self.child.data_frame_filtered = self.data_str(
            self.child.data_frame_filtered)

    def write_logfile(self, fileName, result_log):
        with open("../data/logs/logs.txt", "a") as file:
            file.write(
                "\n{0},{1},{2}".format(
                    dt.now().strftime("%Y%m%d %H:%M:%S"), fileName, result_log
                )
            )

    def data_str(self, df):
        for x in list(df.columns):
            if "Fecha de ultima modificacion" in str(x):
                try:
                    df.loc[:, x] = df.loc[:, x].apply(str)
                    df.loc[:, x] = df.loc[:, x].apply(
                        string_time, args=("%Y-%m-%d %H:%M:%S",)
                    )
                    df.loc[:, x] = df.loc[:, x].apply(
                        lambda x: x.replace(
                            microsecond=0,
                        )
                    )
                except Exception as e:
                    print(e)
                    df.loc[:, x] = df.loc[:, x].apply(str)
                    df.loc[:, x] = df.loc[:, x].apply(
                        string_time, args=("%Y/%m/%d %H:%M",)
                    )
                    df.loc[:, x] = df.loc[:, x].apply(
                        lambda x: x.replace(
                            microsecond=0,
                        )
                    )
                # else:
                #    df.loc[:,x] = df.loc[:,x].apply(str)
                #    df.loc[:,x] = df.loc[:,x].apply(string_time, args=('%Y-%m-%d %H:%M:%S.%f',))
                #    df.loc[:,x] = df.loc[:,x].apply(lambda x: x.replace(microsecond=0,))
            else:
                df.loc[:, x] = df.loc[:, x].apply(str)
        return df

    def send_request_licitaciones(self):
        # Creamos la query para preguntar si los archivos existen en mongo, en la base de datos de licitaciones
        self.child.data_frame_filtered = self.data_str(
            self.child.data_frame_filtered)
        #filtered = self.child.data_frame_filtered
        #filtered.loc[:, "fecha_de_carga"] = dt.today().replace(microsecond=0)
        # enviando la data a la base de datos de licitaciones_publicas
        data_base = "compranet"
        collection = "licitaciones_publicas"
        self.start_connection(data_base, collection)

        # self.send_data_mongo(self.create_query_bulk_mongo(filtered))
        df_query = self.create_query_ids_mongo(self.child.data_frame_filtered)
        print(
            "Cantidad de actas a filtradas por cambios en los últimos {1} días: {0}".format(
                len(self.child.data_frame_filtered), str(
                    self.child.filter_n_day)
            )
        )
        result_log = "Cantidad de actas a filtradas por cambios en los últimos {1} días: {0}".format(
            len(self.child.data_frame_filtered), str(self.child.filter_n_day)
        )
        self.write_logfile("licitaciones", result_log)
        # Hacer petición para preguntar que ids ya existen en mongo
        df_query = self.find_docs(df_query)
        # filtrar el resultado del query con el dataframe del chunk
        self.child.new_licitaciones = self.filter_new_licitaciones(
            self.child.data_frame_filtered, df_query
        )
        print(
            "Cantidad de actas a nuevas para la base de datos licitaciones: {}".format(
                len(self.child.new_licitaciones)
            )
        )
        result_log = (
            "Cantidad de actas a nuevas para la base de datos licitaciones: {}".format(
                len(self.child.new_licitaciones)
            )
        )
        self.write_logfile("licitaciones nuevas mongo", result_log)
        self.child.reporte_actas["actas_nuevas_mongo_licitaciones_publicas"].append(
            len(self.child.new_licitaciones)
        )
        # pregunto si las docs existen en la base de mongo
        if len(self.child.new_licitaciones) == 0:
            print("Sin actas nuevas en mongodb licitaciones_publicas")
            self.child.data_frame_new_licitaciones_no_lite = pd.DataFrame([])
        else:
            print("Actas nuevas en mongodb licitaciones_publicas")
            # creando query para insertar
            news = self.child.new_licitaciones
            news.loc[:, "fecha_de_carga"] = dt.today().replace(microsecond=0)
            # enviando la data a la base de datos de licitaciones_publicas
            #data_base = "compranet"
            #collection = "licitaciones_publicas"
            #self.start_connection(data_base, collection)
            # self.send_data_mongo(self.create_query_bulk_mongo(news))
            # Cuantos de estos archivos han sido transformados por literata
            data_base = "compranet"
            collection = "licitantes"
            projections = ["expediente", "opportunity", "count"]
            columns = {
                "Codigo del expediente": "expediente",
                "opportunityId": "opportunity",
            }
            query = self.create_query(self.child.new_licitaciones, columns)
            # el problema es encontrar el filtro de los que no estan en literata
            query_res = self.find_documents(
                query, projections, data_base, collection)
            # TENGO QUE HACER UNA RESTA DE LOS NEW LICITANCIONES - QUERY, si quiery es cero entonces todos son nuevos
            if len(query_res) == 0:
                print(
                    "Todos las licitaciones son nuevas en mongodb licitantes: ",
                    len(self.child.new_licitaciones),
                )
                self.child.data_frame_new_licitaciones_no_lite = (
                    self.child.new_licitaciones
                )
            else:
                # aqui va la resta: new_licitantes - query_res
                self.child.data_frame_new_licitaciones_no_lite = (
                    self.filter_new_licitantes(
                        self.child.new_licitaciones, query_res)
                )
                print(
                    "Licitaciones nuevas en mongodb licitantes: ",
                    len(self.child.data_frame_new_licitaciones_no_lite),
                )
            self.child.reporte_actas["actas_nuevas_mongo_licitantes"].append(
                len(self.child.data_frame_new_licitaciones_no_lite)
            )

    def send_request_licitantes(self, fields, data_base, collection):
        # Creamos la query para preguntar si los archivos filtrados y que ya existen en licitaciones_publicas existen en mongo, en la base de datos de licitantes
        self.child.data_frame_filtered = self.data_str(
            self.child.data_frame_filtered)
        # Creamos el query que se enviara a la base de datos de mongo de licitantes
        query = self.create_query(self.child.data_frame_filtered, fields)
        print("# data filtrada: {}".format(
            len(self.child.data_frame_filtered)))
        # Hacer petición para preguntar que ids ya existen en mongo
        projections = list(fields.values()) + ["count"]
        df_query = self.find_documents(
            query, projections, data_base, collection)
        # si df_query == 0 entonces todas las actas no se han procesado
        # filtrar el resultado del query con el dataframe del chunk
        self.child.new_licitantes = self.filter_new_licitantes(
            self.child.data_frame_filtered, df_query
        )
        print("# nuevos licitantes: {}".format(len(self.child.new_licitantes)))
        self.child.reporte_actas[
            "actas_nuevas_mongo_licitantes_y_no_licitaciones_publicas"
        ].append(len(self.child.new_licitantes))
        # pregunto si las docs existen en la base de mongo de licitantes
        if len(self.child.new_licitantes) == 0:
            print("No hay actas de juntas por procesar")
        else:
            print("Nuevas actas por procesar")
            # creando query para insertar
            self.child.new_licitantes.loc[:, "fecha_de_carga"] = dt.today().replace(
                microsecond=0
            )
            # actualizando la data a la base de datos de licitaciones_publicas
            #data_base = "compranet"
            #collection = "licitaciones_publicas"
            #self.start_connection(data_base, collection)
            # self.send_data_mongo(self.create_query_bulk_mongo(self.child.new_licitantes))

    def send_data_mongo(self, chunk):
        """Enviar los datos filtrados a la base de datos en un server mongo"""
        self.upload_docs_to_mongo(chunk)

    def create_query_ids_mongo(
        self, df, columns=["Codigo del expediente", "opportunityId"]
    ):
        # creando query tipo json para mongo, aqui solo utilizo codigo y oportunity id
        send = df.loc[:, columns].to_dict("list")
        return {
            "Codigo del expediente": {"$in": send["Codigo del expediente"]},
            "opportunityId": {"$in": send["opportunityId"]},
        }

    def create_query(
        self,
        df,
        columns={"Codigo del expediente": "expediente",
                 "opportunityId": "opportunity"},
    ):
        # creando query tipo json para mongo, aqui solo utilizo codigo y oportunity id
        cols = list(columns.keys())
        send = df.loc[:, cols].to_dict("list")
        return {y: {"$in": send[x]} for x, y in columns.items()}

    def create_query_bulk_mongo(self, chunk):
        return chunk.to_dict("records")

    def docs_repeted(self, query):
        if self.count_docs(query) == 0:
            return True
        else:
            return False

    def count_docs(self, query):
        return len(query["opportunityId"]["$in"]) - self.collection.count_documents(
            query
        )

    def find_docs(
        self,
        query,
        projection_fields=[
            "Codigo del expediente",
            "opportunityId",
            "Fecha de ultima modificacion",
        ],
    ):
        query = self.collection.find(query, projection=projection_fields)
        return pd.DataFrame(query)

    def find_documents(self, query, projections, data_base, collection):
        # mongo = MongoConnection(None, mongo_uri=self.mongo_uri)
        self.start_connection(data_base, collection)
        res = self.collection.find(query, projection=projections)
        return pd.DataFrame(res)

    def filter_new_licitaciones(self, df, query):
        if len(query) > 0:
            return df[
                (df.opportunityId.isin(query.opportunityId) == False)
                & (
                    df["Codigo del expediente"].isin(
                        query["Codigo del expediente"])
                    == False
                )
                & (
                    df["Fecha de ultima modificacion"]
                    .apply(str)
                    .isin(query["Fecha de ultima modificacion"].apply(str))
                    == False
                )
            ]
        else:
            print("Todos los documentos son nuevos")
            return df

    def filter_new_licitantes(self, df, query):
        if len(query) > 0:
            query = query[query["count"] == "1"]
            return df[
                (df.opportunityId.isin(query["opportunity"]) == False)
                & (df["Codigo del expediente"].isin(query["expediente"]) == False)
            ]
        elif len(query) == 0:
            print("Todos los documentos son nuevos")
            return df

    def upload_docs_to_mongo(self, input_docs):
        """Sube los documentos Mongo con un uuid formado con el opportunity id y el id del expediente de cada licitación.

        Args:
            input_docs ([list]): Lista de documentos (diccionarios typo JSON) para subir a Mongodb

        Returns:
            [Bolean]: Regresa True o False dependiendo si fue o No exitoso la subida de docs
        """
        docs = deepcopy(input_docs)
        try:
            col = self.collection
            bulk_updates = []
            for doc in docs:
                doc["uuid"] = doc["Codigo del expediente"] + \
                    "_" + doc["opportunityId"]
                filter_ = {"uuid": doc["uuid"]}
                update_ = ReplaceOne(filter_, doc, upsert=True)
                bulk_updates.append(update_)
            bulk_result = col.bulk_write(bulk_updates)
            return True
        except Exception as e:
            print(e)
            return False


# -------
def string_time(string, formato="%Y-%m-%d %H:%M:%S.%f"):
    """Función para convertir un string en formato datetime

    Args:
        string ([str]): String de una fecha
        formato (str, optional): Es una variable de tipo string que indica el formato al cual se quiere convertir. Defaults to '%Y-%m-%d %H:%M:%S.%f'.

    Returns:
        [Datetime]: Una variable tipo Datetime
    """
    return dt.strptime(string, formato).replace(microsecond=0)


def entities_nombres(df, columns):
    """Uso el api para limpiar los nombres que han sido obtenidos de la base de datos de licitantes en mongo

    Args:
        df (Pandas DataFrame): data frame de entrada
        columns ([list]): lista de strings con los nombres de las columnas a procesar

    Returns:
        [Dataframe]: Regresa un data frame procesado
    """
    output = []
    for index, row in df.loc[:, columns].iterrows():
        if len(row["entities"]) > 0:
            for x in row["entities"]:
                fila = {str(x): row[x] for x in columns if x != "entities"}
                fila["nombre_licitante"] = x["text"]
                output.append(fila)
                # output.append({'nombre_licitante':x['text'], 'id_licitantes' : id_, 'expediente':expediente, 'opportunity':opportunity})
    return output


def send_data_licitantes_contacto(fileName):
    """Introduce una string con el expediente + el opporunity id de la licitación para hacer match de los nombres de licitantes correspondientes
    a la licitación y la base de datos de centro de conocimiento pyme

    Args:
        fileName ([string]): Es un string que contenga, al menos, "opporunity id"_"número de expediente"_+...
        La separación de cada elemento tiene que ser un "_"
    """
    mdb = MongoConnection(None, read_mongo("../auth/uri_robina.txt"))
    opportunity = fileName.split("_")[0]
    expediente = fileName.split("_")[1]
    collection = "licitantes"
    data_base = "compranet"
    projections = [
        "filename",
        "entities",
        "uuid",
        "expediente",
        "opportunity",
        "year",
        "month",
        "count",
        "fecha_carga",
    ]
    if "." in fileName:
        file_name = fileName.split(".")[0]
    query = {"uuid": file_name, "opportunity": opportunity,
             "expediente": expediente}
    mdb.start_connection(data_base, collection)
    df_licitantes = mdb.find_docs(query, projections)
    # ----Obteniendo fecha de actualizacion de licitaciones
    opportunity = fileName.split("_")[0]
    expediente = fileName.split("_")[1]
    collection = "licitaciones_publicas"
    data_base = "compranet"
    projections = [
        "Codigo del expediente",
        "opportunityId",
        "Fecha de ultima modificacion",
    ]
    query = {"opportunityId": opportunity, "Codigo del expediente": expediente}
    mdb.start_connection(data_base, collection)
    df_licitaciones = mdb.find_docs(query, projections)
    print("df_licitaciones: ", list(df_licitaciones.columns))
    print("df_licitantes: ", list(df_licitantes.columns))
    df_licitantes = df_licitantes.merge(
        df_licitaciones,
        how="inner",
        left_on=["opportunity", "expediente"],
        right_on=["opportunityId", "Codigo del expediente"],
        suffixes=("_licitantes", "_licitaciones_publicas"),
    )
    df_licitantes = df_licitantes.drop(
        columns=["Codigo del expediente", "opportunityId"]
    )
    # ---- Limpiando datos de licitantes
    dflicitantes = pd.DataFrame(
        entities_nombres(
            df_licitantes,
            [
                "entities",
                "_id_licitantes",
                "expediente",
                "opportunity",
                "Fecha de ultima modificacion",
                "_id_licitaciones_publicas",
                "count",
            ],
        )
    )
    print(list(dflicitantes.columns))
    dflicitantes["nombre_licitante"] = (
        dflicitantes["nombre_licitante"].apply(str).str.upper()
    )
    dflicitantes["nombre_licitante"] = dflicitantes["nombre_licitante"].apply(
        cleaning_by_line_v3, args=("nombres",)
    )
    dflicitantes["nombre_licitante"] = dflicitantes["nombre_licitante"].apply(
        delete_sa_cv
    )
    dflicitantes = dflicitantes.drop_duplicates()
    # ----- Obteniendo datos de CCP
    data_base = "datalake"
    collection = "ccp_new_summarized"
    projections = [
        "Nombre Establecimiento DENUE",
        "Razon Social DENUE",
        "Establecimientos DENUE",
        "HOOVERS",
        "Telefono DENUE",
        "Pagina web DENUE",
        "Correo Electronico DENUE",
    ]
    ls_output = []
    for (
        id_licitantes,
        expediente,
        opportunity,
        Fecha_de_ultima_modificacion,
        _id_licitaciones_publicas,
        count,
        name_,
    ) in dflicitantes.values:
        # Esta query solo toma la info del primer dict de Establecimiento, en el raro caso de tener más, falta esos casos raros
        query = {
            "$or": [
                {
                    "Establecimientos DENUE.0.raz_social": {
                        "$regex": name_,
                        "$options": "i",
                    }
                },
                {
                    "Establecimientos DENUE.0.nom_estab": {
                        "$regex": name_,
                        "$options": "i",
                    }
                },
                {"Nombre Establecimiento DENUE": {
                    "$regex": name_, "$options": "i"}},
                {"Razon Social DENUE": {"$regex": name_, "$options": "i"}},
                {"HOOVERS.0.Company Name": {"$regex": name_, "$options": "i"}},
            ]
        }
        # Con esto obtenemos un dataframe con la info de de las condiconales or de arriba
        outp_mul = ask_mongo_ccp(
            data_base, collection, projections, query=query)
        if len(outp_mul) > 0:
            if type(outp_mul.to_dict(orient="records")) == list:
                x = outp_mul.to_dict(orient="records")
            else:
                x = [outp_mul.to_dict(orient="records")]
            m = {
                "expediente": expediente,
                "opportunity": opportunity,
                "Fecha_de_ultima_modificacion": string_time(
                    Fecha_de_ultima_modificacion, formato="%Y-%m-%d %H:%M:%S"
                ),
                "id_licitantes": id_licitantes,
                "_id_licitaciones_publicas": _id_licitaciones_publicas,
                "informacion_contacto": x,
            }
            y = {"nombre_licitante": name_, "licitaciones": m}
            if len(y.values()) > 0:
                ls_output.append(y)
    # --- Agregando datos a la base de datos licitantes_contacto
    collection = "licitantes_contacto"
    data_base = "compranet"
    mdb.start_connection(collection=collection, data_base=data_base)
    return upload_docs_to_mongo_info_licitantes(ls_output)


# ---------
# --> obtengo todos los nombres de datalake en ccp
def ask_mongo_ccp(data_base, collection, projections, query={}):
    """Obtengo la lista de entidades obtenidas que están en la base de datos de licitantes"""
    mdb = MongoConnection(None, read_mongo("../auth/uri_robina.txt"))
    mdb.start_connection(data_base, collection)
    return mdb.find_docs(query, projections)


def delete_sa_cv(ent):
    """Función para limpiar los nombres de empresas, se elimina la denominación social.

    Args:
        ent ([string]): String que contiene los nombres de empresas a ser limpiados

    Returns:
        [string]: Nombre de empresa sin denominación social. Y se entrega en mayúsculas
    """
    rgx_lst = [
        " S[ \.]*A[ \.]* DE",
        " S[ \.]* EN C[ \.]*",
        " S[ \.]* EN N[ \.]*C[ \.]*",
        " S[ \.]*DE R[ \.]*L[ \.]*",
        "DE C[ \.]*V[ \.]*",
        "DE R[ \.]*L[ \.]*",
        "EN C[ \.]* POR A",
        " C[ \.]*S[ \.]*",
        "( S[ \.]*C[ \.]*)$",
        " S[ \.]*O[ \.]*F[ \.]*O[ \.]*M[ \.]*",
        " S[\. ]*C[\. ]*",
        " C[\. ]*V[ \.]*",
        " R[\. ]*L[ \.]*",
        " C[ \.]*S[ \.]*",
        #' S[ \.]*A[ \.]*',
        " S[ \.]*A[ \.]*P[ \.]*I[ \.]*",
        " S[ \.]*A[ \.]*A[\.]*",
        " S[ \.]*A[ \.]*C[\.]*",
        " S[ \.]*A[ \.]*S[ \.]*",
    ]
    rgx = "("
    for i, r in enumerate(rgx_lst):
        if i != 0:
            rgx += "|"
        rgx += r
    rgx += ")"
    denominacion = r"((\s)((((B[ ]{0,1})((V)))|((I[ ]{0,1})(((N[ ]{0,1})((C)))))|((P)(( DE | EN | )?)((C)))|(M[ ]{0,1}I)|((I)(( DE | EN | )?)((B[ ]{0,1}P)|(A[ ]{0,1}((S[ ]{0,1}P)|(P)|(S)))))|((F)(( DE | EN | )?)((A)|(C)))|((C[ ]{0,1})((E[ ]{0,1}L)|((POR )?A)))|((A)(( DE | EN | )?)((L(( DE | )?(P[ ]{0,1}R)))|(R(( DE | )?(I[ ]{0,1}C))?)|(B[ ]{0,1}P)|(P(([ ]{0,1}[ELN]))?)|([ACG])))|((U)(( DE | )?)(((S)(( DE | )?(P[ ]{0,1}R)))|(E(([ ]{0,1}C))?)|(C)))|((S[ ]{0,1}O[ ]{0,1}F[ ]{0,1})(((O[ ]{0,1})(([LM](([ ]{0,1}(E[ ]{0,1}(N[ ]{0,1})?R)))?)))|(I[ ]{0,1}P[ ]{0,1}O)))|((S[ ]{0,1}A)(([ ]{0,1}B)|([ ]{0,1}P[ ]{0,1}I)|([ ]{0,1}P[ ]{0,1}I[ ]{0,1}B))?)|((S)((( DE | EN | )?)((I((( DE | EN | )?)((I((( DE | )?(D(( PARA | )?(P[ ]{0,1}M))?))?))|(O[ ]{0,1}L[ ]{0,1})|(R[ ]{0,1}V[ ]{0,1})|(C[ ]{0,1}V)|(R[ ]{0,1}S)|(R[ ]{0,1}[IL])|(C[ ]{0,1}))?))|((C)((( DE | EN | )?)((C[ ]{0,1}V)|(([CP])((( DE | )?((B[ ]{0,1}S)|(S)))|([ ]{0,1}([RC])))?)|(A[ ]{0,1}P)|((POR )?A)|(R[ ]{0,1}[LVS])|(R[ ]{0,1}I)|([SPUL]))?))|(G[ ]{0,1}C)|(N[ ]{0,1}C)|((S[ ]{0,1})(S))|((P)(([ ]{0,1}(A|R)))?)|(L)))))|((S( DE | DE| )?R[ ]{0,1}L)|(DE |DE| )?((R[ ]{0,1}L)|(I[ ]{0,1}P)|(C[ ]{0,1}V)|(R[ ]{0,1}S)|(R[ ]{0,1}I)|(R[ ]{0,1}V)|(O[ ]{0,1}L)|(A[ ]{0,1}R[ ]{0,1}T)|(M[ ]{0,1}I)|((E[ ]{0,1}(N[ ]{0,1})?R)))))(\b))"
    spans = re.search(rgx, ent.upper(), re.IGNORECASE)
    if spans is not None:
        y = re.sub(denominacion, "", ent.upper(), flags=re.IGNORECASE)
        return re.sub(rgx, "", y, flags=re.IGNORECASE)
    else:
        return ent
