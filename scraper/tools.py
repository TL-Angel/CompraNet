import sys
sys.path.append("../")
from scraper.aux_compranet import *
from connections.DBServer import *
from connections.datalakeconn import *
from requests_html import HTMLSession
import re
from datetime import timedelta, datetime as dt
from zipfile import ZipFile
from io import BytesIO
import pandas as pd
import random
import time
import requests
from copy import deepcopy


#############################################
# CONGIF ZONE
#############################################
TMP_EXPEDIENTES = r"../data/tmp/"
TMP_ACTAS = r"../data/tmp/data"
LOG_PENDIENTES = "../data/logs/logs_actas_pendientes.txt"
XPATH_ACTA_PROPOSICIONES = (
    '//tr[.//td[contains(text(),"Acta de presentac")]]//a/@onclick'
)
XPATH_ACTA_ACLARACIONES = '//tr[.//td[contains(text(),"Acta(s)")]]//a/@onclick'
TIPO_ACTA = "proposiciones"
BLOB_FOLDER = "Acta_Presentacion_Y_Proposiciones"
MAPEO_DF_DWH = {
    "Codigo del expediente": "Codigo",
    "Numero del procedimiento": "NumProc",
    "Caracter del procedimiento": "CaracterProc",
    "Forma del procedimiento": "FormaProc",
    "Articulo de excepcion": "ArticuloExcepcion",
    "REFERENCIA_EXPEDIENTE": "RefExpediente",
    "Titulo del expediente": "TituloExpediente",
    "Plantilla del expediente": "PlantillaExpediente",
    "Descripcion del Anuncio": "DescAnuncio",
    "Clave de la UC": "ClaveUC",
    "Nombre de la UC": "NombreUC",
    "Operador": "Operador",
    "Correo electronico": "CorreoOperador",
    "Entidad federativa": "IdEstado",
    "Tipo de contratacion": "TipoContratacion",
    "Publicacion del anuncio": "FechaPublicacion",
    "Vigencia del anuncio": "Vigencia",
    "Clave COG": "ClaveCOG",
    "Fecha de creacion": "FechaCreacion",
    "Fecha de ultima modificacion": "FechaModificacion",
    "Direccion del anuncio": "URLAnuncio",
    "opportunityId": "OpportunityId",
}
#############################################
# Clases para el proceso
#############################################


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
        print('Lectura de Expedientes ok')

    def prepare_data(self):
        self.data_frame_filtered.columns = [
            remover_acentos(x) for x in list(self.data_frame_filtered.columns)
        ]
        self.data_frame_filtered = self.data_str(self.data_frame_filtered)
        self.data_frame_filtered = self.mapping_names_dwh(
            self.data_frame_filtered, MAPEO_DF_DWH
        )
        self.data_frame_filtered['DescAnuncio'] = self.data_frame_filtered['DescAnuncio'].apply(str).apply(lambda x: x.replace("'", ""))
        self.data_frame_filtered["ActaPublicada"] = 0
        self.data_frame_filtered["UrlActaDL"] = ""
        self.data_frame_filtered["NombreArchivoActa"] = ""
        self.data_frame_filtered["OpportunityId"] = self.data_frame_filtered[
            "OpportunityId"
        ].apply(int)
        self.data_frame_filtered["Codigo"] = self.data_frame_filtered["Codigo"].apply(
            str
        )
        self.data_frame_filtered["ClaveCOG"] = self.data_frame_filtered[
            "ClaveCOG"
        ].apply(str)

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
        print("Fecha última modificación: ", self.fecha)
        print(
            "Catidad actas filtradas por ultima actualizacion: {}".format(
                len(self.data_frame_filtered)
            )
        )
        self.reporte_actas["actas_filtradas"].append(len(self.data_frame_filtered))
        self.reporte_actas["actas_filtradas"].append(
            self.data_frame_filtered["Dirección del anuncio"]
        )

    def data_str(self, df):
        for x in list(df.columns):
            if "Fecha" in str(x):
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
                    df.loc[:, x] = df.loc[:, x].apply(str)
                    df.loc[:, x] = df.loc[:, x].apply(
                        string_time, args=("%Y/%m/%d %H:%M:%S",)
                    )
                    df.loc[:, x] = df.loc[:, x].apply(
                        lambda x: x.replace(
                            microsecond=0,
                        )
                    )

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
        """Métopdo para cambiar los nombres del excel a los campos del DWH"""
        return df.rename(columns=mapeo)

    def preparacion_estados(self):
        """Método para preparar los nombre de estados."""
        self.data_frame_filtered["IdEstado"] = self.data_frame_filtered[
            "IdEstado"
        ].apply(str.upper)
        self.data_frame_filtered["IdEstado"] = self.data_frame_filtered[
            "IdEstado"
        ].apply(cleaning_by_line_v3)

    def mapear_id_estados(self):
        sql = Conection("ESTADO")
        df_id_estados = sql.GetIdEstados()
        self.data_frame_filtered["IdEstado"] = self.data_frame_filtered["IdEstado"].map(
            df_id_estados.set_index("Estado")["IdEstado"]
        )
        self.data_frame_filtered["IdEstado"] = self.data_frame_filtered[
            "IdEstado"
        ].apply(str)

    def insertar_fecha_creacion(self):
        """Método para agregar la fecha de creación a los datos filtrados"""
        self.data_frame_filtered["FechaCreacionReg"] = dt.today().replace(microsecond=0)

    def insertar_fecha_modificacion_reg(self):
        """Método para agregar la fecha de creación a los datos filtrados"""
        self.data_frame_filtered["FechaModificacionReg"] = dt.today().replace(
            microsecond=0
        )

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
            print("Error in regex para id: ", e)
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
        self.tmp_data = TMP_ACTAS
        self.blob_name = BLOB_FOLDER

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
        self.month = str(month).zfill(2)
        salida = []
        try:
            if (len(self.child.uploaded_no_downloaded) == 0) & (
                len(self.child.new_licitaciones) == 0
            ):
                print("Sin datos que actualizar")
                return None
            elif (len(self.child.uploaded_no_downloaded) == 0) & (len(self.child.new_licitaciones) > 0):
                data = self.child.new_licitaciones
            elif (len(self.child.new_licitaciones) == 0) & (len(self.child.uploaded_no_downloaded) > 0):
                data = self.child.uploaded_no_downloaded
            else:
                data = pd.concat(
                    [
                        self.child.new_licitaciones,
                        self.child.uploaded_no_downloaded,
                    ],
                    ignore_index=True,
                )
            data = data.drop_duplicates()
            print("Cantidad total de licitaciones a descargar: ", len(data))
            result_log = "Cantidad total de licitaciones a descargar: {}".format(
                str(len(data))
            )
            # ------------------------------------------------------------
            self.child.reporte_actas["total_actas_a_descargar"].append(len(data))
            self.write_logfile("Licitaciones", result_log)
            sleep_time = 10
            for id_exp, id_opt, ActaPublicada, UrlActaDL, NombreArchivoActa in data.loc[
                :,
                [
                    "Codigo",
                    "OpportunityId",
                    "ActaPublicada",
                    "UrlActaDL",
                    "NombreArchivoActa",
                ],
            ].values:
                try:
                    salida.append(
                        self.get_file(
                            id_opt,
                            id_exp,
                            ActaPublicada,
                            UrlActaDL,
                            NombreArchivoActa,
                            "",
                        )
                    )
                except Exception as e:
                    print("Error per request: {}".format(e))
                    result_log = "Error al solicitar descarga de acta: {}".format(e)
                    id_file = "opportunity:{}_expediente:{}".format(id_opt, id_exp)
                    self.write_logfile(id_file, result_log)
                r_t = random.choice(range(-int(sleep_time*0.7), int(sleep_time*0.7)))
                t = int(sleep_time) + r_t
                time.sleep(t)
            return salida
        except Exception as e:
            print("Error al filtrar actas: {}".format(e))
            result_log = "Error al filtrar actas: {}".format(e)
            self.write_logfile("filtrado de actas", result_log)

    def download_links(
        self,
        links,
        contador,
        url_base,
        session,
        fileName,
        ActaPublicada,
        UrlActaDL,
        NombreArchivoActa,
        oppId,
        expId,
    ):
        if len(links) == 3:
            print("contador: {0}, item: {1}".format(contador, links[1]))
            downloadFile = session.get(url_base + links[1], proxies=proxy())
            if downloadFile.status_code == 200:
                print(":::  obteniendo Acta  .......................................")
                ext_split = downloadFile.headers["Content-Disposition"].split(".")
                ext = ext_split[len(ext_split) - 1]
                file_name = fileName + "_" + str(contador) + "." + ext
                file_name_noext = fileName + "_" + str(contador)
                try:
                    self.write_file(downloadFile, fileName, contador, ext)
                    print("Acta descargada")
                    result_log = "Acta descargada"
                    self.write_logfile(file_name, result_log)
                    self.child.reporte_actas["actas_descargadas"].append(file_name)
                    # hacer un if para preguntar si este doc en especifico está en el mongo de licitaciones_publicas
                    # actualizar ActaPublicada si la descarga es exitosa CAMBIAR ActaPublicada
                    res = update_actas_descargadas(
                        str(1),
                        "ActaPublicada",
                        "dbo.Licitacion",
                        [str(expId), str(oppId)],
                    )
                    print(res)
                except Exception as e:
                    print("Acta no descargada, error: {}".format(e))
                    result_log = "Acta no descargada. Error: {}".format(e)
                    self.write_logfile(fileName, result_log)
                    self.child.reporte_actas["actas_no_descargadas"].append(file_name)
                ################## DATA LAKE #############################
                try:
                    #    CREAR UN NUEVO LOG QUE TENGA LA LISTA DE ARCHIVOS QUE NO FUERON MANDADOS AL DL
                    response = upload_file_to_dl(
                        str(self.blob_name),
                        str(self.tmp_data),
                        str(file_name),
                        str(self.year),
                        str(self.month).zfill(2),
                    )
                    print("Archivo subido a data lake?? ", response)
                    if response:
                        result_log = "Respuesta dl: {}".format(response)
                        self.write_logfile(file_name, result_log)
                        self.child.reporte_actas["actas_subidas_al_dl"].append(
                            file_name
                        )
                        # Actualizar UrlActaDL, NombreArchivoActa si el acta fue subida al DL
                        blob_name = (
                            f"{self.blob_name}/{ self.year}/{str(self.month).zfill(2)}/{file_name}"
                        )
                        nombre_acta = str(file_name_noext)
                        res = update_actas_subidas_dl(
                            [blob_name, nombre_acta],
                            ["UrlActaDL", "NombreArchivoActa"],
                            "dbo.Licitacion",
                            [str(expId), str(oppId)],
                        )
                        print(res)
                        print("Codigo: ", str(expId))
                    else:
                        result_log = "Respuesta dl: {}".format(response)
                        self.write_logfile(file_name, result_log)
                        blob_name = (
                            f"{self.blob_name}/{ self.year}/{str(self.month).zfill(2)}/{file_name}"
                        )
                        full_name = r"{0}/{1}".format(self.tmp_data, file_name)
                        self.write_logfile(
                            full_name, " path_dl={}".format(blob_name), LOG_PENDIENTES
                        )
                        self.child.reporte_actas["actas_no_subidas_al_dl"].append(
                            file_name
                        )

                except Exception as e:
                    print("Acta no subida al data lake")
                    result_log = "Acta no subida al data lake, error: "
                    self.write_logfile(file_name, result_log + e)
                    self.write_logfile(file_name, result_log + e, LOG_PENDIENTES)
                    print(e)
                    self.child.reporte_actas["actas_no_subidas_al_dl"].append(file_name)
            else:
                self.child.reporte_actas["actas_no_descargadas"].append(fileName)

    def write_file(self, downloadFile, fileName, contador, ext):
        """Método para escribir el archivo obtenido del scraper para actas de junta de aclaraciones

        Args:
            downloadFile (object): Respuesta del scraper para descargar el archivo
            fileName (str): Nombre del archivo
            contador (str): Contador para el archivo
            ext (str): Extensión del archivo
        """
        with open(
            r"{0}/{1}_{2}.{3}".format(self.tmp_data, fileName, contador, ext), "wb"
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
        full_name = r"{0}/{1}_{2}.{3}".format(self.tmp_data, fileName, contador, ext)
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
        yearmonth = self.year + str(self.month).zfill(2)
        fileName = "{0}_{1}_{2}_{3}".format(TIPO_ACTA, oppId, expId, yearmonth)
        with HTMLSession() as session:
            url_base = "https://compranet.hacienda.gob.mx"
            url_ = "https://compranet.hacienda.gob.mx/esop/guest/go/opportunity/detail?opportunityId={0}".format(
                oppId
            )
            try:
                response = session.get(url_, proxies=proxy())
                print("response: ", response.status_code)
                if response.status_code == 200:
                    print(
                        ":::  Respuesta correcta ........................................."
                    )
                    row_actas = response.html.xpath(XPATH_ACTA_PROPOSICIONES)
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
                                    links,
                                    contador,
                                    url_base,
                                    session,
                                    fileName,
                                    ActaPublicada,
                                    UrlActaDL,
                                    NombreArchivoActa,
                                    oppId,
                                    expId,
                                )
                            )
                            contador += 1
                        return output
                    else:
                        result_log = (
                            "No se encontraron Actas. longitud row_actas: {}".format(
                                str(len(row_actas))
                            )
                        )
                        self.write_logfile(fileName, result_log)
                        print(result_log)
                        update_actas(
                            values=1,
                            columns="Url_visitada",
                            table="DWH_ANALYTICS.dbo.Licitacion",
                            condicion=[expId, oppId]
                            )
                        self.child.reporte_actas["actas_no_descargadas"].append(
                            fileName
                        )
                else:
                    result_log = "Acta no descargada. Status_code: {}".format(
                        response.status_code
                    )
                    # Pendientes_descargar
                    self.write_logfile(fileName, result_log)
                    print(result_log)
                    self.child.reporte_actas["actas_no_descargadas"].append(fileName)
            except Exception as e:
                result_log = "No se descargo acta. Error: {}".format(e)
                self.write_logfile(fileName, result_log)
                print(result_log)
                # Pendientes_descargar
                self.child.reporte_actas["actas_no_descargadas"].append(fileName)
            finally:
                session.close()

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
        years = [current_year]
        list_path = []
        for year in years:
            with HTMLSession() as session:
                # Obtenemos la pagina de descargas de contratos y expedientes
                response = session.get(URL_EXPEDIENTES)
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
                        if res.status_code == 200:
                            result_log = (
                                "Url expediente encontrado. Status code: {}.".format(
                                    res.status_code
                                )
                            )
                            self.write_logfile(
                                "ExpedientesPublicados{0}".format(year), result_log
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
                                        list_path.append(self.tmp_data + fileName)
                                        result_log = "Expediente descargado: {}".format(
                                            fileName
                                        )
                                        self.write_logfile(fileName, result_log)
                else:
                    msgError = response.status_code
                    result_log = (
                        "Error al descargar expediente. Status code: {}.".format(
                            msgError
                        )
                    )
                    self.write_logfile(fileName, result_log)
        return list_path

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
