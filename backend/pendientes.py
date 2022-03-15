import sys
sys.path.append("../")
from connections.DBServer import *
from scraper.aux_compranet import *
from scraper.tools import *
import time
##############################
# CONFIG
##############################
BUCKET = "uniclick-dl-robina-compranet"
folder_bucket = "Actas_Junta_Aclaraciones/"
LOCAL_FOLDER = "../data/tmp/data"
BLOB_FOLDER = "Acta_Junta_Aclaraciones"
##############################


def buscar_actas_pendientes():
    """Método para buscar en la DB las actas pendientes
    """
    fields = ['Codigo', 'OpportunityId']
    sql = Conection('DWH')
    data = sql.buscarActasPendientes(fields)
    data['OpportunityId'] = data['OpportunityId'].apply(int).apply(str)
    data, opportunityId, Codigo = buscar_pendientes_locales(data)
    names, dates, blob_names = preparar_paths(data)
    #---subir al DL
    for name, date, blob_name, oppId, expId  in zip(names, dates, blob_names, opportunityId, Codigo):
        year, month = date[0], date[1].zfill(2)
        print(BLOB_FOLDER," | " ,LOCAL_FOLDER ," | " , name," | " , year," | " , month)
        start = time.time()
        try:
            upload_file_to_dl(BLOB_FOLDER, LOCAL_FOLDER , name, year, month)
            nombre_acta = re.sub("\.\w+", "", name)
            res = update_actas_subidas_dl(
                [blob_name, nombre_acta],
                ["UrlActaDL", "NombreArchivoActa"],
                "dbo.Licitacion",
                [str(expId), str(oppId)],
            )
            print(res)
        except Exception as e:
            print(e)
        print(time.time() - start)

def descargas_pendientes():
    """Función para buscar y descargar las actas que no fueron
    descargadas en su momento
    """
    actas = {
            "actas_filtradas": [],
            "actas_nuevas_licitaciones_publicas": [],
            "total_actas_a_descargar": [],
            "actas_no_subidas_al_dl": [],
            "actas_no_descargadas": [],
            "actas_no_procesadas_literata": [],
            "actas_procesadas_literata": [],
            "actas_descargadas": [],
            "actas_subidas_al_dl": [],
        }
    expedientes = ExpedientesPublicados()
    expedientes.reporte_actas = actas
    today = dt.today() - timedelta(days=0, hours=6, minutes=0)
    today = today.replace(hour=0, minute=0, second=0, microsecond=0)
    current_year = str(today.year)
    month = str(today.month)    
    fields = ['Codigo', 'OpportunityId']
    sql = Conection('DWH')
    data = sql.buscarDescargasPendientes(fields)
    data['OpportunityId'] = data['OpportunityId'].apply(int).apply(str)
    downloaded = DownloadExpedientes(expedientes)
    downloaded.tmp_data = r"../data/tmp/data"
    downloaded.year = current_year
    downloaded.month = month
    print(data.columns.tolist())
    for i, row in data.iterrows():
        id_opt = row[1]
        id_exp = row[0]
        print("OpportunityId: ", id_opt)
        print("Condigo: ", id_exp)
        try:
            downloaded.get_file(
                                    id_opt,
                                    id_exp,
                                    ActaPublicada="",
                                    UrlActaDL="",
                                    NombreArchivoActa="",
                                    db=""
                                )
        except Exception as e:
            print("Error : ", e)


def main():
    start = time.time()
    print("Resubiendo datos al DL: ")
    fields = ['Codigo', 'OpportunityId']
    sql = Conection('DWH')
    data = sql.buscarActasPendientes(fields)
    pendiente_descargadas = sql.buscarDescargasPendientes(fields)
    len_data = len(data) + len(pendiente_descargadas)
    print("Actas por subir al data lake: ", len(data))
    print("Actas por visitar: ", len(pendiente_descargadas))
    while len_data > 0:
        start = time.time()
        if len(data) > 0:
            buscar_actas_pendientes()
        print("Timepo para actas a data lake: ", time.time() - start)
        if len(pendiente_descargadas) > 0:
            descargas_pendientes()
        print("Timepo por actas pendientes: ", time.time() - start)
        sql = Conection('DWH')
        data = sql.buscarActasPendientes(fields)
        pendiente_descargadas = sql.buscarDescargasPendientes(fields)
        len_data = len(data) + len(pendiente_descargadas)   


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt as e:
        print("Interrupted")
        fileName = "pendientes.py"
        result_log = "KeyboardInterrupt error {}".format(e)
        write_logfile(fileName, result_log)
        try:
            sys.exit(0)
        except SystemExit as e:
            fileName = "pendientes.py"
            result_log = "SystemExit error: {}".format(e)
            write_logfile(fileName, result_log)
            os._exit(0)