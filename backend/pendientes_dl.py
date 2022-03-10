import sys
sys.path.append("../")
from scraper.DBServer import *
from scraper.aux_compranet import *
import pandas as pd
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
    #print(data)
    data, opportunityId, Codigo = buscar_pendientes_locales(data)
    #print(data)
    names, dates, blob_names = preparar_paths(data)
    #print("\nnames:\n", blob_names)
    #---subir al DL
    for name, date, blob_name, oppId, expId  in zip(names, dates, blob_names, opportunityId, Codigo):
        year, month = date[0], date[1].zfill(2)
        print(BLOB_FOLDER," | " ,LOCAL_FOLDER ," | " , name," | " , year," | " , month)
        start = time.time()
        try:
            upload_file_to_dl(BLOB_FOLDER, LOCAL_FOLDER , name, year, month)
            # Actualizar UrlActaDL, NombreArchivoActa si el acta fue subida al DL
            # blob_name = (
            #     f"{self.blob_name}/{ self.year}/{self.month}/{file_name}"
            # )
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
            #Aqui imprimir un log del backend
        print(time.time() - start)


def main():
    start = time.time()
    print("Resubiendo datos al DL: ")
    fields = ['Codigo', 'OpportunityId']
    sql = Conection('DWH')
    data = sql.buscarActasPendientes(fields)
    len_data = len(data)
    print(len_data)
    while len_data > 0:
        start = time.time()
        buscar_actas_pendientes()
        print("Timepo por ciclo: ", time.time() - start)
        sql = Conection('DWH')
        data = sql.buscarActasPendientes(fields)
        len_data = len(data)    


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt as e:
        print("Interrupted")
        fileName = "pendientes_dl.py"
        result_log = "KeyboardInterrupt error {}".format(e)
        write_logfile(fileName, result_log)
        try:
            sys.exit(0)
        except SystemExit as e:
            fileName = "app.py"
            result_log = "SystemExit error: {}".format(e)
            write_logfile(fileName, result_log)
            os._exit(0)