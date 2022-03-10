import sys
sys.path.append("../")
import os
import time
from scraper.aux_compranet import *
from scraper.DBServer import *
##############################
# CONFIG
##############################
BUCKET = "uniclick-dl-robina-compranet"
folder_bucket = "Actas_Junta_Aclaraciones/"
LOCAL_FOLDER = "../data/tmp/data"
BLOB_FOLDER = "Acta_Junta_Aclaraciones"
##############################


"""Método para buscar en la DB las actas pendientes
    """
start = time.time()
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
d_files = []
for name, date, blob_name, oppId, expId  in zip(names, dates, blob_names, opportunityId, Codigo):
    year, month = date[0], date[1].zfill(2)
    #print(BLOB_FOLDER," | " ,LOCAL_FOLDER ," | " , name," | " , year," | " , month)
    try:
        d_files.append([name, get_file_size_in_megabytes(LOCAL_FOLDER+"/"+name)])
    except Exception as e:
        print(e)
for x in d_files:
    write_txt('../data/tmp/size_cola_actas_proposiciones.csv', str(x[0])+" , "+str(x[1])+"\n")
print(time.time() - start)