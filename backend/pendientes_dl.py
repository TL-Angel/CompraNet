import sys
sys.path.append("../")
from scraper.DBServer import *
from scraper.aux import *
import pandas as pd
##############################
# CONFIG
##############################


def buscar_actas_pendientes():
    """Método para buscar en la DB las actas pendientes
    """
    fields = ['Codigo', 'OpportunityId']
    sql = Conection('DWH')
    data = sql.buscarActasPendientes(fields)
    data['OpportunityId'] = data['OpportunityId'].apply(int).apply(str)
    #print(data)
    data = buscar_pendientes_locales(data)
    print(data)


if __name__ == "__main__":
    try: 
        # print(sys.argv)
        # n_days = int(sys.argv[1])
        buscar_actas_pendientes()
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