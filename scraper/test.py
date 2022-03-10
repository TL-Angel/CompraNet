import sys
import os
sys.path.append("../")
from DBServer import *

MAX_INTENT = 3

# db = Conection("TEST")
# db.test_conection()
# print(db.Msg)

# fecha_mod = '2022-02-28 00:00:00'
# query_monitoreo = """
# SELECT *
# FROM [DWH_ANALYTICS].[dbo].[Licitacion]
# --where ActaPublicada=1 and UrlActaDL in ('')
# --Count(Codigo)
# where UrlActaDL in ('') and FechaModificacion >= '{0}'
# """.format(fecha_mod)
# sql = Conection('DWH')
# df_m = sql.getQuery(query_monitoreo)
# df_m.to_excel('../data/tmp/monitoreo_1.xlsx')
# print(df_m)

# ##---------------------------
# df_new_licis = pd.read_excel("../data/tmp/lici_news_2022-03-08 21-32-02.xlsx", index_col=0)
sql = Conection('DWH')
# cod = [ str(x) for x in  df_new_licis.Codigo.tolist()]
# cod = ",".join(cod)
# op = [str(x) for x in  df_new_licis.OpportunityId.tolist()]
# op = ",".join(op)

query = """
SELECT  *--COUNT(Codigo)
FROM [DWH_ANALYTICS].[dbo].[Licitacion]
--where ActaPublicada=1 and UrlActaDL in ('')
--where ActaPublicada=0 and UrlActaDL != ''
--WHERE Codigo = '2390735'
--where UrlActaDL in ('') and FechaModificacion >= '2022-03-07 00:00:00'
--where ActaPublicada=1 and FechaModificacion >= '2022-02-27 00:00:00' and  UrlActaDL = ''
--where  FechaModificacion >= '2022-03-08 00:00:00' and  UrlActaDL != ''
--where  FechaModificacion >= '2022-03-07 00:00:00'  and  UrlActaDL != ''  
where FechaModificacion >= '2022-03-08 00:00:00'
--where FechaModificacion IS Not NULL
--WHERE NombreArchivoActa = 'proposiciones_2091573_2400022_20223_1'
--WHERE Codigo IN ('2388142')
"""
df_res_sql = sql.getQuery(query)
df_res_sql.to_excel('../data/tmp/res_sql_test_no_downloaded.xlsx')
# #------------------------------
# def funcion(n_days):
#     n_days = n_days/2
#     print("Los días son: ", n_days)

# if __name__ == "__main__":
#     contador = 0
#     while contador < MAX_INTENT:
#         try: 
#             print(sys.argv)
#             n_days = int(sys.argv[1])
#             funcion(n_days)
#             contador = 4
#             break
#         except Exception as e:
#             contador += 1
#             print("Intento fallido: ", contador)
#             print(e)
#         except KeyboardInterrupt as e:
#         print("Interrupted")
#         fileName = "app.py"
#         result_log = "KeyboardInterrupt error {}".format(e)
#         write_logfile(fileName, result_log)
#         try:
#             sys.exit(0)
#         except SystemExit as e:
#             fileName = "app.py"
#             result_log = "SystemExit error: {}".format(e)
#             write_logfile(fileName, result_log)
#             os._exit(0)