import sys
sys.path.append("../")
from DBServer import *

# db = Conection("TEST")
# db.test_conection()
# print(db.Msg)

fecha_mod = '2022-02-28 00:00:00'
query_monitoreo = """
SELECT *
FROM [DWH_ANALYTICS].[dbo].[Licitacion]
--where ActaPublicada=1 and UrlActaDL in ('')
--Count(Codigo)
where UrlActaDL in ('') and FechaModificacion >= '{0}'
""".format(fecha_mod)
sql = Conection('DWH')
df_m = sql.getQuery(query_monitoreo)
df_m.to_excel('../data/tmp/monitoreo_1.xlsx')
print(df_m)