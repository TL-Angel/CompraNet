# -*- coding: utf-8 -*-
"""
Description: 
"""
__author__ = "Téllez López Angel"
__copyright__ = "@ROBINA, May 2021"
__credits__ = ["Téllez López Angel"]
__license__ = "GPL"
__version__ = "1.0"
__email__ = "atellezl@outlook.com"
__status__ = "Development"

import sys
sys.path.append("../")
import re
import json
from datetime import datetime as dt
import pandas as pd
from difflib import SequenceMatcher as SM
import pymssql

#############################################
# CONFIG
#############################################
TIPOS_DWH = [
    '%s',
    '%s',
    '%s',
    '%s',
    '%s',
    '%s',
    '%s',
    '%s',
    '%s',
    '%s',
    '%s',
    '%s',
    '%s',
    '%s',
    '%s',
    '%s',#"{%Y-%m-%d %H:%M:%S}"
    '%s',#"{%Y-%m-%d %H:%M:%S}",
    '%s',
    '%s',#"{%Y-%m-%d %H:%M:%S}",
    '%s',#"{%Y-%m-%d %H:%M:%S}",
    '%s',
    '%d',
    '%d',
    '%s',
    '%s',
    '%s',#"{%Y-%m-%d %H:%M:%S}",
    '%s'#"{%Y-%m-%d %H:%M:%S}",
]
#####################################

class Conection:   
    def __init__(self, cfg_name='DEFAULT'):   
        #Cargamos el archivo de configuración
        with open('../auth/config.json', 'r') as file:
            config = json.load(file)
            file.close()  
        
        # Asignamos valores a las variables de conexion a Base de datos
        self.cnn_name='DB_CONFIG_'+ cfg_name
        if config.get(self.cnn_name,'')=='':
            self.cnn_name='DB_CONFIG_DEFAULT'

        self.cnn = None
        self.Driver = config[self.cnn_name]['DB_DRIVER']
        self.Server = config[self.cnn_name]['DB_SERVER']
        self.Database = config[self.cnn_name]['DB_NAME']        
        self.UID = config[self.cnn_name]['DB_USER']
        self.PWD = config[self.cnn_name]['DB_PASSWORD']
        self.SecurityIntegrated = config[self.cnn_name]['DB_SECURITY_INTEGRATED']    
        self.Db_Port = 1433    
        
        # Variables temporales para consulta a BD
        self.Shema = config[self.cnn_name]['DB_SHEMA']


        # Variables temporales de almacenamoento de datos
        self.Headers = []
        self.Data = []
        self.DFData=None

        # Variables para exportar datos a excel
        self.HeadersToExcel = []
        self.DataRange = ""
        self.PathFileExcel = "" 
        self.SheetName = ""
        self.TableName = ""
        self.workbook = None
        # Variables que contienen información de los scripts a ejecutar
        self.PathScriptFile = ""
        self.Script =''
        self.Rows = 0
        self.Msg = ""
        
    def __connect(self):        
        self.Msg = ""
        if (self.Server == ""): self.Msg = self.Msg + "- Nombre del servidor.\n"
        if (self.Database == ""): self.Msg = self.Msg + "- Nombre de la base de datos.\n"
        if (self.SecurityIntegrated == ""): self.Msg = self.Msg + "- SecurityIntegrated.\n"
        if (self.SecurityIntegrated == "" or self.SecurityIntegrated.lower() == "no"):
            if (self.UID == ""): self.Msg = self.Msg + "- Usuario.\n" 
            if (self.PWD == ""): self.Msg = self.Msg + "- Password.\n"   
        
        if (self.Msg == ""):  
            if self.cnn_name=='DB_CONFIG_DWH' or self.cnn_name=='DB_CONFIG_TEST' or self.cnn_name=='DB_CONFIG_ESTADO':
                try:
                    if self.SecurityIntegrated.lower() == "no":                        
                        self.cnn = pymssql.connect(self.Server , self.UID , self.PWD ,self.Database) #, port=self.Db_Port)  
                                             
                    else:
                        self.cnn = pymssql.connect(self.Server , self.UID , self.PWD ,self.Database)#, port=self.Db_Port )   
                                              
                    
                except pymssql.Error as e:                
                    #self.Msg="Err Number : {0}, Err description: {1}".format(e.args[0],e.args[1])
                    self.Msg=" Err description: {0}".format(e)
                    self.cnn = None
                    return False  

                else:                    
                    return True

            
            if self.cnn_name=='DB_CONFIG_DEFAULT':              
                self.Msg="Err Number :404, Description: The server could not find the requested content."
                self.cnn = None
                return False  
                
                
        else:            
            self.Msg="No se han especificado los siguientes parametros:\n {0}",format(self.Msg)
            self.cnn=None
            return False



    def __disconect(self):
        if self.cnn!=None:
            self.cnn.close()   


    #def __closeFileExcel(self):
    #    if (self.workbook != None):
    #        self.workbook.close()
    #        self.workbook = None


    def test_conection(self):
        if self.__connect():
            self.Msg='Successful connection.'        
            self.__disconect()
            print('Test OK')
        else:
            print('Test NO')


    def getData(self, AñoMes=202101):
        self.Msg = ""
        self.Rows = 0  
        self.ResultJSON='' 
        self.Script='select top 3000 * from DWH_Analytics.dbo.CNet_Opportunities with(nolock) WHERE añomes={0} and FechaUltimaConsulta is null '.format(AñoMes)   
        #self.Script='Select top 3000 * from DWH_Analytics.dbo.CNet_Opportunities with(nolock) WHERE añomes={0} and FechaUltimaConsulta is null'.format(AñoMes)            
        if (self.Script != ''):
            
            if self.__connect():                
                try:                    
                    cursor = self.cnn.cursor()
                    cursor.execute(self.Script)
                    self.Headers = [column[0] for column in cursor.description]
                    self.Data = cursor.fetchall()
                    self.Rows = len(self.Data)                    
                    self.DFData=pd.DataFrame.from_records(self.Data, columns=self.Headers)    
                except pymssql.DatabaseError as e:
                    self.Msg="Err Description : {0}".format(e.args[1])
                    self.__disconect() 
                    print(self.Msg)
                    return False          
                else:
                    cursor.close()  
                finally:
                    self.__disconect()     
                    return True
            else:
                print(self.Msg)
                print(self.cnn)
                return False

        else:
            self.Msg = "No se ha indicado el script a ejecutar."
            return False

                
    def insertFile(self,oppID,actaID, NameFile, ext, url):
        if self.__connect():                
            try:        
                cursor = self.cnn.cursor()  
                cursor.execute("INSERT DWH_Analytics.dbo.CNet_ActasJuntaAclaraciones ([opportunityID],[actaID],[nombre_Archivo],[extension],[url_Descarga]) OUTPUT INSERTED.opportunityID VALUES ({0},{1}, '{2}','{3}','{4}')".format(oppID,actaID,NameFile,ext,url))  
                #row = cursor.fetchone()  
                #print('OpportunityId: '.format(row[0]))            
                #while row:  
                #    print ("Inserted Product ID : ") +str(row[0])  
                #    row = cursor.fetchone()  
                self.cnn.commit()
                    
            except pymssql.DatabaseError as e:
                self.Msg="Err Description : {0}".format(e.args[1])
                self.__disconect() 
                return False          
            else:
                cursor.close()  
            finally:
                self.__disconect()     
                return True
        else:
            print(self.Msg)
            print(self.cnn)
            return False 
    

    def update_OppID(self,oppID,ActaDescargada=0,ResultadoConsulta=''):
        if self.__connect():                
            try:        
                cursor = self.cnn.cursor()                  
                cursor.execute("Update DWH_Analytics.dbo.CNet_Opportunities set [FechaUltimaConsulta]=getdate(),[ActaDescargada]={0}, [resultadoConsultaDesc]='{1}' where opportunityId={2}  ".format(ActaDescargada,ResultadoConsulta,oppID))   
                self.cnn.commit()            
                    
            except pymssql.DatabaseError as e:
                self.Msg="Err Description : {0}".format(e.args[1])
                self.__disconect() 
                return False          
            else:
                cursor.close()  
            finally:
                self.__disconect()     
                return True
        else:
            print(self.Msg)
            print(self.cnn)
            return False 

    def test_(self):
        if self.__connect():  
            try:
                cursor = self.cnn.cursor() 
                cursor.executemany(
                    "INSERT INTO testing_pymssql VALUES (%d, %s, %d)",
                    [(1, 'lei', 1 ),
                    (2, 'yi', 1),
                    (3, 'TOM', 0)])
                self.cnn.commit()
                
                print('ok, se dieron de alta')
                return True
            except pymssql.DatabaseError as e:
                self.Msg="Err Description : {0}".format(e.args[1])
                self.__disconect() 
                return False          
            else:
                cursor.close()  
            finally:
                self.__disconect()     
                return True
        else:
            print(self.Msg)
            print(self.cnn)
            return False 

    def GetIdEstados(self):
        query_id_estados = """
        SELECT 
        CASE WHEN es.Estado = 'ESTADO DE MEXICO' THEN 'MEXICO' ELSE es.Estado END as Estado ,
        es.IdEstado as IdEstado
        FROM [DWH_UNIFIN].[dwhuf].[DimEstado] es
        where Estado IS NOT NULL;
        """
        self.__connect()
        print(self.Msg)
        cursor = self.cnn.cursor()
        cursor.execute(query_id_estados)
        results = pd.DataFrame(cursor.fetchall() , columns=['Estado', 'IdEstado'])
        self.__disconect()  
        return results

    def InsertData(self, DF_Data, TableName, FieldList=[]):
        """Método para insertar datos a la base de datos usando pymssql.
        """
        if len(FieldList) >= 1:
            cols = ",".join([str(i) for i in FieldList])
            #params = ",".join(['?' for i in FieldList])
            params=",".join(TIPOS_DWH)
            values= [tuple(x) for x in DF_Data.values.tolist()]
        else:
            cols = ",".join([str(i) for i in DF_Data.columns.tolist()])
            #params = ",".join(['?' for i in DF_Data.columns.tolist()])
            params=",".join(TIPOS_DWH)
            values= [tuple(x) for x in DF_Data.values.tolist()]
            
        scriptText = "INSERT INTO dbo." + TableName + \
            " (" + cols + ") VALUES (" + params + ")"
        self.__connect()
        cursor = self.cnn.cursor()
        cursor.executemany(scriptText, values)
        self.cnn.commit()
        print(self.Msg)
        self.__disconect()  

    def searchData(self, ls_codigo, ls_opid, fields =['Codigo', 'OpportunityId']):
        str_fields = ",".join([str(x) for x in fields])
        str_codigo = ",".join([str(x) for x in ls_codigo])
        str_opid = ",".join([str(x) for x in ls_opid])
        query_licitacion = """SELECT {2}
                            FROM DWH_ANALYTICS.dbo.Licitacion 
                            WHERE Codigo IN ({0})
                            AND OpportunityId IN ({1}); """.format(str_codigo, str_opid, str_fields)
        self.__connect()
        cursor = self.cnn.cursor()
        cursor.execute(query_licitacion)
        results = pd.DataFrame(cursor.fetchall() , columns=fields)
        self.__disconect()  
        return results
    
    def searchDbByDate(self, fecha, fields =['Codigo', 'OpportunityId']):
        str_fields = ",".join([str(x) for x in fields])
        query_licitacion = """SELECT {0}
                            FROM DWH_ANALYTICS.dbo.Licitacion 
                            WHERE FechaModificacion >= '{1}';""".format(str_fields,  str(fecha))
        self.__connect()
        cursor = self.cnn.cursor()
        cursor.execute(query_licitacion)
        results = pd.DataFrame(cursor.fetchall() , columns=fields)
        self.__disconect()  
        return results
    def updateValue(self, values, columns, table, condicion):
        """Método para actualizar los valores de la base de datos de SQL
        """
        fecha_mod = dt.today().replace(microsecond=0)
        if columns == 'ActaPublicada':
            query_update = """UPDATE {0} 
                            SET {2} = CAST('{1}' as INT) , FechaModificacionReg = CAST('{4}' as DATETIME)    
                            WHERE {3};""".format(table, str(values), str(columns), condicion , str(fecha_mod))
        if type(columns) == type([1]):
            str_data = ",".join([str(x)+"='"+str(y)+"'" for x, y in zip(columns, values)])
            query_update = """UPDATE {0} 
                            SET {1} , FechaModificacionReg =  CAST('{3}' as DATETIME)
                            WHERE {2};""".format(table, str_data, condicion, str(fecha_mod))
        self.__connect()
        cursor = self.cnn.cursor()
        print(query_update)
        cursor.execute(query_update)
        self.cnn.commit()
        msg = self.Msg
        self.__disconect()  
        return msg
    def buscarActasPendientes(self, fields:list):
        str_fields = ",".join([str(x) for x in fields])
        query_licitacion = """SELECT {0}
                              FROM [DWH_ANALYTICS].[dbo].[Licitacion]
                              WHERE ActaPublicada=1 and UrlActaDL in ('');""".format(str_fields)
        self.__connect()
        cursor = self.cnn.cursor()
        cursor.execute(query_licitacion)
        results = pd.DataFrame(cursor.fetchall() , columns=fields)
        self.__disconect()  
        return results