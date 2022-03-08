# -*- coding: utf-8 -*-
"""
Description: 
"""
__author__ = "Téllez López Angel"
__copyright__ = "@ROBINA Feb 2021"
__credits__ = ["LeftHand", "RightHnad"]
__license__ = "GPL"
__version__ = "1.0"
__email__ = "atellezl@outlook.com"
__status__ = "Development"

import sys
import os
import json
import base64
import pyodbc
import pymssql
import xlsxwriter
import pandas as pd

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
    '%d',
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
    '%r',
    '%s',
    '%s',
    '%s',#"{%Y-%m-%d %H:%M:%S}",
    '%s'#"{%Y-%m-%d %H:%M:%S}",
]
#############################################


class SQLConection:
    def __init__(self):
        # Cargamos el archivo de configuración
        with open('../auth/config2.json', 'r') as file:
            config = json.load(file)
            file.close()
        # Asignamos valores a las variables de conexion a Base de datos
        self.Driver = config['DB_CONFIG']['DB_DRIVER']
        self.Server = config['DB_CONFIG']['DB_SERVER']
        self.Database = config['DB_CONFIG']['DB_NAME']
        self.UID = config['DB_CONFIG']['DB_USER']
        self.PWD = config['DB_CONFIG']['DB_PASSWORD']
        self.Port = config['DB_CONFIG']['DB_PORT']
        self.SecurityIntegrated = config['DB_CONFIG']['DB_SECURITY_INTEGRATED']
        self.cnn = None
        # Asignamos valores a las variables de conexion a Base de datos de Dim Estados
        self.Driver_edo = config['DB_ESTADOCONFIG']['DB_DRIVER']
        self.Server_edo = config['DB_ESTADOCONFIG']['DB_SERVER']
        self.Database_edo = config['DB_ESTADOCONFIG']['DB_NAME']
        self.UID_edo = config['DB_ESTADOCONFIG']['DB_USER']
        self.PWD_edo = config['DB_ESTADOCONFIG']['DB_PASSWORD']
        self.Port_edo = config['DB_ESTADOCONFIG']['DB_PORT']
        self.SecurityIntegrated_edo = config['DB_ESTADOCONFIG']['DB_SECURITY_INTEGRATED']
        self.conn_mssql = None
        # Variables temporales de almacenamoento de datos
        self.Headers = []
        self.Data = []
        # Variables para exportar datos a excel
        self.HeadersToExcel = []
        self.DataRange = ""
        self.PathFileExcel = ""
        self.SheetName = ""
        self.TableName = ""
        self.workbook = None
        # Variables que contienen información de los scripts a ejecutar
        self.PathScriptFile = ""
        self.Script = ""
        self.Rows = 0
        self.Msg = ""

    def __connect(self):
        self.Msg = ""
        if (self.Driver == ""):
            self.Msg = self.Msg + "- Driver.\n"
        if (self.Server == ""):
            self.Msg = self.Msg + "- Nombre del servidor.\n"
        if (self.Database == ""):
            self.Msg = self.Msg + "- Nombre de la base de datos.\n"
        if (self.SecurityIntegrated == ""):
            self.Msg = self.Msg + "- SecurityIntegrated.\n"
        if (self.SecurityIntegrated == "" or self.SecurityIntegrated.lower() == "no"):
            if (self.UID == ""):
                self.Msg = self.Msg + "- Usuario.\n"
            if (self.PWD == ""):
                self.Msg = self.Msg + "- Password.\n"
        if (self.Msg == ""):
            try:

                if self.SecurityIntegrated.lower() == "no":
                    self.cnn = pyodbc.connect('Driver=' + self.Driver + ';Server=' + self.Server + ';Database=' +
                                              self.Database + ';UID=' + self.UID + ';PWD=' + self.PWD + ';APP=ROBINA;')
                else:
                    self.cnn = pyodbc.connect('Driver=' + self.Driver + ';Server=' + self.Server + ';Database=' +
                                              self.Database + ';Trusted_Connection = ' + self.SecurityIntegrated + ';')
                print(self.Msg)

            except pyodbc.Error as e:
                self.Msg = "Err Number : {0} \nErr description: {1}".format(
                    e.args[0], e.args[1])
                print(self.Msg)
                self.cnn = None
                return False

            else:
                return True
        else:
            self.Msg = "No se han especificado los siguientes parametros:\n {0}", format(
                self.Msg)
            print(self.Msg)
            return False

    def __connect_mssql(self, Data_Base: str):
        if Data_Base == 'DWH_ANALYTICS':
            self.conn_mssql = pymssql.connect(
                host=self.Server,
                ##port=self.Port,
                user=self.UID,
                password=self.PWD,
                database=self.Database,
            )
        if Data_Base == 'DWH_UNIFIN':
            self.conn_mssql = pymssql.connect(
                host=self.Server_edo,
                port=self.Port_edo,
                user=self.UID_edo,
                password=self.PWD_edo,
                database=self.Database_edo,
            )

    def __disconect(self):
        if self.cnn != None:
            self.cnn.close()

    def __closeFileExcel(self):
        if (self.workbook != None):
            self.workbook.close()
            self.workbook = None

    def test_conection(self):
        if self.__connect():
            self.Msg = 'Conexión exitosa.'
            self.__disconect()

    def SelectFromFile(self, PathScriptFile):
        self.Msg = ""
        self.Rows = 0
        self.PathScriptFile = PathScriptFile
        # Verificamos si el archivo sql existe y no esta vacio.
        if os.path.exists(self.PathScriptFile) and os.path.getsize(self.PathScriptFile) > 0:
            # Cargamos el archivo sql
            file = open(self.PathScriptFile, "r")
            self.Script = file.read()
            file.close()

            if (self.Script != ''):
                if self.__connect():
                    try:
                        cursor = self.cnn.cursor()
                        cursor.execute(self.Script)
                        self.Headers = [column[0]
                                        for column in cursor.description]
                        self.Data = cursor.fetchall()
                        self.Rows = len(self.Data)

                    except pyodbc.DatabaseError as e:
                        self.Msg = "Err Description : {0}".format(e.args[1])
                        self.__disconect()
                        return False
                    else:
                        cursor.close()
                    finally:
                        self.__disconect()
                        return True
                else:
                    return False

            else:
                self.Msg = "El archivo no contiene un script SQL o esta vacio."
                return False
        else:
            self.Msg = "El archivo no existe o no es accesible."
            return False

    def SelectFromText(self, Script):
        self.Msg = ""
        self.Rows = 0
        if (self.Script != ''):
            if self.__connect():
                try:
                    cursor = self.cnn.cursor()
                    cursor.execute(self.Script)
                    self.Headers = [column[0] for column in cursor.description]
                    self.Data = cursor.fetchall()
                    self.Rows = len(self.Data)

                except pyodbc.DatabaseError as e:
                    self.Msg = "Err Description : {0}".format(e.args[1])
                    self.__disconect()
                    return False
                else:
                    cursor.close()
                finally:
                    self.__disconect()
                    return True
        else:
            self.Msg = "No se ha indicado el script a ejecutar."
            return False

    def InsertFromDataFrame(self, DF_Data, TableName="", FieldList=[]):
        if (len(DF_Data) >= 1 and TableName != ''):
            if self.__connect():
                try:
                    if len(FieldList) >= 1:
                        cols = ",".join([str(i) for i in FieldList])
                        params = ",".join(['?' for i in FieldList])
                    else:
                        cols = ",".join([str(i)
                                        for i in DF_Data.columns.tolist()])
                        params = ",".join(
                            ['?' for i in DF_Data.columns.tolist()])

                    scriptText = "INSERT INTO dbo." + TableName + \
                        " (" + cols + ") VALUES (" + params + ")"
                    cursor = self.cnn.cursor()
                    cursor.executemany(scriptText, DF_Data.values.tolist())
                    print(self.Msg)

                except pyodbc.DatabaseError as e:
                    self.Msg = "Err Number : {0} \nErr description:{1}".format(
                        e.args[0], e.args[1])
                    print(self.Msg)
                else:
                    self.cnn.commit()
                    cursor.close()
                    self.Msg = "Se insertaron los registros correctamente en la tabla:{0}".format(
                        TableName)
                    print(self.Msg)
                finally:
                    self.__disconect()
        else:
            self.Msg = "Datos insuficientes para agregar registros [Registros en DF:{0}, Nombre de la tabla:{1}].".format(
                len(DF_Data), TableName)

    def ExportToExcel(self, PathFileExcel, SheetName="Data", TableName="TData", CloseFileExcel=True):
        self.Msg = ""
        if (self.Rows > 0):
            self.PathFileExcel = PathFileExcel
            self.SheetName = SheetName
            self.TableName = TableName

            # Generamos los encabezados
            self.HeadersToExcel = []
            for item in self.Headers:
                self.HeadersToExcel.append({'header': item})

            # Creamos el nuevo libro y agregamos la hoja donde se escribiran los datos
            if (self.workbook == None):
                self.workbook = xlsxwriter.Workbook(self.PathFileExcel)

            ws = self.workbook.add_worksheet(self.SheetName)
            self.DataRange = xlsxwriter.utility.xl_range(
                0, 0, self.Rows, len(self.Headers)-1)

            # Escribimos los datos en el archivo de Excel
            ws.add_table(self.DataRange, {
                         'name': self.TableName, 'data': self.Data, 'columns': self.HeadersToExcel})

            if CloseFileExcel:
                self.workbook.close()
                self.workbook = None

            if (self.Rows > 1048576):
                self.Msg = "Los datos devueltos exceden el limite de filas permitido en Excel (1,048,576 filas).\n No se guardaron todos los registros."
            return True
    # def UpdateField(self, DF_Data,TableName="",FieldList=[]):
    #     script = "UPDATE dbo."+ TableName+ "SET telefono='662142223' , email='albesanch@mimail.com' WHERE nombre='Alberto Sanchez'"

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
        print(len(values[0]))
        print(scriptText)
        print('\n', values)
        self.__connect_mssql('DWH_ANALYTICS')
        cursor = self.conn_mssql.cursor()
        cursor.executemany(scriptText, values)

    def test_(self):
        try:
            
            if self.__connect_mssql('DWH_ANALYTICS'):
                
                cursor = self.conn_mssql.cursor()

                cursor.executemany(

                    "INSERT INTO testing_pymssql VALUES (%d, %s, %d)",

                    [(1, 'lei', 1 ),

                    (2, 'yi', 1),

                    (3, 'TOM', 0)])
                print('ok')
                return True
            else:
                print('sin conexion')

        except pymssql.Error as err:

            print(err)

            return False

    def GetIdEstados(self):
        query_id_estados = """
        SELECT 
        es.Estado as Estado,
        es.IdEstado as IdEstado
        FROM [DWH_UNIFIN].[dwhuf].[DimEstado] es
        where Estado IS NOT NULL;
        """
        self.__connect_mssql('DWH_UNIFIN')
        cursor = self.conn_mssql.cursor()
        cursor.execute(query_id_estados)
        results = cursor.fetchall()
        return pd.DataFrame(results, columns=['Estado', 'IdEstado'])
