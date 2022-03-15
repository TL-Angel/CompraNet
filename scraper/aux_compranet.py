import sys
sys.path.append("../")
from connections.DBServer import *
from src.utils import *
from connections.datalakeconn import *
import uuid
from pathlib import Path
import os
from bs4 import BeautifulSoup
import requests
from fake_useragent import UserAgent
from google.cloud import storage
from pymongo import ReplaceOne
from copy import deepcopy
import pandas as pd
import pymssql
from datetime import timedelta, datetime as dt
from glob import glob

########################################
# CONFIG
BUCKET = "uniclick-dl-robina-compranet"
folder_bucket = "Actas_Junta_Aclaraciones/"
LOCAL_FOLDER = "../data/tmp/data/"
BLOB_FOLDER = "Acta_Junta_Aclaraciones"
TIMEOUT = 90
CHUNKSIZE = 2097152 # 1024 * 1024 B * 2 = 2 MB
########################################


def set_google_key(key_location: str):
    """Indica en donde se encuentran la key para acceder al DataLake"""
    import os

    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = key_location


def upload_file_to_dl(blobname, folder, file_name, year, month):
    """Sube un archivo de CompreNet al DataLake."""
    blob_name = f"{blobname}/{year}/{month}/{file_name}"
    full_name = r"{0}/{1}".format(folder, file_name)
    BUCKET = "uniclick-dl-robina-compranet"
    set_google_key("../auth/uniclick-dl-robina-prod-compranet.json")
    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET)
    blob = bucket.blob(blob_name)
    ## For slow upload speed
    storage.blob._DEFAULT_CHUNKSIZE = CHUNKSIZE
    storage.blob._MAX_MULTIPART_SIZE = CHUNKSIZE
    print("subiendo al dl ...")
    blob.upload_from_filename(full_name, content_type="application/pdf", timeout=TIMEOUT)
    response = busqueda_archivo_dl(BUCKET, blob_name)
    return response


def list_blobs():
    """Lists all the blobs in the bucket."""
    BUCKET = "uniclick-dl-robina-compranet"
    DL = Datalake(BUCKET)
    # Note: Client.list_blobs requires at least package version 1.17.0.
    blobs = DL.client.list_blobs(BUCKET)
    lista_objs = []
    for blob in blobs:
        lista_objs.append(blob.name)
    return lista_objs


def busqueda_archivo_dl(BUCKET, FULL_NAME):
    set_google_key("../auth/uniclick-dl-robina-prod-compranet.json")
    storage_client = storage.Client()
    blobs = storage_client.list_blobs(BUCKET, prefix=FULL_NAME)
    blobs = [str(x.name) for x in blobs]
    if len(blobs) > 0:
        print("Subido a DL ", blobs)
        return True
    else:
        print("no subido ", blobs)
        return False


def read_mongo(auth_location):
    """Funcion para leer el json y conectar al datalake

    Args:
        auth_location (string): Path de donde se encuentra el archivo json (token) de conexión

    Returns:
        string: Regresa el token necesario para hacer la conexión entre el server y el data lake
    """
    with open(auth_location) as f:
        uri = f.read()[:-1]
    return uri

def gen_tmp_pdf(pdf_bytes):
    """Genera un PDF temporal ya que por alguna razón literata me pide enviarle
    el archivo con `open(filename, 'rb')`.
    """
    tmpdir = Path("../data/tmp/data")
    tmpdir.mkdir(exist_ok=True, parents=True)
    uuid_ = str(uuid.uuid1().int)
    filename = f"tmp_pdf_{uuid_}.pdf"
    path = tmpdir / filename
    with path.open(mode="wb") as f:
        f.write(pdf_bytes)
    return str(path.absolute())


def literata_transcript(pdf_):
    """Realiza el post a Literata para llevar a cabo la transcripción del PDF."""
    upload_url = "http://192.168.150.158:11733/upload"
    if isinstance(pdf_, bytes):
        pdf_path = gen_tmp_pdf(pdf_)
    else:
        pdf_path = pdf_
    files = {"file[]": open(pdf_path, "rb")}
    for i in range(4):
        try:
            resp_post = requests.post(upload_url, files=files)
            txt_file = BeautifulSoup(resp_post.content, "html.parser").find("a").text
            url_txt = f"http://192.168.150.158:11733/uploads/{txt_file}"
            break
        except:
            url_txt = f"http://192.168.150.158:11733/uploads/{txt_file}"
            print("No procesada a literata")
            return "No procesada a literata"
    return requests.get(url_txt).content.decode("utf-8")


def post_to_autoencoder(pdf_bytes, filename="clean_doc"):
    """Realiza el POST a la API local de limpieza con autoencoder."""
    url = f"http://192.168.150.158:56651/autoencoder_clean?filename={filename}"
    files = {"file": pdf_bytes}
    res = requests.post(url, files=files)
    return res.content


def get_extra_data(oppId, extra_data):
    diccionario_salida = {}
    for key, value in extra_data.items():
        with HTMLSession() as session:
            url_base = "https://compranet.hacienda.gob.mx"
            url_ = "https://compranet.hacienda.gob.mx/esop/guest/go/opportunity/detail?opportunityId={0}".format(
                oppId
            )
            response = smartproxy(url_, session)
            if response.status_code == 200:
                print(
                    ":::  Respuesta correcta ........................................."
                )
                extra_data_obteinied = response.html.xpath(value)
                if extra_data_obteinied != []:
                    contador = 1
                    for data in extra_data_obteinied:
                        if contador == 1:
                            print(
                                ":::  Dato localizado  ......................................."
                            )
                            print("{}: ".format(key), extra_data_obteinied[0].text)
                            diccionario_salida[key] = extra_data_obteinied[0].text
                        else:
                            print(
                                ":::  Dato localizado  ......................................."
                            )
                            print("{}_{}: ".format(key, contador), data.text)
                            diccionario_salida[
                                "{}_{}: ".format(key, contador)
                            ] = data.text
                        contador += 1
    return diccionario_salida


def upload_txt_to_dl(
    txt_bytes, filename, bucket=BUCKET, folder_bucket="Actas_Junta_Aclaraciones/"
):
    """Sube una transcripcion de acta de CompreNet al DataLake."""
    DL = Datalake(bucket)
    blobname = f"{folder_bucket}/{filename}.txt"
    return DL.upload_bytes(txt_bytes, blobname, content_type="text/plain")

def get_licitantes_mongodb(fecha):
    """Función para obtener los nombres de los licitantes obtenidos por literata más la información de su respectiva licitación

    Args:
        fecha (datetime): Es la fecha a partir de la cual se filtran las licitaciones

    Returns:
        dict: Devuelve un diccionario con la información de las licitaciones más los nombres obtenidos
    """
    projections = [
        "Codigo del expediente",
        "opportunityId",
        "Fecha de ultima modificacion",
    ]
    mdb2 = MongoConnection(None, read_mongo("../auth/uri_robina.txt"))
    data_base = "compranet"
    collection = "licitaciones_publicas"
    mdb2.start_connection(data_base=data_base, collection=collection)
    query = {"Fecha de ultima modificacion": {"$gte": fecha}}
    query = mdb2.collection.find(query, {"_id": 0})
    licis = pd.DataFrame(query)
    data_base = "compranet"
    collection = "licitantes"
    mdb2.start_connection(data_base=data_base, collection=collection)
    q = licis[["Codigo del expediente", "opportunityId"]].to_dict("list")
    q["expediente"] = q.pop("Codigo del expediente")
    q["opportunity"] = q.pop("opportunityId")
    q["expediente"] = {"$in": q["expediente"]}
    q["opportunity"] = {"$in": q["opportunity"]}
    projections2 = {
        "entities": 1,
        "expediente": 1,
        "opportunity": 1,
        "_id": 0,
        "credito_externo": 1,
        "fecha_acto_fallo": 1,
        "fecha_junta_aclaracion": 1,
        "procedimiento_exclusivo_pymes": 1,
    }
    query2 = mdb2.collection.find(q, projections2)
    names = pd.DataFrame(query2)
    try:
        salida = names[
            [
                "entities",
                "opportunity",
                "expediente",
                "credito_externo",
                "fecha_acto_fallo",
                "fecha_junta_aclaracion",
                "procedimiento_exclusivo_pymes",
            ]
        ].merge(
            licis,
            how="inner",
            left_on=["opportunity", "expediente"],
            right_on=["opportunityId", "Codigo del expediente"],
        )
        salida = salida.rename(columns={"entities": "nombre_licitantes"})
        salida = salida.drop(columns=["opportunity", "expediente"])
        salida.columns = [x.replace(" ", "_") for x in list(salida.columns)]
        salida["nombre_licitantes"] = salida["nombre_licitantes"].apply(
            lambda x: [k["text"] for k in x]
        )
    except Exception as e:
        print(e)
        salida = pd.DataFrame()
    return salida


# -----
def upload_docs_to_mongo_info_licitantes(input_docs):
    """Sube los documentos Mongo con un uuid formado con el opportunity id y el id del expediente de cada licitación.

    Args:
        input_docs ([list]): Lista de documentos (diccionarios typo JSON) para subir a Mongodb

    Returns:
        [Bolean]: Regresa True o False dependiendo si fue o No exitoso la subida de docs
    """
    collection = "licitantes_contacto"
    data_base = "compranet"
    mdb = MongoConnection(None, read_mongo("../auth/uri_robina.txt"))
    mdb.start_connection(collection=collection, data_base=data_base)
    docs = deepcopy(input_docs)
    try:
        col = mdb.collection
        bulk_updates = []
        for doc in docs:
            filter_ = {"nombre_licitante": doc["nombre_licitante"]}
            update_ = ReplaceOne(filter_, doc, upsert=True)
            bulk_updates.append(update_)
        bulk_result = col.bulk_write(bulk_updates)
        return True
    except Exception as e:
        print(e)
        return False


# -------- PETICIONES AL DWH --------------------------------------
def get_cuenta_info_dwh(nombre_cuenta):
    query_cuenta = """
    SELECT 
    Sinonimo1 as nombre_cuenta,
    u.NombreCompleto AS agente_telefonico_cuenta,
    --u.IdEstatusUsuarioCRM as EstatusCRM,
    p.id_c as idcrm_cuenta,
    p.TipoSubTipoDesc as tipo_cuenta
    --,stp.SubTipoPersona
    ,ag.NombreCompleto AS asesor_gestion_cuenta
    ,pr.Producto as producto
    --,p.IdEstatusAtencion
    ,CASE WHEN pp.idEstatusAtendido=1 THEN 'Atendido' ELSE 'Desatendido' END AS estatus_producto
    FROM [DWH_UNIFIN].[dwhuf].[DimPersona] p
    left join [DWH_UNIFIN].[dwhuf].[DimSubTipoPersona] stp on stp.IdSubTipoPersona=p.IdSubTipoPersona
    left join [DWH_UNIFIN].[dwhuf].[DimPersonaGenerales] pg on p.IdPersona=pg.IdPersona
    left join [DWH_UNIFIN].[dwhuf].[DimUsuario] u on u.IdUsuario=pg.IdUsuarioAgenteTelefonico
    left join [DWH_UNIFIN].[dwhuf].[DimPersonaPorProducto] pp on pp.idPersona=p.IdPersona
    left join [DWH_UNIFIN].[dwhuf].[DimUsuario] ag on pp.idAsesorGestion=ag.IdUsuario
    left join [DWH_UNIFIN].[dwhuf].[DimProducto] pr on pr.IdProducto=pp.idProducto
    where Sinonimo1 = '{}'
    --and idBinarioEliminado = 0
    """.format(
        nombre_cuenta
    )
    conn = pymssql.connect(
        host="192.168.150.27",
        port="1433",
        user="userdwh_portiz",
        password="8oXQ*fleC#p0",
        database="DWH_UNIFIN",
    )
    cursor = conn.cursor()
    cursor.execute(query_cuenta)
    results = cursor.fetchall()
    if len(results) > 0:
        res = pd.DataFrame(
            results,
            columns=[
                "nombre_cuenta",
                "agente_telefonico_cuenta",
                "idcrm_cuenta",
                "tipo_cuenta",
                "asesor_gestion_cuenta",
                "producto",
                "estatus_producto",
            ],
        )
    else:
        res = pd.DataFrame(
            {
                "nombre_cuenta": [""],
                "agente_telefonico_cuenta": [""],
                "idcrm_cuenta": [""],
                "tipo_cuenta": [""],
                "asesor_gestion_cuenta": [""],
                "producto": [""],
                "estatus_producto": [""],
            }
        )
    return res


def get_lead_info_dwh(nombre_lead):
    query_lead = """
    SELECT 
    p.Sinonimo1 AS nombre_lead,
    --[IdLeadCRM] as id_crm_lead,
    --,l.[IdPersona]
    [IdLeadCRM] as id_crm_lead,
    --,[IdRegimenFiscal]
    tp.[TipoPersona] as tipo_lead
    ,stp.[SubTipoPersona] as subtipo_lead
    ,u.NombreCompleto as asesor_gestion_lead
    --,l.IdEstatusManagement
    ,em.EstatusManagement as estatus_management
    --,l.IdBinarioEliminado
    --,p.idBinarioEliminado
    FROM [DWH_UNIFIN].[dwhuf].[DimLead] l
    left join [DWH_UNIFIN].[dwhuf].[DimSubTipoPersonas] stp on stp.IdSubTipoPersona=l.IdSubTipoPersona
    left join [DWH_UNIFIN].[dwhuf].[DimPersona] p on p.IdPersona=l.IdPersona
    left join [DWH_UNIFIN].[dwhuf].[DimTipoPersonas] tp on tp.IdTipoPersona=l.IdTipoPersona
    left join [DWH_UNIFIN].[dwhuf].[DimUsuario] u on u.idUsuario=l.IdUsuario
    left join [DWH_UNIFIN].[dwhuf].[DimEstatusManagement] em on em.IdEstatusManagement=l.IdEstatusManagement
    where p.Sinonimo1 ='{}'
    """.format(
        nombre_lead
    )
    conn = pymssql.connect(
        host="192.168.150.27",
        port="1433",
        user="userdwh_portiz",
        password="8oXQ*fleC#p0",
        database="DWH_UNIFIN",
    )
    cursor = conn.cursor()
    cursor.execute(query_lead)
    results = cursor.fetchall()
    if len(results) > 0:
        res = pd.DataFrame(
            results,
            columns=[
                "nombre_lead",
                "id_crm_lead",
                "tipo_lead",
                "subtipo_lead",
                "asesor_gestion_lead",
                "estatus_management",
            ],
        )
    else:
        res = pd.DataFrame(
            {
                "nombre_lead": [""],
                "id_crm_lead": [""],
                "tipo_lead": [""],
                "subtipo_lead": [""],
                "asesor_gestion_lead": [""],
                "estatus_management": [""],
            }
        )
    return res


def get_dwh_info(data_frame, name: str = "text"):
    data_frame = data_frame.fillna("")
    ls_other = []
    for row in data_frame.iterrows():
        if len(str(row[1][name])) > 0 and (type(row[1][name]) == str):
            nombre = split_razon_social(str(row[1][name]))[0]
            nombre = nombre.strip()
            print(nombre)
            temp_cuentas = get_cuenta_info_dwh(nombre).to_dict("records")
            temp_leads = get_lead_info_dwh(nombre).to_dict("records")
            d2_temp = pd.DataFrame(row[1]).transpose()
        else:
            d2_temp = pd.DataFrame(row[1]).transpose()
            temp_leads = pd.DataFrame(
                {
                    "nombre_lead": [""],
                    "id_crm_lead": [""],
                    "tipo_lead": [""],
                    "subtipo_lead": [""],
                    "asesor_gestion_lead": [""],
                    "estatus_management": [""],
                }
            ).to_dict("records")
            temp_cuentas = pd.DataFrame(
                {
                    "nombre_cuenta": [""],
                    "agente_telefonico_cuenta": [""],
                    "idcrm_cuenta": [""],
                    "tipo_cuenta": [""],
                    "asesor_gestion_cuenta": [""],
                    "producto": [""],
                    "estatus_producto": [""],
                }
            ).to_dict("records")
        d2_temp["cuenta_"] = [temp_cuentas]
        d2_temp["lead_"] = [temp_leads]
        d2_temp = d2_temp.reset_index(drop=True)
        columnas = list(d2_temp.columns)
        columnas.remove("cuenta_")
        columnas.remove("lead_")
        if len(d2_temp.loc[0, "lead_"]) <= 1:
            lead = pd.DataFrame(d2_temp.loc[0, "lead_"]).reset_index(drop=True)
            leads = pd.concat(
                [d2_temp.drop(columns=["lead_", "cuenta_"]), lead],
                axis=1,
                join="outer"
            )
        else:
            leads = (
                d2_temp.groupby(columnas)
                .lead_.apply(lambda x: pd.DataFrame(x.values[0]))
                .reset_index()
            )
            if (
                len([x for x in list(leads.columns) if len(re.findall("level_", x))])
                > 0
            ):
                leads = leads.drop(columns=["level_{}".format(str(len(columnas)))])
        if len(d2_temp.loc[0, "cuenta_"]) <= 1:
            cuenta = pd.DataFrame(d2_temp.loc[0, "cuenta_"]).reset_index(drop=True)
            cuentas = pd.concat(
                [d2_temp.drop(columns=["lead_", "cuenta_"]), cuenta],
                axis=1,
                join="outer",
            )
        else:
            cuentas = (
                d2_temp.groupby(columnas)
                .cuenta_.apply(lambda x: pd.DataFrame(x.values[0]))
                .reset_index()
            )
            if (
                len([x for x in list(cuentas.columns) if len(re.findall("level_", x))])
                > 0
            ):
                cuentas = cuentas.drop(columns=["level_{}".format(str(len(columnas)))])
        ls_other.append(
            cuentas.merge(leads, how="outer", left_on=columnas, right_on=columnas)
        )
    return pd.concat(ls_other).reset_index(drop=True)


def split_razon_social(s1):
    rgx_lst = [
        " S[ \.]*A[ \.]*DE",
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
    res = re.split(rgx, cleaning_by_line_v3(s1, "nombres"))
    res = [re.sub(denominacion, "", x) for x in res if len(str(x)) > 0 and x != None]
    return res


# ------------------------------------------------------


def write_logfile(fileName, result_log):
    with open("../data/logs/logs.txt", "a") as file:
        file.write(
            "\n{0},{1},{2}".format(
                dt.now().strftime("%Y%m%d %H:%M:%S"), fileName, result_log
            )
        )


def write_report_actas(fileName):
    pd.DataFrame(fileName).to_csv("../data/tmp/reporte_actas.csv")

def write_txt(fileName, content):
    with open(fileName, "a") as file:
        file.write(
            content
            )


# -------------------------------------------------------
def proxy():
    user = "robiproxies"
    pwd = "Un1click"
    port_rnd = str(random.randint(1, 9999)).zfill(4)
    proxy = f"http://{user}:{pwd}@gate.dc.smartproxy.com:2{port_rnd}"
    return {"http": proxy, "https": proxy}


def smartproxy(url: str, session2, country=None, sticky=True):
    """Devuelve los proxies seleccionados."""
    user_agent = UserAgent()
    header = {"User-Agent": user_agent.random}
    user = "robiproxies"
    psswd = "Un1click"
    if sticky:
        r = str(random.randint(1, 9999)).zfill(4)
    else:
        r = "0000"
    if country == "US":
        proxy = {
            "http": f"http://{user}:{psswd}@us.smartproxy.com:1{r}",
            "https": f"http://{user}:{psswd}@us.smartproxy.com:1{r}",
        }
    elif country == "MX":
        proxy = {
            "http": f"http://{user}:{psswd}@mx.smartproxy.com:2{r}",
            "https": f"http://{user}:{psswd}@mx.smartproxy.com:2{r}",
        }
    elif country == "GLOBAL":
        proxy = {
            "http": f"http://{user}:{psswd}@gate.smartproxy.com:1{r}",
            "https": f"http://{user}:{psswd}@gate.smartproxy.com:1{r}",
        }
    elif country == None:
        proxy = {
            "http": f"http://{user}:{psswd}@gate.dc.smartproxy.com:2{r}",
            "https": f"http://{user}:{psswd}@gate.dc.smartproxy.com:2{r}",
        }
    return session2.get(
        url,
        proxies=proxy,
        headers=header,
    )


def ask_db_licitacion(DF_data, fields=["Codigo", "OpportunityId"]):
    ls_codigo = DF_data["Codigo"].values.tolist()
    ls_opid = DF_data["OpportunityId"].values.tolist()
    sql = Conection("DWH")
    response_sql = sql.searchData(ls_codigo, ls_opid)
    if len(response_sql) > 0:
        return DF_data[
                (DF_data["OpportunityId"].isin(response_sql["OpportunityId"]) == True)
                & (DF_data["Codigo"].isin(response_sql["Codigo"]) == True)
            ]
    else:
        return pd.DataFrame()


def ask_dl_licitacion(
    fecha, fields=["Codigo", "OpportunityId", "UrlActaDL", "NombreArchivoActa"]
):
    sql = Conection("DWH")
    response = sql.searchDbByDate(fecha, fields=fields)
    return response


def filter_new_licitaciones(df):
    # Query es un data frame que tiene la intersección de los datos en db Licitación y
    # con los datos filtrados con las fecha de última modificación
    if len(df) == 0:
        print("No hay nuevas licitaciones, ni actas que actalizar.Todo = 0")
        return df, df
    query = ask_db_licitacion(df)
    if len(query) > 0:
        return df[
            (df["OpportunityId"].isin(query["OpportunityId"]) == False)
            & (df["Codigo"].isin(query["Codigo"]) == False)
        ], df[
            (df["OpportunityId"].isin(query["OpportunityId"]) == True)
            & (df["Codigo"].isin(query["Codigo"]) == True)
        ]
    else:
        print("Todos los documentos son nuevos")
        return df, pd.DataFrame()


def filter_licitaciones_no_dl(df, fecha, fields):
    """Método para filtrar las licitaciones que ya están la db de sql, pero no han sido subidas a Data Lake"""
    # Query es un data frame que tiene la intersección de los datos en db Licitación que estan en DL y
    # con los datos filtrados con las fecha de última modificación
    query = ask_dl_licitacion(fecha, fields)
    # Tengo que buscar los licitantastes que tienen la misma FechaModificacion y que tienen URLACTA
    query = query[
        (query["UrlActaDL"].str.contains("proposiciones_"))
        & (query["NombreArchivoActa"].str.contains("proposiciones_"))
    ]
    if len(query) > 0:
        return df[
            (df["OpportunityId"].isin(query["OpportunityId"]) == False)
            & (df["Codigo"].isin(query["Codigo"]) == False)
        ]
    else:
        print("Todos los documentos filtrados NO están en DL")
        return df


def update_actas_descargadas(values, columns, table, condicion):
    """Método para actualizar valores en la base datos de sql"""
    condicion = """ Codigo = '{}' AND OpportunityId = CAST('{}' as INT)""".format(
        str(condicion[0]), str(condicion[1])
    )
    sql = Conection("DWH")
    res = sql.updateValue(values, columns, table, condicion)
    return res


def update_actas_subidas_dl(values, columns, table, condicion):
    """Método para actualizar valores en la base datos de sql"""
    condicion = """ Codigo = '{}' AND OpportunityId = CAST({} as INT)""".format(
        str(condicion[0]), str(condicion[1])
    )
    sql = Conection("DWH")
    res = sql.updateValue(values, columns, table, condicion)
    return res
    
def update_actas(values, columns, table, condicion):
    """Método para actualizar valores en la base datos de sql"""
    condicion = """ Codigo = '{0}' AND OpportunityId = CAST({1} as INT)""".format(
        str(condicion[0]), str(condicion[1])
    )
    sql = Conection("DWH")
    res = sql.updateValue(values, columns, table, condicion)
    return res


def buscar_pendientes_locales(data):
    """Busca los actas de proposiciones locales que fueron descargas,
    dado un data frame con los códigos de expediente y opp Id
    """
    data = data[["OpportunityId", "Codigo"]].values
    opportunityId = [x[0] for x in data]
    Codigo = [x[1] for x in data]
    names_pattern = [
        LOCAL_FOLDER + "proposiciones_" + str(x[0]) + "_" + str(x[1]) + "*.*"
        for x in data
    ]
    paths = [glob(x) for x in names_pattern]
    return paths, opportunityId, Codigo


def preparar_paths(data):
    """Método para preparar las direcciones locales a direcciones en DL"""
    names = [x[0].replace(LOCAL_FOLDER, "") for x in data]
    data = [x.split("_") for x in names]
    dates = [[x[3][:4], x[3][4:]] for x in data]
    blob_names = [
        BLOB_FOLDER + "/" + date[0] + "/" + date[1].zfill(2) + "/" + file_name
        for date, file_name in zip(dates, names)
    ]
    if len(names) == len(dates):
        return names, dates, blob_names
    return False

def get_file_size_in_megabytes(file_path):
    """ Get size of file at given path in bytes"""
    size = os.path.getsize(file_path)
    return size/(1024*1024)

def filtrar_uploaded_no_downloaded(data):
    """Función para filtrar las licitaciones que ya fueron cargadas
    previamente a la DB de SQL, pero que en la fecha en cual fueron
    actualizadas no se ha descargado el acta. Es decir UrlActaDL = ''
    y ActaPublicada = 0. Y utilizar UPDATE.

    Args:
        data (any): Data frame con las licitaciones filtradas
        query (str): Query para buscar las actas
    """
    Codigo = ",".join(["'"+str(x)+"'" for x in data.Codigo.tolist()])
    OppId = ",".join(["CAST('"+str(x)+"' as INT)" for x in data.OpportunityId.tolist()])
    query = """
    SELECT *
    FROM [DWH_ANALYTICS].[dbo].[Licitacion]
    WHERE Codigo IN ({0}) 
    AND OpportunityId IN ({1})
    AND ActaPublicada = CAST(0 as INT) 
    AND UrlActaDL = '' 
    """.format(Codigo, OppId)
    sql = Conection("DWH")
    query = sql.getQuery(query)
    return data[
        (data["OpportunityId"].isin(query["OpportunityId"]) == True)
        & (
            data["Codigo"].isin(query["Codigo"])
            == True
        )
    ]

def filtrar_uploaded_no_downloaded2(data):
    """Función para filtrar las licitaciones que ya fueron cargadas
    previamente a la DB de SQL, pero que en la fecha en cual fueron
    actualizadas no se ha descargado el acta. Es decir UrlActaDL = ''
    y ActaPublicada = 0. Y utilizar UPDATE.

    Args:
        data (any): Data frame con las licitaciones filtradas
        query (str): Query para buscar las actas
    """
    Codigo = ",".join([str(x) for x in data.Codigo.tolist()])
    OppId = ",".join([str(x) for x in data.OpportunityId.tolist()])
    query = """
    SELECT *
    FROM DWH_ANALYTICS.dbo.Licitacion
    WHERE Codigo IN ({0}) 
    AND OpportunityId IN ({1})
    AND ActaPublicada = CAST(0 as INT) 
    AND UrlActaDL = '' 
    """.format(Codigo, OppId)
    sql = Conection("DWH")
    query = sql.getQuery(query)
    return query

def filtrar_uploaded_downloaded(df, query):
    """Función para filtrar las licitaciones que ya fueron cargadas
    previamente a la DB de SQL,y que ya se descargaron pero que su 
    fecha de última de actualización ya cambió. Es decir UrlActaDL =/ ''
    y ActaPublicada =/ 0.

    Args:
        data (any): Data frame con las licitaciones filtradas que ya están en db
        query (any): Data frame contine licitaciones que ya están en db y que ya
                    no tiene actas descargas
    """
    return df[
        (df["OpportunityId"].isin(query["OpportunityId"]) == False)
        & (
            df["Codigo"].isin(query["Codigo"])
            == False
        )
    ]

def update_data_from_db(data):
    """Método para actualizar los datos de las licitacions que ya se tenian previamente en DB,
    pero que tienen cambios en, al menos, la fecha de última actualización. Para 
    ello se actualizara

    Args:
        data (aby): Dataframe con las licitaciones que han cambiado en su fecha de última actualización
                    y que ya se tenía previamente en DB.
    """
    print("Actualizando las base de datos ...")
    fecha_mod_reg =str(dt.today().replace(microsecond=0))
    table = "DWH_ANALYTICS.dbo.Licitacion"
    columns = data.columns.tolist()
    col_blocked = ['URLAnuncio', 'ActaPublicada', 'UrlActaDL', 
                   'NombreArchivoActa', 'FechaCreacionReg']
    for x in col_blocked: columns.remove(x)
    dat = data[columns]
    for i, row in dat.iterrows():
        codigo = str(row[0])
        numproc = str(row[1])
        s_numproc = "NumProc = '{}'".format(str(numproc))
        caractproc = row[2]
        s_caractproc = "CaracterProc = '{}'".format(str(caractproc))
        formaproc = row[3]
        s_formaproc = "FormaProc = '{}'".format(str(formaproc))
        articuloexcep = row[4]
        s_articuloexcep = "ArticuloExcepcion = '{}'".format(str(articuloexcep))
        refexp = row[5]
        s_refexp = "RefExpediente = '{}'".format(str(refexp))
        tituloexp = row[6]
        s_tituloexp = "TituloExpediente = '{}'".format(str(tituloexp))
        plantexp = row[7]
        s_plantexp = "PlantillaExpediente = '{}'".format(str(plantexp))
        descanu = row[8]
        s_descanu = "DescAnuncio = '{}'".format(str(descanu))
        claveuc = row[9]
        s_claveuc = "ClaveUC = '{}'".format(str(claveuc))
        nombreuc = str(row[10])
        s_nombreuc = "NombreUC = '{}'".format(nombreuc)
        operador = str(row[11])
        s_operador = "Operador = '{}'".format(operador)
        correoop = str(row[12])
        s_correop = "CorreoOperador = '{}'".format(correoop)
        idestado = str(row[13]).zfill(2)
        s_idestado = "IdEstado = '{}'".format(idestado)
        tipocontrac = str(row[14])
        s_tipocontrac = "TipoContratacion = '{}'".format(tipocontrac)
        fechapub = str(row[15])
        s_fechapub = "FechaPublicacion = CAST('{}' as DATETIME)".format(fechapub)
        vigencia = str(row[16])
        s_vigencia = "Vigencia = CAST('{}' as DATETIME)".format(vigencia)
        claveog = str(row[17])
        s_claveog = "ClaveCOG = '{}'".format(claveog)
        fechacreacion = str(row[18])
        s_fechacreacion = "FechaCreacion = CAST('{}' as DATETIME)".format(fechacreacion)
        fechamod = str(row[19])
        s_fechamod = "FechaModificacion = CAST('{}' AS DATETIME)".format(fechamod)       
        oppId = str(row[20])
        condicion = "Codigo = '{0}' AND OpportunityID = CAST('{1}' as INT)".format(str(codigo), str(oppId))
        sets = [s_numproc, s_caractproc,s_formaproc,s_articuloexcep,s_refexp, s_tituloexp,s_plantexp, s_descanu,s_claveuc ,s_nombreuc, 
        s_operador,s_correop,s_idestado, s_tipocontrac, s_fechapub, s_vigencia, s_claveog, s_fechacreacion, 
        s_fechamod]
        sets = ", ".join(sets)
        query_update = """UPDATE {0}
        SET {1}, FechaModificacionReg = CAST('{3}' as DATETIME)
        WHERE {2}""".format(table, sets, condicion, fecha_mod_reg)
        sql = Conection("DWH")
        print(query_update)
        query = sql.updateQuery(query_update)
    return query
def insertar_datos_nuevos(data, FieldList=[]):
    """Métido para insertar datos nuevos a la base de datos
    """
    sql = Conection('DWH')
    sql.InsertData(data, TableName='Licitacion',FieldList=FieldList )
if __name__ == "__main__":
    upload_txt_to_dl("hola", "prueba")
    DL = Datalake(BUCKET)
    DL.download_to_file(folder_bucket, "../data/tmp/prueba.txt")
