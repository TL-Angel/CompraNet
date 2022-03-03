import requests
from bs4 import BeautifulSoup
import os
from pathlib import Path
import uuid
import sys
from tools import *
from src.utils import *
from fake_useragent import UserAgent
sys.path.append("..")
from connections.datalakeconn import *

# from api.tools import *
from connections.mongoconn import MongoConn
from pymongo import ReplaceOne
from copy import deepcopy
import pandas as pd
import pymssql
import json
from datetime import timedelta, datetime as dt

# BUCKET = 'uniclick-dl-literata-union'
BUCKET = "uniclick-dl-robina-compranet"
folder_bucket = "Actas_Junta_Aclaraciones/"


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
            # return requests.get(url_txt).content.decode('utf-8')
            break
        except:
            url_txt = f"http://192.168.150.158:11733/uploads/{txt_file}"
            print("No procesada a literata")
            return "No procesada a literata"
    # if isinstance(pdf_, bytes): Path(pdf_path).unlink()
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


def upload_docs_to_mongo(
    input_docs, data_base="api_extractor_licitantes", collection="extracciones"
):
    """Sube los documentos Mongo."""
    docs = deepcopy(input_docs)
    extra_data_compranet_xpath = {
        "credito_externo": "//tr[.//td[contains(text(), 'Crédito externo')]]//td[position()=4]",
        "fecha_acto_fallo": "//tr[.//td[contains(text(), 'Fecha del acto de fallo')]]//td[position()=4]",
        "fecha_junta_aclaracion": "//tr[.//td[contains(text(), 'Fecha junta de aclaraciones')]]//td[position()=4]",
        "procedimiento_exclusivo_pymes": "//tr[.//td[contains(text(), 'Procedimiento exclusivo para MIPYMES')]]//td[position()=4]",
        "fecha_junta_revision_proyecto": "//tr[.//td[contains(text(), 'Fecha de la junta de revisión de proyecto')]]//td[position()=4]",
    }
    try:
        conn = MongoConn("../auth/mongo_robina.json")
        col = conn[data_base][collection]
        bulk_updates = []
        for doc in docs:
            today = dt.today() - timedelta(days=0, hours=6, minutes=0)
            doc["uuid"] = doc["filename"].split(".")[0]
            split_ = doc["uuid"].split("_")
            doc["opportunity"] = split_[0]
            for idx, value in get_extra_data(
                split_[0], extra_data_compranet_xpath
            ).items():
                try:
                    doc[idx] = value
                except Exception as e:
                    print("{} no encontrado".format(str(idx)))
                    pass
            doc["expediente"] = split_[1]
            doc["year"] = split_[2][:4]
            doc["month"] = split_[2][4:]
            doc["count"] = split_[3]
            doc["fecha_carga"] = today.replace(microsecond=0)
            filter_ = {"uuid": doc["uuid"]}
            update_ = ReplaceOne(filter_, doc, upsert=True)
            bulk_updates.append(update_)
        bulk_result = col.bulk_write(bulk_updates)
        return True
    except Exception as e:
        print(e)
        return False


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
            # doc['nombre_licitante'] = doc['Codigo del expediente']+ "_" + doc['opportunityId']
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
                [d2_temp.drop(columns=["lead_", "cuenta_"]), lead], axis=1, join="outer"
            )  # .reset_index(drop=True)
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
            )  # .reset_index(drop=True)
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
        #' S[ \.]*A[ \.]*',
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
    # with open('../data/tmp/reporte_actas.json', 'w') as file:
    #    file.write(json.dumps(json.JSONEncoder().encode(fileName)))
    # file.close()
    pd.DataFrame(fileName).to_csv("../data/tmp/reporte_actas.csv")


# -------------------------------------------------------
def proxy_session(url, session):
    user = "robiproxies"
    pwd = "Un1click"
    port_rnd = str(random.randint(1, 9999)).zfill(4)
    proxy = f"http://{user}:{pwd}@gate.dc.smartproxy.com:2{port_rnd}"
    print(proxy)
    return session.get(url, proxies={"http": proxy, "https": proxy})


def smartproxy(url: str, session2, country=None, sticky=True):
    """Devuelve los proxies seleccionados."""
    user_agent = UserAgent()
    headers = {'User-Agent': user_agent.random}
    user = "robiproxies"
    psswd = "Un1click"
    if sticky:
        r = str(random.randint(1, 9999)).zfill(4)
    else:
        r = str(random.randint(1, 9999)).zfill(4)
        #r = "0000"
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
        #proxies={"http": proxy["http"], "https": proxy["https"]},
        headers = {'User-Agent': user_agent.random}
        )
def smartproxy2( country=None, sticky=True):
    """Devuelve los proxies seleccionados."""
    user_agent = UserAgent()
    headers = {'User-Agent': user_agent.random}
    user = "robiproxies"
    psswd = "Un1click"
    if sticky:
        r = str(random.randint(1, 9999)).zfill(4)
    else:
        r = str(random.randint(1, 9999)).zfill(4)
        #r = "0000"
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
    return proxy


if __name__ == "__main__":
    upload_txt_to_dl("hola", "prueba")
    DL = Datalake(BUCKET)
    DL.download_to_file(folder_bucket, "./prueba.txt")
