import sys
sys.path.append("../")
from scraper.tools import *
from scraper.aux_compranet import *
from datetime import timedelta, datetime as dt
import os
import time

###################################
# CONFIG
###################################
TMP_EXPEDIENTES = r"../data/tmp/"
TMP_ACTAS = r"../data/tmp/data"
INTENTOS = 3
###################################


def app(n_days=1):
    reporte_actas = {}
    start = time.time()
    URL_EXPEDIENTES = "https://www.gob.mx/compranet/documentos/datos-abiertos-250375"
    today = dt.today() - timedelta(days=0, hours=6, minutes=0)
    today = today.replace(hour=0, minute=0, second=0, microsecond=0)
    current_year = str(today.year)
    month = str(today.month)
    expedientes = DownloadExpedientes(None)
    expedientes.tmp_data = TMP_EXPEDIENTES
    list_expedientes = expedientes.get_expedientes_publicados(
        URL_EXPEDIENTES, current_year
    )
    for excel in list_expedientes:
        actas = {
            "actas_filtradas": [],
            "actas_nuevas_licitaciones_publicas": [],
            "actas_uploaded_db": '',
            "actas_uploaded_no_downloaded": '',
            "actas_uploaded_yes_downloaded": '',
            "total_actas_a_descargar": [],
            "actas_no_subidas_al_dl": [],
            "actas_no_descargadas": [],
            "actas_no_procesadas_literata": [],
            "actas_procesadas_literata": [],
            "actas_descargadas": [],
            "actas_subidas_al_dl": [],
        }
        print("Empezando proceso de extracción para {}".format(excel))
        expedientes_anuales = ExpedientesPublicados(path_file=excel)
        expedientes_anuales.reporte_actas = actas
        expedientes_anuales.reading_expedientes_publicados()
        expedientes_anuales.get_opportunity_id()
        expedientes_anuales.filter_n_day = n_days
        expedientes_anuales.filter_by_last_update()
        expedientes_anuales.prepare_data()
        expedientes_anuales.insertar_fecha_creacion()
        expedientes_anuales.insertar_fecha_modificacion_reg()
        expedientes_anuales.preparacion_estados()
        expedientes_anuales.mapear_id_estados()
        expedientes_anuales.data_frame_filtered = (
            expedientes_anuales.data_frame_filtered.fillna("")
        )
        expedientes_anuales.data_frame_filtered = (
            expedientes_anuales.data_frame_filtered
        )
        # Las de abajo deben de ser las licitaciones filtradas - las licitaciones de la db
        (
            expedientes_anuales.new_licitaciones,
            expedientes_anuales.uploaded_db_licitaciones,
        ) = filter_new_licitaciones(expedientes_anuales.data_frame_filtered)
        expedientes_anuales.uploaded_no_downloaded = filtrar_uploaded_no_downloaded(
            expedientes_anuales.uploaded_db_licitaciones
        )
        expedientes_anuales.uploaded_downloaded = filtrar_uploaded_downloaded(
            expedientes_anuales.uploaded_db_licitaciones,
            expedientes_anuales.uploaded_no_downloaded,
        )
        print("DF filtradas:  ", len(expedientes_anuales.data_frame_filtered))
        print("New licitaciones:  ", len(expedientes_anuales.new_licitaciones))
        print("uploaded db:  ", len(expedientes_anuales.uploaded_db_licitaciones))
        actas['actas_uploaded_db'] = str(len(expedientes_anuales.uploaded_db_licitaciones))
        print(
            "Uploaded no downloaded: ", len(expedientes_anuales.uploaded_no_downloaded)
        )
        actas["actas_uploaded_no_downloaded"] = str(len(expedientes_anuales.uploaded_no_downloaded))
        print("Uploaded yes downloaded: ", len(expedientes_anuales.uploaded_downloaded))
        actas["actas_uploaded_yes_downloaded"] = str(len(expedientes_anuales.uploaded_downloaded))
        #---------------------------
        # Mandar new licitaciones a DB Licitaciones
        # ---------------------------
        insertar_datos_nuevos(
            expedientes_anuales.new_licitaciones,
            FieldList=expedientes_anuales.new_licitaciones.columns.tolist(),
        )
        # --------------------------
        # Actualizar archivos existentes en db con nuevos cambios
        # --------------------------
        update_data_from_db(expedientes_anuales.uploaded_no_downloaded)
        # ---- Descargar actas -----------
        downloaded = DownloadExpedientes(expedientes_anuales)
        downloaded.tmp_data = TMP_ACTAS
        downloaded.download_data_expediente_publicados(current_year, month)
        reporte_actas[
            excel.replace(TMP_EXPEDIENTES, "")
        ] = downloaded.child.reporte_actas
    write_report_actas(reporte_actas)
    print(time.time() - start)


if __name__ == "__main__":
    try:
        print(sys.argv)
        n_days = int(sys.argv[1])
        for i in range(INTENTOS):
            try:
                app(n_days)
                break
            except Exception as e:
                print(e)
    except KeyboardInterrupt as e:
        print("Interrupted")
        fileName = "app.py"
        result_log = "KeyboardInterrupt error {}".format(e)
        write_logfile(fileName, result_log)
        try:
            sys.exit(0)
        except SystemExit as e:
            fileName = "app.py"
            result_log = "SystemExit error: {}".format(e)
            write_logfile(fileName, result_log)
            os._exit(0)
