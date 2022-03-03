# -*- coding: utf-8 -*-
__author__ = "Oscar López"
__copyright__ = "Copyright 2021, Robina"
__credits__ = ["Oscar López"]
__license__ = "GPL"
__version__ = "0.1"
__email__ = "lpz.oscr@gmail.com"
__status__ = "Development"

import os
#google_key = '../auth/uniclick-dl-robina-prod-compranet.json'
google_key = '../auth/literata_prod_datalake.json'
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = google_key
from google.cloud import storage
from copy import deepcopy
# ROBINA_BUCK = 'uniclick-dl-robina-prod-billing-and-declarations'


def set_google_key(key_location: str):
    """Indica en donde se encuentran la key para acceder al DataLake"""
    import os
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = key_location


class Datalake:
    """Conexion a un bucket del Datalake."""

    def __init__(self, bucket_name: str, verbose=False):
        self.verb = verbose
        self.client = storage.Client()
        self.bucket_name = bucket_name
        self.bucket = self.client.bucket(bucket_name)

    def upload_file(self, file_name: str, blob_name: str):
        """Sube el archivo al bucket.
        """
        blob = self.bucket.blob(blob_name)
        blob.upload_from_filename(file_name)
        if self.verb:
            msg = f'archivo {file_name} subido a {self.bucket_name}/{blob_name}'
            print(msg)

    def upload_bytes(self, data_bytes: bytes, blob_name: str,
                     content_type: str='text/plain'):
        """Sube los bytes al bucket.
        """
        blob = self.bucket.blob(blob_name)
        blob.upload_from_string(data_bytes, content_type=content_type)
        if self.verb:
            msg = f'archivo subido a {self.bucket_name}/{blob_name}'
            print(msg)

    def download_bytes(self, blob_name: str):
        """Descarga el blob del bucket y lo devuelve como bytes.
        """
        blob = self.bucket.blob(blob_name)
        blob_bytes = blob.download_as_string()
        if self.verb: print(f'blob {blob_name} descargado')
        return blob_bytes

    def download_to_file(self, blob_name: str, file_name: str):
        """Descarga el blob del bucket al path de `file_name`.
        """
        blob = self.bucket.blob(blob_name)
        blob.download_to_filename(file_name)
        if self.verb: print(f'blob {blob_name} descargado en {file_name}')


# def upload_to_datalake(file_name, blob_name, bucket_name=ROBINA_BUCK,
#                        verbose=False):
#         """
#         """
#         # import os
#         # os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "./key_google_robina_edit.json"
#         dl = Datalake(bucket_name, verbose=verbose)
#         dl.upload(file_name, blob_name)
#
# def bytes_from_datalake(blob_name, bucket_name=ROBINA_BUCK, verbose=False):
#         """
#         """
#         dl = Datalake(bucket_name, verbose=verbose)
#         return dl.download_bytes(blob_name)


if __name__ == '__main__':

    pass

    # Prueba de upload:
    cons_buck = 'uniclick-dl-robina-prod'
    dl = Datalake(cons_buck, verbose=True)

    # file_name = '../data/delete_me.pdf'
    blob_name = 'api_dictamenes/dictamenes/p.pdf'
    # dl.upload_from_filename(file_name, blob_name)

    dl.download_to_file(blob_name, './delete_meeeee.pdf')
    # upload_to_datalake(file_name, blob_name)
    #
    # print(bytes_from_datalake(blob_name))
