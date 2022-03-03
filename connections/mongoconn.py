from pymongo import MongoClient
import json
import os.path


def json_to_uri(json_):
    """
    Construye el string de la uri a partir de un diccionario.
    """
    user = json_['user']
    passwd = json_['passwd']
    host = json_['host']
    port = json_['port']
    auth = json_['auth']
    return f'mongodb://{user}:{passwd}@{host}:{port}/{auth}'

def MongoConn(auth_location, timeout=2000):#, database, collection,
    """
    Genera un objeto pymongo de conexión a la base y colección seleccionadas.
    """
    ext = os.path.splitext(auth_location)[1]
    if ext == '.json':
        with open(auth_location, 'rb') as f:
            json_ = json.load(f)
        uri = json_to_uri(json_)
    else:
        with open(auth_location) as f:
            uri = f.read()[:-1]
    client = MongoClient(uri, serverSelectionTimeoutMS=timeout)
    return client#[database][collection]


if __name__ == '__main__':

    # Para el caso de un json
    conn = MongoConn('../auth/mongo_robina.json')#, 'literata', 'api_dictamenes')
    print(conn)

    # Para el caso de la uri escrita en un txt
    conn = MongoConn('../auth/uri_robina.txt')#, 'literata', 'api_dictamenes')
    print(conn)
