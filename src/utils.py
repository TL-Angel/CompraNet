import re
from unicodedata import normalize

#####################################################################
# LIMPIEZA DE TEXTO
#####################################################################


def fix_lines_str(str_):
    """Remueve la newline simple."""
    str_ = re.sub("\n(?!\n)", " ", str_)
    str_ = re.sub(" +", " ", str_)
    str_ = re.sub("(\n )+", "\n", str_)
    return str_


def remover_acentos(texto):
    lista_cars = [
        ["Á", "A"],
        ["É", "E"],
        ["Í", "I"],
        ["Ó", "O"],
        ["Ú", "U"],
        ["á", "a"],
        ["é", "e"],
        ["í", "i"],
        ["ó", "o"],
        ["ú", "u"],
        ["À", "A"],
        ["È", "E"],
        ["Ì", "I"],
        ["Ò", "O"],
        ["Ù", "U"],
        ["à", "a"],
        ["è", "e"],
        ["ì", "i"],
        ["ò", "o"],
        ["ù", "u"],
    ]

    for cars in lista_cars:
        texto = re.sub(cars[0], cars[1], normalize("NFC", texto))
    return texto.strip()


def limpieza_texto(texto):
    texto = remover_acentos(texto)
    texto = re.sub(" +", " ", texto)
    texto = re.sub("[^a-zA-Z0-9\s]", " ", texto)
    texto = re.sub(" +", " ", texto)
    return texto.strip()


def clean_sample(sample):
    cont = sample[0]
    clean_cont = limpieza_texto(cont)
    entities = sample[1]["entities"]
    orgs = []
    clean_orgs = []

    for ent in entities:
        org = cont[ent[0]: ent[1]]
        orgs.append(org)
        clean_orgs.append(limpieza_texto(org))

    new_sample = (clean_cont, {"entities": []})
    for org in clean_orgs:
        span = re.search(org, clean_cont).span()
        new_sample[1]["entities"].append((span[0], span[1], "ORG"))

    return new_sample


def cleaning_by_line_v3(file, formato=r"alfabetopuntos"):
    # def cleaning_by_line_v3(file, formato= r"nombres"):
    """Funcion para limpiar datos a partir de un forma prediseñado en el argumento de "formato".

    Args:
        file ([string]): string a procesar

        formato (string, optional): Es el tipo de formato a elegir de los disponibles. Defaults to r"nombres".

    Returns:
        [string]: Regresa una string procesada
    """
    if formato == r"alfanumericopuntos":
        pattern = r"[^a-zA-Z0-9\.\,\:\nñÑ ]"
    elif formato == r"alfabetopuntos":
        pattern = r"[^a-zA-Z\.\,\:Ññ ]"
    elif formato == r"licitantes":
        pattern = r"[^a-zA-ZÑñ ]"
        file = file.upper()
    elif formato == r"alfabeto_min":
        pattern = r"[^a-zñÑ ]"
        file = file.lower()
    elif formato == r"alfa_min":
        pattern = r"[^a-zñ ]"
        p2 = r"[\.\,\:\n\t]+"
        file = file.lower()
        file = re.sub(p2, "", file)
    elif formato == r"nombres":
        pattern = r"[\.\,\:]"
        file = re.sub("(?=\w[X]+\w)[X]+", "", file.upper())
    file = remover_acentos(file)
    file = re.sub(pattern, " ", file)
    file = re.sub(" +", " ", file)
    file = re.sub("^\s+|\s+$|\s+(?=\s)", "", file)
    return file