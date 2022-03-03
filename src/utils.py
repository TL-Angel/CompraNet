import json
import re
import random
import spacy
from unicodedata import normalize
from pymongo import MongoClient


#####################################################################
# CONEXION DB
#####################################################################

with open("../auth/uri_robina.txt") as f:
    URI = f.read().strip()

CONN = MongoClient(URI)
PYME_COL = CONN["datalake"]["hoovers_in_denue"]


def limit_gpu_use(verbose=True):
    import tensorflow as tf

    gpus = tf.config.experimental.list_physical_devices("GPU")
    if gpus:
        try:
            # Restrict TensorFlow to only use the fourth GPU
            tf.config.experimental.set_visible_devices(gpus[0], "GPU")
            # Currently, memory growth needs to be the same across GPUs
            for gpu in gpus:
                tf.config.experimental.set_memory_growth(gpu, True)
            logical_gpus = tf.config.experimental.list_logical_devices("GPU")
            if verbose:
                print(len(gpus), "Physical GPUs,", len(logical_gpus), "Logical GPUs")
            return True
        except RuntimeError as e:
            # Memory growth must be set before GPUs have been initialized
            if verbose:
                print(e)
            return False
    else:
        if verbose:
            print("No GPUs")
        return True


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
        org = cont[ent[0] : ent[1]]
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


#####################################################################
# GENERADOR DE PYMES
#####################################################################


def add_noise_pyme(name):
    if random.randint(0, 9) > 4:
        name = name.upper()

    if random.randint(0, 9) > 4:
        parts = name.split(".")
        if len(parts) == 1:
            return name
        else:
            new_name = ""
            for part in parts:
                cond2 = random.randint(0, 9)
                if cond2 > 4:
                    random_sep = ""
                else:
                    random_sep = "."
                new_name += part + random_sep
            return re.sub("(\.\.)$", ".", new_name)

    else:
        return name


def gen_pyme_list(size):
    pipe = [
        {"$sample": {"size": size}},
        {
            "$project": {
                "Company Name": 1,
                "_id": 0,
            }
        },
    ]
    return [add_noise_pyme(x["Company Name"]) for x in PYME_COL.aggregate(pipe)]


def random_pyme(lst):
    i = random.randint(0, len(lst) - 1)
    return lst[i]


def check_per(elem):
    ent = elem["entities"]
    for e in ent:
        if e[2] == "PER":
            return True
            break
    return False


def check_per_v2(elem):
    ent = elem[1]["entities"]
    for e in ent:
        if e[2] == "PER":
            return True
            break
    return False


def replace_entities(data, pyme_lst):
    try:
        cont = data["content"]
        ents = data["entities"]
        new_ents = []
        new_data = {}
        new_data["content"] = ""
        new_data["entities"] = []

        for i in range(len(ents)):
            new_ents.append(random_pyme(pyme_lst))
        #     new_ents = list(set(new_ents))

        new_cont = cont
        for i, new_ent in enumerate(new_ents):
            if i == 0:
                new_cont = cont
            old = cont[ents[i][0] : ents[i][1]]
            new_cont = new_cont.replace(old, " " + new_ent + " ", 1)
            new_cont = re.sub(" +", " ", new_cont)
            span = re.search(new_ent, new_cont).span()
            new_data["entities"].append([span[0], span[1], ents[i][2]])

        new_data["content"] = new_cont
        return new_data
    except Exception as e:
        print(e)
        print(data)
        return data


def replace_entities_v2(data, pyme_lst):
    try:
        cont = data[0]
        ents = data[1]["entities"]
        #             print(ents)
        new_ents = []
        new_data = ["", {"entities": []}]
        #         new_data['content'] = ''
        #         new_data['entities'] = []

        for i in range(len(ents)):
            new_ents.append(random_pyme(pyme_lst))
        #     new_ents = list(set(new_ents))

        new_cont = cont
        for i, new_ent in enumerate(new_ents):
            if i == 0:
                new_cont = cont
            old = cont[ents[i][0] : ents[i][1]]
            new_cont = new_cont.replace(old, new_ent, 1)
        for i, new_ent in enumerate(new_ents):
            span = re.search(new_ent, new_cont).span()
            new_data[1]["entities"].append([span[0], span[1], ents[i][2]])

        new_data[0] = new_cont
        return tuple(new_data)
    except Exception as e:
        print(e)
        print(data)
        return data


def print_ents(data):
    cont = data[0]
    entities = data[1]["entities"]
    orgs = []

    for ent in entities:
        org = cont[ent[0] : ent[1]]
        print(org)


#####################################################################
# ENTRENAMIENTO
#####################################################################


def train_spacy(data, iterations, return_history=False, from_scratch=True):
    TRAIN_DATA = data
    if from_scratch:
        nlp = spacy.blank("es")  # create blank Language class
        if "ner" not in nlp.pipe_names:
            ner = nlp.create_pipe("ner")
            nlp.add_pipe(ner, last=True)
    else:
        nlp = spacy.load("es_core_news_lg")
        # create the built-in pipeline components and add them to the pipeline
        # nlp.create_pipe works for built-ins that are registered with spaCy
        ner = nlp.get_pipe("ner")

    # add labels
    for _, annotations in TRAIN_DATA:
        for ent in annotations.get("entities"):
            ner.add_label(ent[2])

    # get names of other pipes to disable them during training
    other_pipes = [pipe for pipe in nlp.pipe_names if pipe != "ner"]
    history = []

    with nlp.disable_pipes(*other_pipes):  # only train NER

        optimizer = nlp.begin_training()

        for itn in range(iterations):
            print("Starting iteration " + str(itn))
            random.shuffle(TRAIN_DATA)
            losses = {}
            for text, annotations in TRAIN_DATA:
                nlp.update(
                    [text],  # batch of texts
                    [annotations],  # batch of annotations
                    drop=0.2,  # dropout - make it harder to memorise data
                    sgd=optimizer,  # callable to update weights
                    losses=losses,
                )
            history.append(losses)
            print(losses)
    if return_history:
        return nlp, history
    else:
        return nlp


#####################################################################
# EXTRACCION NER
#####################################################################

# NER_MODEL = spacy.load('../data/models/compranet_org_10k_1')
NER_MODEL = spacy.load(
    "../data/models/ner_alfanumpuntos_100iter_20000items_batch30_tsize50"
)


def filter_orgs(ent_lst):
    rgx_lst = [
        " S[ \.]*A[ \.]* DE",
        " S[ \.]* EN C[ \.]*",
        " S[ \.]* EN N[ \.]*C[ \.]*",
        "DE C[ \.]*V[ \.]*",
        "DE R[ \.]*L[ \.]*",
        "EN C[ \.] POR A" " C[ \.]*S[ \.]*",
        "( S[ \.]*C[ \.]*)$",
        "S[ \.]*O[ \.]*F[ \.]*O[ \.]*M",
        " S\.[ ]*A\.",
        "S\.[ ]*C\.",
        " S[ \.]*A[ \.]* ",
        "C\.[ ]*V\.",
        "R\.[ ]*L\.",
        "C\.[ ]*S\.",
        "S[ \.]*A[ \.]*P[ \.]*I",
        "S[ \.]*A[ \.]*A[\.]*",
        " S[ \.]*A[ \.]*C[\.]*",
        " S[ \.]*A[ \.]*S[\.]*",
    ]
    rgx = "("
    for i, r in enumerate(rgx_lst):
        if i != 0:
            rgx += "|"
        rgx += r
    rgx += ")"

    org_lst = []
    # print(ent_lst)
    for ent in ent_lst:
        if re.search(rgx, ent, re.IGNORECASE):
            org_lst.append(ent)

    return list(set(org_lst))


def extract_ents(text, filter_=True, raw=True):
    if raw:
        doc = NER_MODEL(cleaning_by_line_v3(text, formato="licitantes"))
    else:
        doc = NER_MODEL(text)

    results = []

    for ent in doc.ents:
        if str(ent.label_) == "ORG":
            text = ent.text.upper().strip()
            results.append({"text": text, "label": ent.label_})

    if filter_:
        filtered = filter_orgs([x["text"] for x in results])
        # print(filtered)
        results = [x for x in results if x["text"] in filtered]

    return [dict(t) for t in {tuple(d.items()) for d in results}]
