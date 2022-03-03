from thinc.api import Adam
from thinc.api import SGD
import spacy
import random
from thinc.api import compounding
from pathlib import Path
import json
from spacy.training import Example
import re
from unicodedata import normalize
from pymongo import MongoClient
import string
import pandas as pd

# from tqdm import tqdm
def creating_new_model(arquitecture, idioma):
    # modelop en blanco
    nlp = spacy.blank(idioma)
    # creamos un ner y se agrega al pipeline
    ner = nlp.create_pipe(arquitecture)
    nlp.add_pipe(arquitecture)
    return nlp, ner


###############################################
def loading_model(model):
    # modelop en blanco
    nlp = spacy.load(model)
    # creamos un ner y se agrega al pipeline
    return nlp


###############################################
# Importando labels
def import_labels(ruta):
    f = open(ruta)
    return json.JSONDecoder().decode(f.read())


###############################################
# Importando traning data set
def import_data_set(ruta):
    with open(ruta, "r", encoding="utf-8") as f:
        data_training = [json.loads(line) for line in f]
    return data_training


##############################################
def trim_entity_spans(data: list) -> list:
    """Removes leading and trailing white spaces from entity spans.

    Args:
        data (list): The data to be cleaned in spaCy JSON format.

    Returns:
        list: The cleaned data.
    """
    invalid_span_tokens = re.compile(r"\s")

    cleaned_data = []
    for x in data:
        text = x["data"]
        entities = x["label"]
        valid_entities = []
        for start, end, label in entities:
            valid_start = start
            valid_end = end
            while valid_start < len(text) and invalid_span_tokens.match(
                text[valid_start]
            ):
                valid_start += 1
            while valid_end > 1 and invalid_span_tokens.match(text[valid_end - 1]):
                valid_end -= 1
            valid_entities.append([valid_start, valid_end, label])
        cleaned_data.append([text, {"entities": valid_entities}])

    return cleaned_data


##############################################
def trim_entity_spans_line(data: list) -> list:
    """Removes leading and trailing white spaces from entity spans.

    Args:
        data (list): The data to be cleaned in spaCy JSON format.

    Returns:
        list: The cleaned data.
    """
    invalid_span_tokens = re.compile(r"\s")

    cleaned_data = []
    for x in data:
        text = x["data"]
        entities = x["label"]
        valid_entities = []
        for start, end, label in entities:
            valid_start = start
            valid_end = end
            while valid_start < len(text) and invalid_span_tokens.match(
                text[valid_start]
            ):
                valid_start += 1
            while valid_end > 1 and invalid_span_tokens.match(text[valid_end - 1]):
                valid_end -= 1
            valid_entities.append([valid_start, valid_end, label])
        cleaned_data.append([text, {"entities": valid_entities}])

    return cleaned_data


##############################################
def cleaning_by_line(file):
    file = file.lower()
    file = remover_links(file)
    file = remover_puntuacion_emojis(file)
    file = remover_acentos(file)
    return re.sub("[^a-zA-Z0-9ñÑ]", " ", file)


################################
def cleaning_txt_list(rutas, tipo=".txt", formato="alfabetopuntos"):
    list_paths = [str(x) for x in Path(rutas).iterdir() if tipo in str(x)]
    docs = []
    for path in list_paths:
        if ".txt" in path:
            with open(path, "r") as f:
                file = str(f.read()).split('"')
                file = "".join(file)
                file = file.split(".\n")
                file = [cleaning_by_line_v2(f, formato) for f in file]
                docs.append(file)
    return docs, list_paths


def limpieza_texto(texto):
    texto = remover_acentos(texto)
    texto = re.sub(" +", " ", texto)
    texto = re.sub("[^a-zA-Z0-9\s.,:]", " ", texto)
    texto = re.sub(" +", " ", texto)
    return texto.strip()


def printing_jsonl(lista, paths):
    jsons = []
    digi = []
    for f, path in zip(lista, paths):
        with open(path.replace(".txt", "_limpiov4.jsonl"), "w", encoding="utf-8") as o:
            jsons.append(path.replace(".txt", "_limpiov4.jsonl"))
            for parrafo in f:
                file = {"data": parrafo, "label": []}
                digi.append(file)
                o.write(json.JSONEncoder().encode(file) + "\n")
        o.close()
    return jsons, digi


def digit_txt(lista_txt):
    digi = []
    for file in lista_txt:
        for parrafo in file:
            parrafos_list = {"data": parrafo, "label": []}
            digi.append(parrafos_list)
    return digi


def opening_txt(path_file):
    with open(path_file, "r", encoding="utf-8") as o:
        return str(o.read())


################################
def adding_labels(labels, ner):
    # importing labels
    for label in labels:
        ner.add_label(label["text"])
    return labels, ner


################################
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


def remover_links(file):
    file = re.sub("<.*?>", " ", file)
    url = re.compile(r"https?://\S+|www\.\S+")
    return url.sub(r"", file)


def remover_puntuacion_emojis(file):
    file = file.replace("“", "")
    file = file.replace("”", "")
    file = file.replace("", "")
    file = file.replace("´", "")
    file = file.replace("'", "")
    puntuacion = str.maketrans("", "", string.punctuation.replace(".", ""))
    file = file.translate(puntuacion)
    emoj = re.compile(
        "["
        u"\U0001F600-\U0001F64F"  # emoticons
        u"\U0001F300-\U0001F5FF"  # symbols & pictographs
        u"\U0001F680-\U0001F6FF"  # transport & map symbols
        u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
        u"\U00002500-\U00002BEF"  # chinese char
        u"\U00002702-\U000027B0"
        u"\U00002702-\U000027B0"
        u"\U000024C2-\U0001F251"
        u"\U0001f926-\U0001f937"
        u"\U00010000-\U0010ffff"
        u"\u2640-\u2642"
        u"\u2600-\u2B55"
        u"\u200d"
        u"\u23cf"
        u"\u23e9"
        u"\u231a"
        u"\ufe0f"  # dingbats
        u"\u3030"
        "]+",
        re.UNICODE,
    )
    return emoj.sub(r"", file)


############################################
def df_ner_model(test_data):
    df = df_test_v2(test_data)
    df = df.drop_duplicates()
    orgs = df[df.Entity_obtenida == "ORG"]
    orgs_true = df[
        (df.Entity_obtenida == "ORG")
        & (df.Entity_dataset == "ORG")
        & (df["T_F"] == "TP")
    ]
    orgs_false = df[
        (df.Entity_obtenida == "ORG")
        & (df.Entity_dataset == "ORG")
        & (df["T_F"] == "FP")
    ]
    org_mix = df[(df.Entity_obtenida == "ORG") & (df["T_F"] == "MIX")]
    manzanas = [
        len(orgs_false) / len(orgs),
        len(orgs_true) / len(orgs),
        len(org_mix) / len(orgs),
    ]
    nombres = ["ORG falsos postivios", "ORG verdaderos postivios", "ORG mixs"]
    plt.pie(manzanas, labels=nombres, autopct="%0.1f %%")
    plt.axis("equal")
    plt.show()
    return df, orgs, orgs_true, orgs_false, org_mix


############################################
##### generacion de aumento de data #######
def gen_pyme_list(size, pymes_col):
    pipe = [
        {"$sample": {"size": size}},
        {
            "$project": {
                "Company Name": 1,
                "_id": 0,
            }
        },
    ]
    pyme_lst = [x["Company Name"] for x in pymes_col.aggregate(pipe)]
    return pyme_lst


def random_pyme(lst):
    i = random.randint(0, len(lst) - 1)
    return lst[i]


#####################
def check_per(elem):
    ent = elem["entities"]
    for e in ent:
        if e[2] == "PERSON":
            return True
            break
    return False


def check_per_v2(elem):
    ent = elem[1]["entities"]
    for e in ent:
        if e[2] == "PERSON":
            return True
            break
    return False


#################################
#### reemplazar entidades ######
def replace_entities(data, pyme_lst):
    try:
        cont = data[0]
        ents = data[1]["entities"]
        new_ents = []
        new_data = {}
        new_data["content"] = ""
        new_data["entities"] = []

        for i in range(len(ents)):
            new_ents.append(random_pyme(pyme_lst))

        new_cont = cont
        for i, new_ent in enumerate(new_ents):
            if i == 0:
                new_cont = cont
            old = cont[ents[i][0] : ents[i][1]]
            new_cont = new_cont.replace(old, new_ent, 1)
            span = re.search(new_ent, new_cont).span()
            new_data["entities"].append([span[0], span[1], ents[i][2]])

        new_data["content"] = new_cont
        return new_data
    except Exception as e:
        print(e)
        return data

    return file.strip()


def clean_entities_v3(data, formato="alfabetopuntos"):
    try:
        cont = data["data"]
        ents = data["label"]
        new_ents = []
        new_data = {}
        new_data["entities"] = []

        new_cont = cleaning_by_line_v3(cont, formato)
        for i, new_ent in enumerate(ents):
            old = cont[ents[i][0] : ents[i][1]]
            old = cleaning_by_line_v3(old, formato)
            old = re.sub(r"^\s+|\s+$|\s+(?=\s)", "", old)
            new_cont = re.sub(r"^\s+|\s+$|\s+(?=\s)", "", new_cont)
            span = re.search(old, new_cont).span()
            new_data["entities"].append([span[0], span[1], ents[i][2]])
            new_data["entities"] = check_duplicates(new_data["entities"])
        return new_cont, new_data
    except Exception as e:
        print(e)
        cont = data["data"]
        ents = data["label"]
        new_data = {}
        new_data["entities"] = ents
        return new_cont, new_data


def only_org(data):
    d_all = []
    for text, labels in data:
        new_labels = {}
        new_labels["entities"] = []
        for l in labels["entities"]:
            if l[2] == "ORG":
                new_labels["entities"].append(l)
        if len(new_labels["entities"]) > 0:
            d_all.append((text, new_labels))
    return d_all


def replace_entities_v2(data, pyme_lst, ent_type="ORG"):
    try:
        cont = data[0]
        ents = data[1]["entities"]
        new_ents = []
        new_data = ["", {"entities": []}]

        for i in range(len(ents)):
            if ents[i][2] == ent_type:
                new_ents.append(random_pyme(pyme_lst))
            else:
                new_ents.append(cont[ents[i][0] : ents[i][1]])
        new_cont = cont

        for i, new_ent in enumerate(new_ents):
            if ents[i][2] == ent_type:
                old = cont[ents[i][0] : ents[i][1]]
                new_cont = new_cont.replace(old, new_ent, 1)
            else:
                pass
        for i, new_ent in enumerate(new_ents):
            span = re.search(new_ent, new_cont).span()
            new_data[1]["entities"].append([span[0], span[1], ents[i][2]])
        new_data[0] = new_cont
        new_data[1]["entities"] = check_duplicates(new_data[1]["entities"])
        return tuple(new_data)
    except Exception as e:
        print(e)
        return new_data


#########################################
#### data augmengtation #####
def new_pymes(list_text, list_pymes):
    return [replace_entities_v2(x, list_pymes) for x in list_text]


def list_pymes(iteraciones, list_text, list_pymes):
    ls = []
    for x in range(iteraciones):
        ls.append(new_pymes(list_text, list_pymes))
    return ls


def data_augmentation(iteraciones, list_text, list_pymes):
    x = list_text
    for i in range(iteraciones):
        x = x + new_pymes(list_text, list_pymes)
    return x


def cleaning_by_line_v2(file, formato=r"alfanumericopuntos"):
    if formato == r"alfanumericopuntos":
        pattern = r"[^a-zA-Z0-9\.\,\:\nñÑ ]"
    elif formato == r"alfabetopuntos":
        pattern = r"[^a-zA-Z\.\,\:ñÑ ]"
    elif formato == r"alfabeto_min":
        pattern = r"[^a-zñ ]"
        file = file.lower()
    elif formato == r"alfa_min":
        pattern = r"[^a-zñ ]"
        p2 = r"[\.\,\:\n\t]"
        file = file.lower()
        file = re.sub(p2, "", file)
        file = " ".join([i for i in file.split(" ") if i != " "])
    file = remover_acentos(file)
    return re.sub(pattern, " ", file)


def cleaning_by_line_v3(file, formato=r"alfabetopuntos"):
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
        pattern = r"[^A-ZÑ]"
        file = re.sub("(?=\w[X]+\w)[X]+", "", file.upper())
    file = remover_acentos(file)
    file = re.sub(pattern, " ", file)
    file = re.sub(" +", " ", file)
    file = re.sub("^\s+|\s+$|\s+(?=\s)", "", file)
    return file


def training_ner(nlp, data_set, n_iter, size_batch=10, optimizador=False, test_size=50):
    test_path = r"test_dataset_json/data29filesORG.jsonl"
    data_test = import_data_set(test_path)
    data_test = [clean_entities_v3(data) for data in data_test]
    data_test = random.sample(data_test, test_size)
    err_data = []
    scores = []
    if optimizador == False:
        optimizador = Adam(
            learn_rate=0.001,
            beta1=0.9,
            beta2=0.999,
            eps=1e-08,
            L2=1e-6,
            grad_clip=1.0,
            use_averages=True,
            L2_is_weight_decay=True,
        )

    # -> Optimizer (new)
    optimizer = nlp.begin_training(sgd=optimizador)
    other_pipes = [pipe for pipe in nlp.pipe_names if pipe != "ner"]
    ls_loss = []
    with nlp.disable_pipes(*other_pipes):
        for i in tqdm(range(n_iter), desc="Iteraciones"):
            random.shuffle(
                data_set
            )  # aqui mezcla los datos de entreamiento de manera aletoria por cada iteracion
            losses = {}  # creamos un diccionario para ir guardando los losses
            for batch in tqdm(
                spacy.util.minibatch(data_set, size=size_batch), desc="minibatch"
            ):  # itera sobre cada minibatch
                try:
                    # en las siguientes lineas separamos en listas diferetens cuales son texto y cuales son annotaions de cada minibatch
                    examples = []
                    for text, annotations in batch:
                        doc = nlp.make_doc(text)
                        example = Example.from_dict(doc, annotations)
                        examples.append(example)
                    nlp.update([example], sgd=optimizer, losses=losses, drop=0.2)
                    ls_loss.append(losses)
                    scores.append(evaluar(data_test, nlp))

                except Exception as e:
                    print(e)
                    err_data.append(batch)
                    pass

    return nlp, ls_loss, scores, err_data


def ner_pipeline(idioma, labels_raw, data_set, n_iter, size_batch=10, test_size=50):
    modelo = "ner"
    nlp, ner = creating_new_model(modelo, idioma)
    labels, ner = adding_labels(labels_raw, ner)
    nlp, loss, scores, err_data = training_ner(nlp, data_set, n_iter, size_batch)
    return nlp, loss, scores, err_data


def get_list_pymes(num_pymes, robina_path=r"uri_robina.txt"):
    with open(robina_path) as f:
        uri = f.read().strip()
    conn = MongoClient(uri)
    pymes_col = conn["datalake"]["hoovers_in_denue"]
    return gen_pyme_list(num_pymes, pymes_col)


def filter_null_dataset(data_set, formato=r"alfanumericopuntos"):
    lss = []
    for text, labels in data_set:
        labels["entities"] = check_duplicates(labels["entities"])
        if len(labels["entities"]) > 0:
            text = cleaning_by_line_v2(text, formato)
            lss.append((text, labels))
    return lss


def check_duplicates(list_elements):
    """Check if given list contains any duplicates"""
    for elem in list_elements:
        if list_elements.count(elem) > 1:
            while list_elements.count(elem) > 1:
                list_elements.remove(elem)
        else:
            pass
    return list_elements


####
def transformando_data(data_, model):
    x = []
    datos_par = []
    for text, labels in data_:
        x.append((text, model(text), labels))
    resultados = []
    pag = 0
    for text, res, labels in x:  # iter sobre cada parrafo
        test = [(str(text)[l[0] : l[1]].lower(), l[2]) for l in labels["entities"]]
        test2 = [cleaning_by_line_v3(str(i[0]), formato=r"alfa_min") for i in test]
        for label_obtenida in res.ents:  # iter sobre cada entidad obtenida, de cada res
            y_o = cleaning_by_line_v3(str(label_obtenida.text), formato=r"alfa_min")
            for label_test in test:  # iter sobre cada label del test de cada parrafo
                tx = cleaning_by_line_v3(str(label_test[0]), formato=r"alfa_min")
                if ((tx in y_o) or len([i for i in test2 if i in y_o]) > 0) and (
                    len(y_o) > 3
                ):
                    resultados.append(
                        [
                            str(label_obtenida.text),
                            label_obtenida.label_,
                            y_o,
                            str(test2),
                            label_test[1],
                            "TP",
                            len(test),
                            len(res.ents),
                            pag,
                        ]
                    )
                elif (y_o in " ".join(test2)) and (len(y_o) > 3):
                    resultados.append(
                        [
                            str(label_obtenida.text),
                            label_obtenida.label_,
                            y_o,
                            str(test2),
                            label_test[1],
                            "MIX",
                            len(test),
                            len(res.ents),
                            pag,
                        ]
                    )
                else:
                    resultados.append(
                        [
                            str(label_obtenida.text),
                            label_obtenida.label_,
                            "NO MATCH " + y_o,
                            str(test2),
                            label_test[1],
                            "FP",
                            len(test),
                            len(res.ents),
                            pag,
                        ]
                    )
        datos_par.append([pag])
        pag = pag + 1
    return x, resultados


def df_test(x):
    col_ = [
        "Texto_obtenido",
        "Label_obtenida",
        "Texto_dataset",
        "Label_dataset",
        "Len_test_labels",
        "Len_obtenidas_labels",
    ]
    resultados = []
    for texto, labels in x:
        for ent in texto.ents:
            label = [
                set(
                    [
                        (str(texto)[x[0] : x[1]].lower(), x[2])
                        for x in labels["entities"]
                    ]
                )
            ]
            for i in label:
                m, n = i[0], i[1]
                if str(m).lower() in set(label):
                    resultados.append([str(ent.text), str(ent.label_), m, n])
                    label.remove(i)

    df = pd.DataFrame(resultados, columns=col_)
    df["Coinciden"] = df["Texto_obtenido"].values == df["Texto_dataset"].values
    df["Coinciden"] = df["Coinciden"].apply(int)
    return df


def df_test_v2(
    resultados,
    col=[
        "Label_obtenida",
        "Entity_obtenida",
        "Match_label_contenido",
        "Labels_dataset",
        "Entity_dataset",
        "T_F",
        "Len_test_labels",
        "Len_obtenidas_labels",
        "Num_parrafo",
    ],
):
    df = pd.DataFrame(resultados, columns=col)
    return df


def evaluar(data, nlp):
    examples = []
    err = []
    evaluacion = []
    for text, annots in data:
        try:
            doc = nlp.make_doc(text)
            examples.append(Example.from_dict(doc, annots))
        except:
            err.append((text, annots))
    # for exp in examples:
    try:
        # evaluacion.append(nlp.evaluate(examples))
        evaluacion = nlp.evaluate(examples)
    except:
        err.append(examples)
    return evaluacion


#####################################################################
# EXTRACCION NER
#####################################################################

NER_MODEL = spacy.load("ner_alfanumpuntos_100iter_20kitems")


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
    l1 = r"""(?<=LICITANTE)\s+[a-zA_Z., ]+"""
    org_lst = []
    res = []
    for ent in ent_lst:
        if re.search(rgx, ent, re.IGNORECASE):
            res.append(ent)
            e = " ".join(re.findall(l1, ent))
            org_lst.append(e)
    return list(set(org_lst)), list(set(res))


def extract_orgs(text, filter_=True, raw=False):
    if raw:
        doc = NER_MODEL(re.sub("\n", " ", limpieza_texto(text)))
    else:
        doc = NER_MODEL(text)
    orgs = []

    for ent in doc.ents:
        if str(ent.label_) == "ORG":
            text = ent.text.upper().strip()
            orgs.append(text)
    if filter_:
        return filter_orgs(orgs)
    else:
        return orgs
