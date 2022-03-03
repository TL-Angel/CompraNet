import re
from unicodedata import normalize


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
