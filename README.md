# Resumen del proyecto
El proyecto de Compranet es un flujo de trabajo que busca, descarga y almancena en data lake y DWH la información y archivos de las licitaciones filtradas con un día de antelación del accionar del flujo.

**Table of Contents**

[TOCM]

[TOC]

# Descripción del Proceso
El proyecto de Compranet consiste en un script que busca y descarga el expediente de licitaciones de las últimas 24 horas (Fecha de busqueda). Una vez obtenido el archivo excel, se filtran las licitaciones que cumplen con la condición que su Fecha de Última Modificación sea menor o igual que la Fecha de busqueda, a estas licitaciones se guardan como atributo de llamaremos data_filtered.

De las licitaciones filtradas se hace una segundo filtro, el cual consiste en preguntar a la base de datos de SQL si dichas licitaciones ya existen en la base en cuestion o son nuevas. Las nuevas se guardan como un atributo llamado new_licitaicones y las que ya existen se guardan como uploaded_db_licitaciones. 

Para las licitaciones que previamente existen en la base de datos, se hace un tercer filtro para preguntar ¿cuántas de estas actas tienen un archivo descargado y subido al data lake?. Aquellas actas que no han sido descargadas pero que ya existen en la base de datos se guardan como atributo con el nombre de uploaded_no_downloaded y aquellas que han sido descargadas como uploaded_yes_downloaded.

Acto seguido se insertan las nuevas licitaciones a la base de datos y se actualiza la información para las actas uploaded_no_downloaded con la nueva información que contiene el archivo de expedientes publicados, respectivamente.

Despues, de la carga de información se suman en un único data frame las actas nuevas y las uploaded_no_downloaded y se filtran si hay duplicados. Para posteriormente iniciar la descarga de cada acta de propuesta.

Si existe dicho archivo para el acta en cuestion, se descarga y se sube al data lake, actualizando los campos ActaPublicada y Url_visitada para el caso de la descarga y UrlActaDL para cuando se sube al data lake.

# Composición del proyecto
El proyecto está repartido en el siguiente sistema de estructura que contiene:
* app
* backend
* connections
* scraper
* src

### app
En esta carpeta se encuentra el archivo principal  `app.py` el cual acciona el flujo de trabajo completo para descargar las actas, filtrar las licitaciones y actualizar la base de datos.

Para accionar el script el usuario debe de entregarle un input númerico tipo entero que indique el número de días anteriores para los cuales se van a filtrar las licitaciones públicas presentadas en el archivo de ExpedientesPublicados que se ha descargado.

Ejemplo:

```
python3 -m pipenv run python app.py 1
```
En el ejemplo anterior el entero "1" siginifca que se filtraran las licitaciones con un día de 
anterioridad. Es decir, se filtran las licitaciones con 24 horas de anticipación con respecto a la fecha a la cual ha sido accionado el script.

### backend
Con respecto a la carpeta backend, aquí se encuentra el script `pendientes.py` que será accionado cada cierto tiempo. Que se encarga de dos procesos:
* Buscar en la base de datos cuales licitaciones ya fueron descargadas (ActaPublicada=1) y que no han sido subidas al Data Lake (UrlActaDL=''). Y subir dichas actas al Data Lake
* Buscar las actas que tuvieron problemas para acceder al link (Url_visitada=0) y que no han sido descaegadas (ActaPublicada=0), en el proceso princial. Y re-intentar la descarga de dichas licitaciones (en el caso de que exista tal acta), y posteriormente subir dicha acta al Data Lake.

En ambos procesos la base de datos en DWH debe ser actualizada.

### connections
En esta carpeta se encuentra el archivo `datalakeconn.py` que es modulo donde se encuentran las funciones para hacer conexición con el Data Lake.

También en esta carpeta se encuentra el archivo `DBServer.py` que es el modulo encargado de conectar con las base de datos en DWH.

### scraper
En la carpeta scraper se encuentra el archivo `tools.py` que es un modulo con las clases que utiliza el archivo principal `app.py`. Además, también es en tools donde se encuentran las scrapers para descargar las actas de compranet.

Sumado a lo anterior, también está incluido `aux_compranet.py` que es un modulo con funciones auxiliares para algunos procesos en tools.py.

### src
En src se encuentra el archivo `utils.py` que es modulo con funciones de utilidades generales que se utiliza en tools.py

