# Proceso de validación de certificados de AFP

El objetivo general del presente proceso es validar certificados de cotizaciones de
AFPs, en el marco del proceso de aceptación de nuevos afiliados de la isapre.

El proceso desarrollado consta de las siguientes etapas de procesamiento:
1. Extracción de listado de certificados desde base de datos colmena
2. Descarga de documentos (pdf) asociados a listado de certificados
3. Extracción de metadatos y texto a partir de documentos (pdfs)
4. Extracción de certificado original desde sitio web de AFPs
5. Comparación entre documento y certificado original
6. Carga de resultados en base de datos colmena

Estas etapas se ejecutan de manera secuencial en una máquina virtual dentro del
proyecto "" de la nube de GCP de Colmena. En particular, dentro de la máquina se
configuró un repositorio con la siguiente estructura de archivos y carpetas:

- src
  - 1_extract.py (código de python que extrae el listado de certificados desde base de datos colmena)
  - 2_download.py (código de python que descarga los documentos pdf en base a listado de certificados)
  - 3_parse.py (código de python que extrae los metadatos y texto de cada documento descargado)
  - 4_afp.py (código de python que extrae el certificado original desde el sitio web de cada AFP)
  - 5_compare.py (código de python que compara el texto entre cada documento y certificado original)
  - 6_upload.py (código de python que carga el listado de certificados y los outputs en base de datos colmena)
- input
  - certificados.csv (almacena el listado de certificados extraídos desde la base de datos de colmena)
- output
  - certificados.csv (almacena listado de certificados con los respectivos metadatos e información extraída a partir del texto (rut, código de verificación), resultado de la extracción del certificado original desde sitio web de AFPs, y resultado de la comparación entre documento y certificado original)
- logs (alamcena registros de los errores de ejecución del proceso)
- pdfs (almacena los pdfs descargados en la etapa 2 del proceso)
- ./
  - requirements.txt (lista las librerías de python y su versión instaladas y utilizadas en el proyecto)
  - script.sh (script que ejecuta el proceso completo)
 
La ejecución del script (script.sh) se configuró a través de cron para que se ejecutara automáticamente todos los martes en la madrugada.

A continuación se describe cada una de las etapas indicando los respectivos inputs
que consume y los outputs que genera.

### 1. Extracción de listado de certificados desde base de datos colmena

La primera etapa considera la extracción de un listado de certificados de cotización
entregados por los afiliados a partir de una tabla generada en la base de datos
Snowflake de Colmena. Se ejecuta a través del código en el archivo "src/1_extract.py".
En particular, se utiliza la tabla "AFP_CERTIFICADOS" de la base "OPX" y se ejecuta la
consulta de los certificados con fecha de ingreso de los últimos 30 días que no hayan sido procesados (que no sean parte de la tabla output).
Se genera como resultado un archivo csv almacenado en "input/certificados.csv" con
las siguientes columnas: (doc_idn, link, periodo_produccion, fecha_ingreso). La
columna "link" contiene la url para descargar cada archivo en el formato
"http://mt.colmena.cl:4479/notifEnvioMailRest/public/documento/{doc_idn}".

### 2. Descarga de documentos (pdf) asociados a listado de certificados

La segunda etapa considera la descarga de cada uno de los documentos pdf en base
a los links almacenados en el archivo "input/certificados.csv". Se ejecuta a través del
código en el archivo "src/2_download.py".
En particular se utiliza la columna "link" del archivo csv y cada pdf es almacenado en
la carpeta "pdf" con el nombre "{doc_idn}.pdf".

### 3. Extracción de metadatos y texto a partir de documentos (pdfs)

La tercera etapa toma como input el listado de certificados desde el archivo
"input/certificados.csv" y para cada uno extrae los metadatos (creador, productor,
fecha de creación, y fecha de modificación) e información a partir del texto (AFP, rut,
código de verificación, y si es certificado de cotización) de cada pdf respectivo. Se
ejecuta a través del código en el archivo "src/3_parse.py".
Esta etapa genera como output un archivo csv almacenado en "output/certificados.csv" con
las siguientes columnas: (doc_idn, link, periodo_produccion, fecha_ingreso, metadata_creator, metadata_producer, metadata_creadate, metadata_moddate, afp, es_cert_cot, es_metadata, codver, rut).
La columna "es_cert_cot" (de tipo binaria True/False) indica si en el texto del
documento se encontró el título "Certificado de cotizaciones". La columna
"es_metadata" (de tipo binaria True/False) indica si los metadatos del documento se
condicen con los de la respectiva AFP (en base a una comparación no exacta).

### 4. Extracción de certificado original desde sitio web de AFPs

La cuarta etapa considera la consulta de manera automatizada en el sitio web de
cada AFP para comprobar la validez del certificado. Se ejecuta a través del código en
el archivo "src/4_afp.py".
Esta etapa toma como input el listado de certificados desde el archivo
"output/certificados.csv" y solo considera aquellos que cumplan con ser certificado de
cotizaciones (columna "es_cert_cot" = True) y que contenga información en las
columnas "afp", "rut" y "codver" (código de verificación).
La extracción del certificado original es un código y configuración particular para
cada AFP y el pdf extraído es almacenado en la carpeta "pdfs" con el nombre
"_{id}.pdf".
Adicionalmente, se genera como output una nueva columna (res_afp) en el archivo csv almacenado en "output/certificados.csv" que indica si el proceso de extracción fue exitoso o no (columna de tipo binaria ok/error).

### 5. Comparación entre documento y certificado original

La quinta etapa considera la comparación del texto extraído desde cada documento
y su respectivo certificado original extraído desde el sitio web de cada AFP. Se ejecuta
a través del código en el archivo "src/5_compare.py".
Esta etapa toma como input el listado de certificados desde el archivo
"output/certificados.csv" y solo considera aquellos que hayan sido extraídos sin error (columna "res_afp" = "ok"). Para cada certificado extrae el texto completo a partir del documento descargado "pdfs/{id}.pdf" y lo compara con el texto completo a partir del documento
"pdfs/_{id}.pdf". Si existen diferencias en el texto entre ambos documentos, se genera
un output con el detalle de las diferencias elaborado con la librería "difflib" de Python.
Esto genera como output nuevas columnas en el archivo csv almacenado en "output/certificados.csv" que incluye: la columna "es_dif" que indica si existen
diferencias en el texto entre ambos documentos (columna de tipo binaria True/False)
y la columna "res_dif" que detalla esas diferencias si es que existen.

### 6. Carga de resultados en base de datos colmena

La sexta etapa considera cargar el output del proceso (output/certificados.csv) en la tabla "AFP_CERTIFICADOS_OUTPUT" de la base de datos "OPX" de colmena.
La carga considera todos los registros de certificados con fecha de ingreso de los últimos 30 días que no hayan eran parte de la tabla output y las siguientes columnas: doc_idn, link, periodo_produccion, fecha_ingreso, metadata_creator, metadata_producer, metadata_creadate, metadata_moddate, es_metadata, afp, es_cert_cot, codver, rut, res_afp, es_dif, res_dif.
