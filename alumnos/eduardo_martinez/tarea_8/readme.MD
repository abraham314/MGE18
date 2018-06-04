# Tarea 8 - Luigi 

## Equipo 5

30/04/2018

_Jorge III Altamirano Astorga - 175904_

_Eduardo Selim Martinez Mayorga - 175921_

_Ariel Ernesto Vallarino Maritorena - 175875_

## Presentación

Presentamos esta tarea el equipo 5 para su amable consideración donde se utiliza Luigi para hacer una serie de pasos que forman un pipeline de procesamiento de datos. 

![[Overview del pipeline ](https://s3.amazonaws.com/jorge-altamirano/profeco/tarea_8.jpg)](https://s3.amazonaws.com/jorge-altamirano/profeco/tarea_8.jpg)

## Evidencia del cluster

![[Cluster en AWS](https://s3.amazonaws.com/jorge-altamirano/profeco/cluster.jpg)](https://s3.amazonaws.com/jorge-altamirano/profeco/cluster.jpg)

## Lista de archivos

Se incluyen los archivos solicitados:

* `pipeline.py` - Código Luigi

* `parquet.py` - Archivo que parquetea la base de datos de PROFECO con Apache Spark

* `agg.py` - Archivo que realiza las transformaciones de Apache Spark, en este caso un sumario

* `requirements.txt` (con base en un ambiente pyenv virtualenv) - Archivo con los requerimientos

* `readme.MD` (instrucciones de ejecución y detalle de status de las tareas)

Adicionalmente pusimos nuestros borradores en la carpeta `drafts/` como información complementaria, sin embargo: no constituye parte fundamental del proyecto o tarea, aunque los incluímos porque es una referencia (no ordenada) de lo que realizamos.

## Ejecución de los scripts

Es necesario como pre-requisito adicional a las librerías Python de `requirements.txt`:

1. Mac o Linux (preferentemente): Ubuntu 17.10+ o Fedora 22+

2. pyenv (recomendado) o python 3.6.4+

3. El [paquete de herramientas `awscli` ](https://aws.amazon.com/cli/) [configuradas!](https://docs.aws.amazon.com/cli/latest/userguide/cli-config-files.html) con la cuenta Amazon Web Services.

```
# En caso de que sea necesario instalar recurrir a la documentación
# oficial https://github.com/pyenv/pyenv#installation
pyenv install 3.5.2
pyenv shell 3.5.2
pip install --user -r requirements.txt
luigid --background
# La siguiente línea es la que ejecuta propiamente el Luigi :-)
python pipeline.j3a.py --local-scheduler run_pipeline
```

Estamos plenamente conscientes del requerimiento _Python(EMR) 3.4x_ y entendemos el beneficio sustancial de que sea "Unicode compliant" dicha versión, pero no lo utilizamos porque _le dimos la vuelta_ al problema de UTF cambiando el encoding, además de falta de tiempo para ejecutar y probar plenamente, pues observamos la [documentación oficial](https://aws.amazon.com/premiumsupport/knowledge-center/emr-pyspark-python-3x/) y es posible hacerlo (incluso sin pyenv) con esta configuración en json:

```
[
    {
    "Classification": "spark-env",
    "Configurations": [
            {
                "Classification": "export",
                "Properties": {
                    "PYSPARK_PYTHON": "/usr/bin/python3"
                }
            }
        ]
    }
]
```

## Input

Utilizamos la base de datos de PROFECO que nos fue proporcionada y se encuentra también disponible [aquí](https://s3.amazonaws.com/jorge-altamirano/profeco/data.csv). Esta la convertimos a formato Apache Parquet y se hicieron las transformacionesa partir de este archivo.

## Output

[![](https://s3.amazonaws.com/jorge-altamirano/profeco/summary.jpg)](https://s3.amazonaws.com/jorge-altamirano/profeco/summary.jpg)

## Conclusiones

Luigi nos resultó una herramienta diferente a lo que estábamos acostumbrados a programar (sin clases), por lo que nos tomó un tiempo acostumbrarnos y tomar la curva de aprendizaje necesaria. Nos pareció muy relevante que la herramienta estuviera programada en Python y pudiéramos realizar todo en Python. Esto lo hace en una opción natural a elegir para hacer pipelines.

Ahora tenemos una mejor comprehensión del trabajo del científico de datos en particular, y de Ciencia de Datos en general; pues las partes se van uniendo y se va formando un esquema mucho más completo de un entregable que será utilizado por alguna organización, esto de manera más profesional y sistemática _standing on giants' soulders_. Pues estamos aprovechando la infraestructura que otros científicos de datos han creado y utilizan con éxito. Es una herramienta más a tener en nuestro portafolio.

## Bibliografía

Nos apoyamos para la realización de este trabajo la siguiente bibliografía:

* <https://github.com/spotify/luigi/>
* <https://github.com/spotify/luigi/issues/1116>
* <https://github.com/spotify/luigi/blob/master/examples/pyspark_wc.py>
* <http://bionics.it/posts/luigi-tutorial>
* <https://stackoverflow.com/questions/2697039/python-equivalent-of-setinterval>
* <https://stackoverflow.com/questions/40218393/how-to-configure-luigi-task-retry-correctly>
* <https://stackoverflow.com/questions/23302184/running-pyspark-script-on-emr>
* <https://stackoverflow.com/questions/9942594/unicodeencodeerror-ascii-codec-cant-encode-character-u-xa0-in-position-20>
* <https://stackoverflow.com/a/39293287>