# Integrantes del equipo

+ Rafael Larrazolo de la Cruz 118426
+ Diego Alejandro Estrada Rivera 165352
+ Victor Quintero Mármol González 175897

# Instrucciones

1. Instalar los paquetes necesarios indicados en el archivo *requirements.txt*:

*/path/to/ENV/bin/pip install -r requirements.txt*

Adicional a estos paquetes, se utilizan los paquetes os, time y sys, los cuales forman parte del stdlib de Python, por lo que solo es necesario importarlos (esta acción se hace dentro de *pipeline.py*)

2. Verificar que se tengan los archivos *parquete.py*, *agg.py* y *prfeco.csv* dentro de la misma ruta en S3, así como también crear una carpeta llamada *logs* en esa misma ruta. 

3. Verificar que en esa misma ruta de S3 no se tengan carpetas con los nombres *parquete* ni *resultado*.

4. Correr el archivo *pipeline.py*:

*/path/to/ENV/bin/python pipeline.py --local-scheduler Pipeline*

5. Si el pipeline fue exitoso revisar que en la ruta del script *pipeline.py* ahora se encuentre un archivo con formato json. En la ruta de S3 ahora habrá carpetas con los nombres *parquete*, que contiene el archivo *prfeco.csv* transformado en multiples archivos parquete, y *resultado*, que contiene el resultado del procesamiento en EMR en formato json.


# Resultado

Al ejecutar el archivo *pipeline.py* desde linea de comando se puede ver que el script fue exitoso, obteniendo los siguientes status de las tareas:

victor@victor-HP-Pavilion-Notebook:~/Documents/Maestria_DS/Metodos_gran_escala/tarea_8/tarea_8$ python pipeline.py --local-scheduler Pipeline
DEBUG: Checking if Pipeline() is complete
/home/victor/.local/lib/python3.6/site-packages/luigi/worker.py:346: UserWarning: Task Pipeline() without outputs has no custom complete() method
  is_complete = task.complete()
DEBUG: Checking if TerminateEmrCluster() is complete
INFO: Informed scheduler that task   Pipeline__99914b932b   has status   PENDING
DEBUG: Checking if DownloadS3() is complete
INFO: Informed scheduler that task   TerminateEmrCluster__99914b932b   has status   PENDING
DEBUG: Checking if AggProcessing() is complete
INFO: Informed scheduler that task   DownloadS3__99914b932b   has status   PENDING
DEBUG: Checking if DataFrameToParquete() is complete
INFO: Informed scheduler that task   AggProcessing__99914b932b   has status   PENDING
DEBUG: Checking if WaitEmrCluster() is complete
INFO: Informed scheduler that task   DataFrameToParquete__99914b932b   has status   PENDING
DEBUG: Checking if InitializeEmrCluster(cluster_name=cluster_equipo1, ec2_keyname=MGE_key, log_url=s3://vq-mcd2018/Tarea_8/logs) is complete
INFO: Informed scheduler that task   WaitEmrCluster__99914b932b   has status   PENDING
INFO: Informed scheduler that task   InitializeEmrCluster_cluster_equipo1_MGE_key_s3___vq_mcd2018__86554218ee   has status   PENDING
INFO: Done scheduling tasks
INFO: Running Worker with 1 processes
DEBUG: Asking scheduler for work...
DEBUG: Pending tasks: 7
INFO: [pid 5922] Worker Worker(salt=701102315, workers=1, host=victor-HP-Pavilion-Notebook, username=victor, pid=5922) running   InitializeEmrCluster(cluster_name=cluster_equipo1, ec2_keyname=MGE_key, log_url=s3://vq-mcd2018/Tarea_8/logs)
INFO: [pid 5922] Worker Worker(salt=701102315, workers=1, host=victor-HP-Pavilion-Notebook, username=victor, pid=5922) done      InitializeEmrCluster(cluster_name=cluster_equipo1, ec2_keyname=MGE_key, log_url=s3://vq-mcd2018/Tarea_8/logs)
DEBUG: 1 running tasks, waiting for next task to finish
INFO: Informed scheduler that task   InitializeEmrCluster_cluster_equipo1_MGE_key_s3___vq_mcd2018__86554218ee   has status   DONE
DEBUG: Asking scheduler for work...
DEBUG: Pending tasks: 6
INFO: [pid 5922] Worker Worker(salt=701102315, workers=1, host=victor-HP-Pavilion-Notebook, username=victor, pid=5922) running   WaitEmrCluster()
STARTING
STARTING
STARTING
STARTING
STARTING
STARTING
STARTING
STARTING
STARTING
STARTING
STARTING
WAITING
INFO: [pid 5922] Worker Worker(salt=701102315, workers=1, host=victor-HP-Pavilion-Notebook, username=victor, pid=5922) done      WaitEmrCluster()
DEBUG: 1 running tasks, waiting for next task to finish
INFO: Informed scheduler that task   WaitEmrCluster__99914b932b   has status   DONE
DEBUG: Asking scheduler for work...
DEBUG: Pending tasks: 5
INFO: [pid 5922] Worker Worker(salt=701102315, workers=1, host=victor-HP-Pavilion-Notebook, username=victor, pid=5922) running   DataFrameToParquete()
PENDING
PENDING
PENDING
RUNNING
RUNNING
RUNNING
RUNNING
RUNNING
RUNNING
RUNNING
RUNNING
RUNNING
RUNNING
RUNNING
COMPLETED
INFO: [pid 5922] Worker Worker(salt=701102315, workers=1, host=victor-HP-Pavilion-Notebook, username=victor, pid=5922) done      DataFrameToParquete()
DEBUG: 1 running tasks, waiting for next task to finish
INFO: Informed scheduler that task   DataFrameToParquete__99914b932b   has status   DONE
DEBUG: Asking scheduler for work...
DEBUG: Pending tasks: 4
INFO: [pid 5922] Worker Worker(salt=701102315, workers=1, host=victor-HP-Pavilion-Notebook, username=victor, pid=5922) running   AggProcessing()
PENDING
PENDING
PENDING
RUNNING
RUNNING
COMPLETED
INFO: [pid 5922] Worker Worker(salt=701102315, workers=1, host=victor-HP-Pavilion-Notebook, username=victor, pid=5922) done      AggProcessing()
DEBUG: 1 running tasks, waiting for next task to finish
INFO: Informed scheduler that task   AggProcessing__99914b932b   has status   DONE
DEBUG: Asking scheduler for work...
DEBUG: Pending tasks: 3
INFO: [pid 5922] Worker Worker(salt=701102315, workers=1, host=victor-HP-Pavilion-Notebook, username=victor, pid=5922) running   DownloadS3()
download: s3://vq-mcd2018/Tarea_8/resultado/_SUCCESS to ./_SUCCESS
download: s3://vq-mcd2018/Tarea_8/resultado/part-00000-100309c3-8a68-4597-b8ef-399f5e3812d9-c000.json to ./part-00000-100309c3-8a68-4597-b8ef-399f5e3812d9-c000.json
INFO: [pid 5922] Worker Worker(salt=701102315, workers=1, host=victor-HP-Pavilion-Notebook, username=victor, pid=5922) done      DownloadS3()
DEBUG: 1 running tasks, waiting for next task to finish
INFO: Informed scheduler that task   DownloadS3__99914b932b   has status   DONE
DEBUG: Asking scheduler for work...
DEBUG: Pending tasks: 2
INFO: [pid 5922] Worker Worker(salt=701102315, workers=1, host=victor-HP-Pavilion-Notebook, username=victor, pid=5922) running   TerminateEmrCluster()
TERMINATING
898
-----------------TIEMPO LIMITE DESTRUCCION DEL CLUSTER: CANCELANDO PIPELINE------------------
{'ResponseMetadata': {'RequestId': 'a02bff77-4db4-11e8-9e95-43947acefe3b', 'HTTPStatusCode': 200, 'HTTPHeaders': {'x-amzn-requestid': 'a02bff77-4db4-11e8-9e95-43947acefe3b', 'content-type': 'application/x-amz-json-1.1', 'content-length': '0', 'date': 'Wed, 02 May 2018 02:58:00 GMT'}, 'RetryAttempts': 0}}
INFO: [pid 5922] Worker Worker(salt=701102315, workers=1, host=victor-HP-Pavilion-Notebook, username=victor, pid=5922) done      TerminateEmrCluster()
DEBUG: 1 running tasks, waiting for next task to finish
INFO: Informed scheduler that task   TerminateEmrCluster__99914b932b   has status   DONE
DEBUG: Asking scheduler for work...
DEBUG: Pending tasks: 1
INFO: [pid 5922] Worker Worker(salt=701102315, workers=1, host=victor-HP-Pavilion-Notebook, username=victor, pid=5922) running   Pipeline()
INFO: [pid 5922] Worker Worker(salt=701102315, workers=1, host=victor-HP-Pavilion-Notebook, username=victor, pid=5922) done      Pipeline()
DEBUG: 1 running tasks, waiting for next task to finish
INFO: Informed scheduler that task   Pipeline__99914b932b   has status   DONE
DEBUG: Asking scheduler for work...
DEBUG: Done
DEBUG: There are no more tasks to run at this time
INFO: Worker Worker(salt=701102315, workers=1, host=victor-HP-Pavilion-Notebook, username=victor, pid=5922) was stopped. Shutting down Keep-Alive thread
INFO: 
===== Luigi Execution Summary =====

Scheduled 7 tasks of which:
* 7 ran successfully:
    - 1 AggProcessing()
    - 1 DataFrameToParquete()
    - 1 DownloadS3()
    - 1 InitializeEmrCluster(cluster_name=cluster_equipo1, ec2_keyname=MGE_key, log_url=s3://vq-mcd2018/Tarea_8/logs)
    - 1 Pipeline()
    ...

This progress looks :) because there were no failed tasks or missing external dependencies

===== Luigi Execution Summary =====

