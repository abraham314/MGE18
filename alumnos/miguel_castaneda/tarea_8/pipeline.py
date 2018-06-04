### Tarea 8
### pipeline.py 
##

import os
import csv
import json
import luigi
import time
import random
import requests
from util import ejecuta_cmd
from util import copiaArchivoS3
from util import crea_cluster
from util import termina_cluster
from util import estadoCluster
from util import estadoStep
from util import creaStep
from util import sincronizaCarpeta


class CargaArchivosS3(luigi.Task):

    clusterName = luigi.Parameter(default="itam")
    keyName     = luigi.Parameter(default="mno_key")
    subnet      = luigi.Parameter(default="subnet-ade70de6")
    region      = luigi.Parameter(default="us-west-2")
    s3log       = luigi.Parameter(default="s3://metodosgranescala/tarea8")

    def output(self):
        return luigi.LocalTarget('/tmp/tarea8/CopiaArchivosS3.txt')

    def run(self):  
        ## Copia archivos a s3 
        copiaArchivoS3("all_data.csv",self.s3log + "/datos/")       
        ## Copia scripts a s3
        copiaArchivoS3("agg.py",self.s3log + "/scripts/")       
        copiaArchivoS3("parquet.py",self.s3log + "/scripts/")
        print("AWS cargo los archivos exitosamente")       
        with self.output().open('w') as f:
            f.write("AWS cargo los archivos exitosamente")
    
class CreaClusterEMR(luigi.Task):

    clusterName = luigi.Parameter(default="itam")
    keyName     = luigi.Parameter(default="mno_key")
    subnet      = luigi.Parameter(default="subnet-ade70de6")
    region      = luigi.Parameter(default="us-west-2")
    s3log       = luigi.Parameter(default="s3://metodosgranescala/tarea8")

    def output(self):
        return luigi.LocalTarget('/tmp/tarea8/CreaClusterEMR.txt')

    def run(self):
        id_cluster = crea_cluster(self.clusterName, self.subnet, self.keyName, self.region, self.s3log)
        with self.output().open('w') as f:
            f.write(str(id_cluster))
        print("Creando cluster de AWS...")

    def requires(self):
        return CargaArchivosS3()

class EstadoCreacionEMR(luigi.Task):

    clusterName = luigi.Parameter(default="itam")
    keyName     = luigi.Parameter(default="mno_key")
    subnet      = luigi.Parameter(default="subnet-ade70de6")
    region      = luigi.Parameter(default="us-west-2")
    s3log       = luigi.Parameter(default="s3://metodosgranescala/tarea8")

    id_cluster = ""
    
    def output(self):
        return luigi.LocalTarget("/tmp/tarea8/EstadoCreacionEMR.txt")

    def run(self):
        estado_aws = str()
        mins = 0
        seg = 0
        inc_seg = 10

        with self.input().open() as id_cluster_file:
            id_cluster = id_cluster_file.readline()


        while estado_aws!='WAITING' and mins < 10:
            estado_aws = estadoCluster(id_cluster)
            print("Llevas "+str(mins)+" minutos y "+str(seg)+" segundos")
            print("El estado es "+ estado_aws)
            time.sleep(inc_seg)
            seg += inc_seg
            if seg // 60 == 1:
                mins += 1 
            seg = seg%60

        if mins >= 10:
            termina_cluster(self.id_cluster)
            print("La creación del Cluster tardó más de 10 minutos, terminando pipeline y cerrando cluster...")
            quit()

        if estado_aws == 'WAITING':
            print("El cluster está listo para ejecutar el primer step")
            with self.output().open('w') as f:
                f.write(id_cluster)

    def requires(self):
        return CreaClusterEMR()        

class CrearStepParquet(luigi.Task):

    clusterName = luigi.Parameter(default="itam")
    keyName     = luigi.Parameter(default="mno_key")
    subnet      = luigi.Parameter(default="subnet-ade70de6")
    region      = luigi.Parameter(default="us-west-2")
    s3log       = luigi.Parameter(default="s3://metodosgranescala/tarea8")

    id_cluster = ""
    step_id = ""
    nombreScript = luigi.Parameter(default="s3://metodosgranescala/tarea8/scripts/parquet.py")

    entrada = luigi.Parameter(default="s3://metodosgranescala/tarea8/datos/all_data.csv")
    salida = luigi.Parameter(default="s3://metodosgranescala/tarea8/datos/all_data.parquet")
    

    def output(self):
        return luigi.LocalTarget('/tmp/tarea8/CrearStepParquet.txt')

    def run(self):


        with self.input().open() as id_cluster_file:
            self.id_cluster = id_cluster_file.readline()

        self.step_id = creaStep(self.id_cluster,self.nombreScript,self.entrada,self.salida)
        print("Corriendo el step para transformar en Parquet...")
        with self.output().open('w') as f:
            f.write(str(self.id_cluster)+"\n"+str(self.step_id))

    def requires(self):
        return EstadoCreacionEMR()

class EstadoStepParquet(luigi.Task):

    clusterName = luigi.Parameter(default="itam")
    keyName     = luigi.Parameter(default="mno_key")
    subnet      = luigi.Parameter(default="subnet-ade70de6")
    region      = luigi.Parameter(default="us-west-2")
    s3log       = luigi.Parameter(default="s3://metodosgranescala/tarea8")

    estado_step = ""
    id_cluster = ""
    step_id = ""
    

    def output(self):
        return luigi.LocalTarget('/tmp/tarea8/EstadoStepParquet.txt')

    def run(self):
        mins = 0
        seg = 0
        inc_seg = 10
        with self.input().open() as parameter_file:
            self.id_cluster = parameter_file.readline().rstrip()
            self.step_id = parameter_file.readline()
            print(self.id_cluster +":"+self.step_id)

        estado_step = estadoStep(self.id_cluster,self.step_id)

        estados_detener_ejecucion = ['COMPLETED','FAILED']

        while estado_step not in estados_detener_ejecucion and mins < 60:
            estado_step = estadoStep(self.id_cluster,self.step_id)
            print("Llevas "+str(mins)+" minutos y "+str(seg)+" segundos")
            print("El estado del Step es "+ estado_step)
            time.sleep(inc_seg)
            seg += inc_seg
            if seg // 60 == 1:
                mins += 1 
            seg = seg%60

        if mins >= 60:
            #termina_cluster(self.id_cluster)
            print("El Step del Cluster tardó más de 60 minutos, terminando pipeline y cerrando cluster...")
            quit()

        if estado_step == 'FAILED':
            #termina_cluster(self.id_cluster)
            print("El step de las agregaciones falló, terminando pipeline y cerrando cluster...")

        if estado_step == 'COMPLETED':
            print("El step del parquet se ha corrido exitosamente")
            with self.output().open('w') as f:
                f.write(str(self.id_cluster)+"\n"+str(self.step_id))

    def requires(self):
        return CrearStepParquet()

class CrearStepAgg(luigi.Task):

    clusterName = luigi.Parameter(default="itam")
    keyName     = luigi.Parameter(default="mno_key")
    subnet      = luigi.Parameter(default="subnet-ade70de6")
    region      = luigi.Parameter(default="us-west-2")
    s3log       = luigi.Parameter(default="s3://metodosgranescala/tarea8")

    id_cluster = ""
    step_id = ""
    nombreScript = luigi.Parameter(default="s3://metodosgranescala/tarea8/scripts/agg.py")

    entrada = luigi.Parameter(default="s3://metodosgranescala/tarea8/datos/all_data.parquet")
    salida = luigi.Parameter(default="s3://metodosgranescala/tarea8/datos/resultado.parquet")
    

    def output(self):
        return luigi.LocalTarget('/tmp/tarea8/CrearStepAgg.txt')

    def run(self):

        with self.input().open() as id_cluster_file:
            self.id_cluster = id_cluster_file.readline().rstrip()

        self.step_id = creaStep(self.id_cluster,self.nombreScript,self.entrada,self.salida)
        print("Corriendo el step para hacer las agregaciones...")
        with self.output().open('w') as f:
            f.write(str(self.id_cluster)+"\n"+str(self.step_id))

    def requires(self):
        return EstadoStepParquet()

class EstadoStepAgg(luigi.Task):

    clusterName = luigi.Parameter(default="itam")
    keyName     = luigi.Parameter(default="mno_key")
    subnet      = luigi.Parameter(default="subnet-ade70de6")
    region      = luigi.Parameter(default="us-west-2")
    s3log       = luigi.Parameter(default="s3://metodosgranescala/tarea8")

    estado_step = ""
    id_cluster = ""
    step_id = ""
    

    def output(self):
        return luigi.LocalTarget('/tmp/tarea8/EstadoStepAgg.txt')

    def run(self):
        mins = 0
        seg = 0
        inc_seg = 10
        with self.input().open() as parameter_file:
            self.id_cluster = parameter_file.readline().rstrip()
            self.step_id = parameter_file.readline()
            print(self.id_cluster +":"+self.step_id)

        estado_step = estadoStep(self.id_cluster,self.step_id)

        estados_detener_ejecucion = ['COMPLETED','FAILED']

        while estado_step not in estados_detener_ejecucion and mins < 60:
            estado_step = estadoStep(self.id_cluster,self.step_id)
            print("Llevas "+str(mins)+" minutos y "+str(seg)+" segundos")
            print("El estado del Step es "+ estado_step)
            time.sleep(inc_seg)
            seg += inc_seg
            if seg // 60 == 1:
                mins += 1 
            seg = seg%60

        if mins >= 60:
            #termina_cluster(self.id_cluster)
            print("El Step del Cluster tardó más de 60 minutos, terminando pipeline y cerrando cluster...")
            quit()

        if estado_step == 'FAILED':
            #termina_cluster(self.id_cluster)
            print("El step de las agregaciones falló, terminando pipeline y cerrando cluster...")

        if estado_step == 'COMPLETED':
            print("El step de las agregaciones se ha corrido exitosamente")
            with self.output().open('w') as f:
                f.write(str(self.id_cluster)+"\n"+str(self.step_id))

    def requires(self):
        return CrearStepAgg()

class CopiaResumenLocal(luigi.Task):

    clusterName = luigi.Parameter(default="itam")
    keyName     = luigi.Parameter(default="mno_key")
    subnet      = luigi.Parameter(default="subnet-ade70de6")
    region      = luigi.Parameter(default="us-west-2")
    s3log       = luigi.Parameter(default="s3://metodosgranescala/tarea8")

    archivoOrigen = luigi.Parameter(default="s3://metodosgranescala/tarea8/datos/resultado.parquet")
    carpetaDestino = luigi.Parameter(default="/tmp/tarea8/resultado.parquet")

    id_cluster = ""

    def output(self):
        return luigi.LocalTarget('/tmp/tarea8/CopiaResumenLocal.txt')

    def run(self):
        sincronizaCarpeta(self.archivoOrigen,self.carpetaDestino)
        print("Copiando archivo de resultados desde S3 a local")

        with self.input().open() as parameter_file:
            self.id_cluster = parameter_file.readline().rstrip()

        with self.output().open('w') as f:
            f.write(str(self.id_cluster))

    def requires(self):
        return EstadoStepAgg()

class TerminaClusterEMR(luigi.Task):

    clusterName = luigi.Parameter(default="itam")
    keyName     = luigi.Parameter(default="mno_key")
    subnet      = luigi.Parameter(default="subnet-ade70de6")
    region      = luigi.Parameter(default="us-west-2")
    s3log       = luigi.Parameter(default="s3://metodosgranescala/tarea8")

    id_cluster = ""

    def output(self):
        return luigi.LocalTarget('/tmp/tarea8/TerminaClusterEMR.txt')

    def run(self):
        with self.input().open() as parameter_file:
            self.id_cluster = parameter_file.readline().rstrip()
        termina_cluster(self.id_cluster)
        print("La instruccion para terminar el cluster se ha enviado...")
        with self.output().open('w') as f:
            f.write(self.id_cluster)

    def requires(self):
        return CopiaResumenLocal()

class EstadoTerminacionEMR(luigi.Task):

    clusterName = luigi.Parameter(default="itam")
    keyName     = luigi.Parameter(default="mno_key")
    subnet      = luigi.Parameter(default="subnet-ade70de6")
    region      = luigi.Parameter(default="us-west-2")
    s3log       = luigi.Parameter(default="s3://metodosgranescala/tarea8")

    id_cluster = ""
    
    def output(self):
        return luigi.LocalTarget("/tmp/tarea8/EstadoTerminacionEMR.txt")

    def run(self):
        estado_aws = str()
        mins = 0
        seg = 0
        inc_seg = 10

        with self.input().open() as id_cluster_file:
            self.id_cluster = id_cluster_file.readline()

        while estado_aws!='TERMINATED' and mins < 10:
            estado_aws = estadoCluster(self.id_cluster)
            print("Llevas "+str(mins)+" minutos y "+str(seg)+" segundos")
            print("El estado es "+ estado_aws)
            time.sleep(inc_seg)
            seg += inc_seg
            if seg // 60 == 1:
                mins += 1 
            seg = seg%60

        if mins >= 10:
            termina_cluster(self.id_cluster)
            print("La terminacion del Cluster tardo más de 10 minutos, terminando pipeline...")
            quit()

        if estado_aws == 'TERMINATED':
            print("El cluster esta terminado")
            with self.output().open('w') as f:
                f.write("El cluster esta terminado")

    def requires(self):
        return TerminaClusterEMR()  
