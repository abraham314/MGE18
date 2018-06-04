### Pipeline Luigi para análisis de Quien es Quien en los Precios  
#### Francisco Bahena, Cristian Challu, Eduardo Hidalgo, Daniel Sharp  

# Importar librerías que se utilizarán
import luigi
import subprocess
import time
import sys

# Pase de inicialización de clúster
class StartCluster(luigi.Task):
    # Se asigna el valor a los parámetros que se utilizarán para levantar el clúster
    cluster_name = luigi.Parameter()
    keyname = luigi.Parameter()
    subnet_id = luigi.Parameter()
    region_id = luigi.Parameter()
    s3_url_log = luigi.Parameter()
    
    def run(self):
        # Se crea el comando que se utilizará para levantar el clúster a través de la CLI de Amazon.
        # En este caso se utilizan 3 máquinas para el clúster. 1 Master (m4.large) y dos esclavos (r3.xlarge). Se utilizan las r3 porque están optimizadas en memoria y tiene 30 GB RAM cada una
        command = "aws emr create-cluster --name "+self.cluster_name+" --release-label emr-5.13.0 --applications Name=Spark Name=Hadoop --instance-groups Name=Master,InstanceGroupType=MASTER,InstanceType=m4.large,InstanceCount=1 Name=Core,InstanceGroupType=CORE,InstanceType=r3.xlarge,InstanceCount=2 --ec2-attributes SubnetId="+self.subnet_id+",KeyName="+self.keyname+ " --use-default-roles --region " + self.region_id + " --log-uri " + self.s3_url_log + " --no-termination-protected --configurations file://./configs/emr-config.json"
        # Se envía el comando a bash y se obtiene el resultado que es el id del clúster. Es importante porque lo utilizaremos para los comandos posteriores
        proc = subprocess.Popen(command, shell =True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        tmp = proc.stdout.read()
        cluster_id = tmp.decode('utf-8').replace('\n','')
        print(cluster_id)
        time_passed = 0
        # Evaluamos cada 10 segundos el estatus del clúster, hasta por un máximo de 600 segundos (10 minutos)
        while time_passed < 600:
            print("Revisando correcto levantamiento de cluster durante máximo 600 segundos. Tiempo: " + str(time_passed))
            time.sleep(10)
            comm = 'aws emr describe-cluster --cluster-id ' + cluster_id
            proc = subprocess.Popen(comm, shell = True, stdout = subprocess.PIPE, stderr=subprocess.STDOUT)
            # Por la forma en como se recibe el output del comando, necesitamos 'limpiarlo' y, para verificar el estatus, lo metemos en un diccionario que podemos ver fácilmente.
            tmp = proc.stdout.read()
            tmp = tmp.decode('utf-8').split('\n')[:-1]
            dictio = {}
            for line in tmp:
                line = line.split('\t',1)
                dictio[line[0]] = line[1]
            # Esta es la condición deseada pues waiting implica que el clúster está listo. Cuando esto es cierto, escribe un archivo con el id del cluster y rompe el while
            if (dictio['STATUS'] == 'WAITING'):
                with self.output().open('w') as f:
                    f.write(cluster_id)
                break
            time_passed += 10
        # Si se cumple esta condición es porque el cluster no se levantó en menos de 600 segundos. Por lo tanto, se envía la instrucción para eliminarlo y se detiene el pipeline
        if (time_passed == 600):
            comm = 'aws emr terminate-clusters --cluster-ids ' + cluster_id
            proc = subprocess.Popen(comm, shell = True, stdout = subprocess.PIPE, stderr=subprocess.STDOUT)
            raise NameError('CLUSTER NO SE CREÓ EN MENOS DE 10 MINUTOS')
    
    def output(self):
        return luigi.LocalTarget("CREATE_CLUSTER_COMPLETE.txt")

    # Paso para convertir el archivo de CSV a Parquet
class ConvertToParquet(luigi.Task):
    cluster_name = luigi.Parameter()
    keyname = luigi.Parameter()
    subnet_id = luigi.Parameter()
    region_id = luigi.Parameter()
    s3_url_log = luigi.Parameter()

    def run(self):
        # Creamos la ruta de la cubeta
        s3_path = "s3a://"+str(self.s3_url_log).split('/')[2]
        # Leemos el nombre del cluster del archivo que se generó en el paso anterior
        with open("CREATE_CLUSTER_COMPLETE.txt", "r") as f:
            cluster_id = f.readline()
        # Creamos el comando para agregar el script que crea el parquet
        command = "aws emr add-steps --cluster-id " + cluster_id + " --steps Type=spark,Name=Parquet,Args=[--deploy-mode,cluster,--master,yarn,--conf,spark.yarn.submit.waitAppCompletion=true,"+s3_path+"/profeco/parquet.py,'"+s3_path+"'],ActionOnFailure=CONTINUE"
        proc = subprocess.Popen(command, shell =True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        tmp = proc.stdout.read()
        # Obtenemos el ID del Step creado y lo escribimo a un txt
        parquet_id = tmp.decode('utf-8').replace('\n','').replace('STEPIDS	','')
        print(parquet_id)
        with self.output().open('w') as f:
            f.write(parquet_id)
        # Creaos un 'do while' a partir del cual se puede monitorear el status de la ejecución. El ciclo se rompe cuando el proceso de completa.
        # Este es útil también para evitar que comience la ejecución del siguiente step
        while True:
            comm = 'aws emr describe-step --cluster-id ' + cluster_id + ' --step-id ' + parquet_id
            proc = subprocess.Popen(comm, shell = True, stdout = subprocess.PIPE, stderr=subprocess.STDOUT)
            tmp = proc.stdout.read()
            tmp = tmp.decode('utf-8').split('\n')[:-1]
            dictio = {}
            for line in tmp:
                line = line.split('\t',1)
                dictio[line[0]] = line[1]
            print("Status Actual Conversión a Parquet: "+ dictio['STATUS'])
            time.sleep(10)  
            if not dictio['STATUS']!='COMPLETED':
                break
            if dictio['STATUS']=='FAILED':
                comm = 'aws emr terminate-clusters --cluster-ids ' + cluster_id
                proc = subprocess.Popen(comm, shell = True, stdout = subprocess.PIPE, stderr=subprocess.STDOUT)
                raise NameError('NO SE CREO PARQUET CORRECTAMENTE')

    def output(self):
        return luigi.LocalTarget("CREATE_PARQUET_COMPLETE.txt")
        
    def requires(self):
        return StartCluster(self.cluster_name, self.keyname, self.subnet_id, self.region_id, self.s3_url_log)
        
# Paso que crea el resumen de nuestros datos. Este consiste en una tabla con la cuenta del número de artículos por producto y su precio promedio
class Agg(luigi.Task):
    cluster_name = luigi.Parameter()
    keyname = luigi.Parameter()
    subnet_id = luigi.Parameter()
    region_id = luigi.Parameter()
    s3_url_log = luigi.Parameter()

    def run(self):
        # Creamos la ruta de la cubeta
        s3_path = "s3a://"+str(self.s3_url_log).split('/')[2]
        # Leemos el cluster id para los siguientes pasos
        with open("CREATE_CLUSTER_COMPLETE.txt", "r") as f:
            cluster_id = f.readline()
        # Se agrega la ejecución del script al cluster
        command = "aws emr add-steps --cluster-id " + cluster_id + " --steps Type=spark,Name=Agg,Args=[--deploy-mode,cluster,--master,yarn,--conf,spark.yarn.submit.waitAppCompletion=true,"+s3_path+"/profeco/agg.py," + s3_path + "],ActionOnFailure=CONTINUE"
        proc = subprocess.Popen(command, shell =True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        tmp = proc.stdout.read()
        agg_id = tmp.decode('utf-8').replace('\n','').replace('STEPIDS	','')
        print(agg_id)
        with self.output().open('w') as f:
            f.write(agg_id)
        
        # Evaluamos el estado del proceso durante un máximo de 600 segundos
        time_passed = 0
        while time_passed < 600:
            print("Revisando correcta ejecución del script durante máximo 600 segundos. Tiempo: " + str(time_passed))
            time.sleep(10)
            comm = 'aws emr describe-step --cluster-id ' + cluster_id + ' --step-id ' + agg_id
            proc = subprocess.Popen(comm, shell = True, stdout = subprocess.PIPE, stderr=subprocess.STDOUT)
            tmp = proc.stdout.read()
            tmp = tmp.decode('utf-8').split('\n')[:-1]
            dictio = {}
            for line in tmp:
                line = line.split('\t',1)
                dictio[line[0]] = line[1]
            print("Status Actual: "+ dictio['STATUS'])
            # Evaluamos si el proceso está terminado, en cuyo caso rompemos el while
            if (dictio['STATUS'] == 'COMPLETED'):
                with self.output().open('w') as f:
                    f.write(agg_id)
                break
            # Evaluamos si el proceso falló, en cuyo caso detenemos el pipeline
            if (dictio['STATUS'] == 'FAILED'):
                comm = 'aws emr terminate-clusters --cluster-ids ' + cluster_id
                proc = subprocess.Popen(comm, shell = True, stdout = subprocess.PIPE, stderr=subprocess.STDOUT)
                raise NameError('TAREA FALLÓ, NO SE EJECUTO CORRECTAMENTE')
            time_passed += 10
            
        if (time_passed == 600):
            comm = 'aws emr terminate-clusters --cluster-ids ' + cluster_id
            proc = subprocess.Popen(comm, shell = True, stdout = subprocess.PIPE, stderr=subprocess.STDOUT)
            raise NameError('TAREA NO SE EJECUTÓ EN MENOS DE 10 MINUTOS')
    
    def output(self):
        return luigi.LocalTarget("CREATE_AGG_COMPLETE.txt")
        
    def requires(self):
        return ConvertToParquet(self.cluster_name, self.keyname, self.subnet_id, self.region_id, self.s3_url_log)
# Paso para terminar el clúster
class TerminateCluster(luigi.Task):
    cluster_name = luigi.Parameter()
    keyname = luigi.Parameter()
    subnet_id = luigi.Parameter()
    region_id = luigi.Parameter()
    s3_url_log = luigi.Parameter()
    
    def run(self):

        with open("CREATE_CLUSTER_COMPLETE.txt", "r") as f:
            cluster_id = f.readline()
        # Enviamos instrucción para eliminar el clúster
        command = 'aws emr terminate-clusters --cluster-ids ' + cluster_id        
        proc = subprocess.Popen(command, shell =True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        # Verificamos durante un máximo de 600 segundos que se elimine el cluster correctamente
        time_passed = 0
        while time_passed < 600:
            print("Revisando correcta terminación de cluster durante máximo 600 segundos. Tiempo: " + str(time_passed))
            time.sleep(10)
            comm = 'aws emr describe-cluster --cluster-id ' + cluster_id
            proc = subprocess.Popen(comm, shell = True, stdout = subprocess.PIPE, stderr=subprocess.STDOUT)
            tmp = proc.stdout.read()
            tmp = tmp.decode('utf-8').split('\n')[:-1]
            dictio = {}
            for line in tmp:
                line = line.split('\t',1)
                dictio[line[0]] = line[1]
            # Se evalúa si se eliminó correctamente y se rompe el while
            if (dictio['STATUS'] == 'TERMINATED'):
                with self.output().open('w') as f:
                    f.write("Cluster eliminado")
                break
            time_passed += 10
        # Si tomó más de 600 segundos, se detiene el pipeline.
        if (time_passed == 600):
            raise NameError('CLUSTER NO SE ELIMINÓ EN MENOS DE 10 MINUTOS')        


    def output(self):
        return luigi.LocalTarget("ENDED_CLUSTER_COMPLETE.txt")
        
    def requires(self):
        return Agg(self.cluster_name, self.keyname, self.subnet_id, self.region_id, self.s3_url_log)

# Fin del pipeline, se descarga el archivo con el análisis de S3
class EndPipeline(luigi.Task):
    cluster_name = luigi.Parameter()
    keyname = luigi.Parameter()
    subnet_id = luigi.Parameter()
    region_id = luigi.Parameter()
    s3_url_log = luigi.Parameter()

    def run(self):
        # Creamos la ruta de la cubeta
        s3_path = "s3://"+str(self.s3_url_log).split('/')[2]
        comm = 'aws s3 cp '+ s3_path +'/profeco/agg/ ./ --recursive --exclude "*" --include "*.csv"'
        proc = subprocess.Popen(comm, shell =True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        with self.output().open('w') as f:
            f.write("Pipeline terminado: "+proc)
        
    def output(self):
        return luigi.LocalTarget("FINISHED_PIPELINE.txt")

    def requires(self):
        return TerminateCluster(self.cluster_name, self.keyname, self.subnet_id, self.region_id, self.s3_url_log)

class StartPipeline(luigi.Task):
    cluster_name = luigi.Parameter()
    keyname = luigi.Parameter()
    subnet_id = luigi.Parameter()
    region_id = luigi.Parameter()
    s3_url_log = luigi.Parameter()
    
    def requires(self):
        return EndPipeline(self.cluster_name, self.keyname, self.subnet_id, self.region_id, self.s3_url_log)

