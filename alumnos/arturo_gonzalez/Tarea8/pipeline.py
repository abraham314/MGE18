import os
import json
import luigi
import time
import boto3
from glob import glob
 
#Iniciar el cluster de aws 
class StartCluster(luigi.Task):
    
    def requires(self):
        #Este task no requiere nada para arrancar (podriamos omitir este metodo), pero prefereimos
        #Ser explicitos. 
        return None

    def run(self):        
        client = boto3.client('emr', region_name='us-west-2')
        response = client.run_job_flow(
            Name="Tarea8-172906,90226,160668,148571,131113",
            ReleaseLabel='emr-5.13.0',
            LogUri='s3://al102964-bucket1/tarea8/logs',
            Instances={
                'MasterInstanceType': 'm4.xlarge',
                'SlaveInstanceType': 'm4.xlarge',
                'InstanceCount': 4,
                'KeepJobFlowAliveWhenNoSteps': True,
                'TerminationProtected': False,
                'Ec2SubnetId': 'subnet-460e170e',
                'Ec2KeyName': 'tutorial_key',
            },
            Applications=[                        
                {'Name': 'Spark'},
                {'Name': 'Zeppelin'}
            ],
            Configurations=[
                {
                    "Classification": "spark",
                    "Properties": {"maximizeResourceAllocation": "true","PYSPARK_PYTHON": "/usr/bin/python3"}
                }
            ],            
            VisibleToAllUsers=True,
            JobFlowRole='EMR_EC2_DefaultRole',
            ServiceRole='EMR_DefaultRole'
        )
        #Crea un archivo "writable" de nombre TMPSTARTCLUSTER que tiene dentro el parametro "JobFlowId" del diccionario "response"
        print(response)
        #Escribimos a archivo el id del cluster
        with open("tmpStartCluster", "w") as f:
            f.write(str(response["JobFlowId"]))
                    
    def output(self):
        return luigi.LocalTarget("tmpStartCluster")

#Clase que espera que el cluster este inicializado y que no pasen mas de 10 minutos requiere de la clase "StartCluster"
class WaitForCluster(luigi.Task):
        
    def requires(self):
        return StartCluster()
    
    #Lee el archivo de la clase anterior para obtener el id del cluster.
    def run(self):
        with self.input().open('r') as file_id:
            cluster_params = file_id.read()
        
        #Define las variables CLIENT, STATE y ELAPSED_SECONDS
        client = boto3.client('emr', region_name='us-west-2')

        state = 'STARTING'
        elapsed_seconds = 0
        
        #Mientras se este inicializando el cluster y lleve menos de 10 minutos
        #Imprime cada 10 segundos el estado del cluster y cuanto tiempo lleva corriendo el cluster
        while state == 'STARTING' and elapsed_seconds < 600:   
            response = client.describe_cluster(
                ClusterId=cluster_params
            )
            print(response)
            state = response["Cluster"]["Status"]["State"]
            starting_time = response["Cluster"]["Status"]["Timeline"]["CreationDateTime"]
            elapsed_seconds = int(time.time())-int(starting_time.strftime('%s'))
            print(str(state) + " "+ str(elapsed_seconds) + "\n")                        
            time.sleep(10)                    
        
        #Si el estado sigue en "STARTING" pero ELAPSED_SECONDS ya es más de 10 minutos (600 segundos) asigna el valor de "SHUTTING_DOWN" a VARIABLE
        #Si no, i.e. el estado STATE es diferente a "STARTING" asigna el valor de "SUBMITTING_STEP" a VARIABLE
        if state == 'STARTING':
            variable = 'SHUTTING_DOWN'        
        else:
            variable = 'SUBMITTING_STEP'
        
        #Crea el arhivo TMPWAITFORCLUSTER como readable y en el escribe las variables CLUSTER_PARAMS (id del cluster) y VARIABLE (estado del cluster)
        with open("tmpWaitForCluster", "w") as f:
            f.write(str(cluster_params+','+variable))


    def output(self):
        return luigi.LocalTarget("tmpWaitForCluster")

#clase SUBMITPARQUETSTEP que requiere de la clase WAITFORCLUSTER
class SubmitParquetStep(luigi.Task):  
    
    cluster_id = luigi.Parameter()

    def requires(self):
        return None
    
    #Lee los archivos de salida de la clase anterior y asignalos a CLUSTER_ID y a ESTATUS, es decir el CLUSTER_ID = CLUSTER_PARAMS (id del cluster)
    #y STATUS = VARIABLE (estado del cluster)
    def run(self):
        client = boto3.client('emr', region_name='us-west-2')
        step_parquet = {"Name": "parquetear" + time.strftime("%Y%m%d-%H:%M"),
            'ActionOnFailure': 'TERMINATE_JOB_FLOW',
            'HadoopJarStep': {
                'Jar': 's3n://elasticmapreduce/libs/script-runner/script-runner.jar',
                'Args': ["/usr/bin/spark-submit","s3a://al102964-bucket1/tarea8/parquet.py"]
            }
        }            
        action = client.add_job_flow_steps(JobFlowId=self.cluster_id, Steps=[step_parquet])
        print(action)
        with open("tmpSubmitParquetStep", "w") as f:
            f.write(str(self.cluster_id+','+action["StepIds"][0]))
        
    def output(self):
        return luigi.LocalTarget("tmpSubmitParquetStep")

#Clase SUBMITAGGSTEP que requiere de la clase SUBMITPARQUETSTEP
class SubmitAggStep(luigi.Task):  
    cluster_id = luigi.Parameter()    


    def requires(self):
        return SubmitParquetStep(self.cluster_id)
    
    #Lee los archivos de salida de la clase anterior y asignalos a CLUSTER_ID (id del cluster) y STEP_ID (status del step)
    def run(self):
        with self.input().open('r') as status:
            cluster_id,step_id = status.read().split(',')
        #Si el status del step es diferente a "JOB_NOT_STARTED" ejecuta como un step el programa agg.py, crea el archivo TMPSUBMITAGGSTEP y almacena el
        #id del cluster (cluster_id) y los status de los step (step_id, action["StepIDS"[0]])
        
        client = boto3.client('emr', region_name='us-west-2')            
        step_agg = {"Name": "agg" + time.strftime("%Y%m%d-%H:%M"),
            'ActionOnFailure': 'TERMINATE_JOB_FLOW',
            'HadoopJarStep': {
                'Jar': 's3n://elasticmapreduce/libs/script-runner/script-runner.jar',
                'Args': ["/usr/bin/spark-submit","s3a://al102964-bucket1/tarea8/agg.py"]
            }
        }
        action = client.add_job_flow_steps(JobFlowId=cluster_id, Steps=[step_agg])
        print(action)
        with open("tmpSubmitAggStep", "w") as f:
            f.write(str(cluster_id+","+ step_id+","+action["StepIds"][0]))
        #Si el status del step es igual a "JOB_NOT_STARTED" crea el archivo TMPSUBMITAGGSTEP y almacena el
        #id del cluster (cluster_id) y los status de los step como "JOB_NOT_STARTED"
    

    def output(self):
        return luigi.LocalTarget("tmpSubmitAggStep")



class MonitorSteps(luigi.Task):
    
    cluster_id = luigi.Parameter()    

    def requires(self):
        #Requiere de la clase SubmitAggStep antes de ésta
        return SubmitAggStep(self.cluster_id)
    
    #Abre el archivo de la salida de la clase anterior a modo de solo lectura, guarda la información del archivo a "cluster_id", "step_parquet_id" y "step_agg_id" separado por comas
    def run(self):
        with self.input().open('r') as status:
            cluster_id,step_parquet_id,step_agg_id = status.read().split(',')
        
        steps_ids = [step_parquet_id,step_agg_id]        
        client = boto3.client('emr', region_name='us-west-2')
                    
        flag = True
        states = []

        #MIentras la bandera sea igual a verdadero,  reinicializa la lista de estados de step (states[]),
        #por cada uno de los status de los steps (steps_ids) describe el step del cluster y appendealo a la lista de estados (states.append)
        #Cuando los estados de alguno de los 2 steps son cancelados o fallaron, o si ambos steps se completaron, cambia el valor de la bandera 
        while flag == True :
            states = []                   
            
            for step_id in steps_ids:        
                response = client.describe_step(
                    ClusterId=cluster_id,
                    StepId=step_id
                )
                print(response)
                states.append(response["Step"]["Status"]["State"])
            
            print(str(states) + "\n")                        
            
            if states[0] == 'CANCELLED' or states[0] == 'FAILED':     
                flag = False
            elif states[1] == 'CANCELLED' or states[1] == 'FAILED':     
                flag = False                
            elif states[0] == 'COMPLETED' and states[1] =='COMPLETED':
                flag = "Exito"                                
            time.sleep(10)  
        
        #Si se salió del loop por fallo, i.e. bandera es igual a falso, asigna "UNSUCCESFUL" a VARIABLE si no asignale "SUCCESFUL" (i.e. se salió del loop por bandera = exito)
        if flag == 'False':
            variable = 'UNSUCCESFUL'        
        else:
            variable = 'SUCCESFUL'
        
        #Crea el archivo tmpMonitorSteps y escribe en el id del cluster y si fue exitoso o no la ejeccución de steps
        with open("tmpMonitorSteps", "w") as f:
            f.write(str(cluster_id+","+variable))
        #Si el status del step_parquet_id es igual a "JOB_NOT_STARTED" crea el archivo TMPMONITORSTEPS Y Escribe en el "JOBS_NOT_STARTED"          
        
    def output(self):
        return luigi.LocalTarget("tmpMonitorSteps")

class TerminateCluster(luigi.Task):   

    cluster_id = luigi.Parameter()    

    def requires(self):
        return None
    
    #Abre el archivo de la salida de la clase anterior a modo de solo lectura, guarda la información del archivo a cluster_id y step_id separado por comas 
    def run(self):
        print("El id del cluster es: ")
        print(self.cluster_id)
        client = boto3.client('emr', region_name='us-west-2')
        
        #Termina los flows de trabajo del cluster         
        response = client.terminate_job_flows(JobFlowIds=[self.cluster_id])
        
        #Crea el archivo "tmpTerminateCluster" y en el escribe el id del cluster y "terminated"
        with open("tmpTerminateCluster", "w") as f:
            f.write(str(self.cluster_id+' Terminated'))
    
    def output(self):
        return luigi.LocalTarget("tmpTerminateCluster")


class Initializer(luigi.Task):
	
    def requires(self):
        return None
 
    def run(self):        
        #Cargamos dependencias de manera dinamica para poder tener mayor flexibilidad en los flujos
        #Esto se logra llamando a yield en lugar de incluir luigi tasks en el metodo requires
                        
        #LLamamos la clase WaitForCluster 
        wait_for_cluster_dependency = WaitForCluster()        
        yield wait_for_cluster_dependency   
        
        #Leemos salida de WaitForCluster
        with open("tmpWaitForCluster","r") as FileWaitForCluster:
            cluster_id,action = FileWaitForCluster.read().split(',')
        
        #Si el cluster levanto exitosamente dentro de 10 minutos entra a bloque:
        if action != "SHUTTING_DOWN":

            #Mandamos llamar MonitorSteps que depende de levantar el step de parquet y de agg
            monitor_steps_dependency = MonitorSteps(cluster_id)           
            yield monitor_steps_dependency
            
            #Verificamos el resultado de monitor steps
            with open("tmpMonitorSteps","r") as FileWaitForCluster:
                cluster_id,state = FileWaitForCluster.read().split(',')
                
                #Terminamos el cluster cuando ambos steps hayan terminado satisfactoriamente o 
                #lo terminamos cuando alguno de los dos falle.
                terminate_cluster_dependency = TerminateCluster(cluster_id)
                yield terminate_cluster_dependency
                
                if state == "SUCCESFUL":
                    #Copia el archivo de agregacion de un bucket de aws a local
                    os.system("aws s3 cp --recursive 's3://al102964-bucket1/tarea8/salida/' ./salida/")        
                    
                    #Escribimos a archivo tmpInitializer que todo fue exitoso
                    with open("tmpInitializer", "w") as f:
                        f.write('Los steps se completaron exitosamente y se descargo el archivo de agregacion final')
                else: 
                    #Escribimos a archivo tmpInitializer que hubo errores en los steps
                    with open("tmpInitializer", "w") as f:
                        f.write('Hubo errores en los steps')                    
        
        #Si el cluster no levanto exitosamente dentro de 10 minutos, lo termina
        else: 
            terminate_cluster_dependency = TerminateCluster(cluster_id)
            yield terminate_cluster_dependency

            #Escribimos a archivo tmpInitializer que el cluster no pudo levantar satisfactoriamente
            with open("tmpInitializer", "w") as f:
                f.write('El cluster no arranco en tiempo o hubo error al arrancar por lo que no se descargo ningun archivo de agregacion')
        
                
    def output(self):
        return luigi.LocalTarget("tmpInitializer")

        

class StartPipeline(luigi.Task):
    #producto = luigi.Parameter()
    #Borramos los archivos temporales de la ejecucion anterior asi como los resultados anteriores de aws
    os.system('rm tmp*')
    os.system('rm -r salida')
    os.system('aws s3 rm --recursive s3://al102964-bucket1/tarea8/logs')
    os.system('aws s3 rm --recursive s3://al102964-bucket1/tarea8/salida')
    os.system('aws s3 rm --recursive s3://al102964-bucket1/tarea8/parquet')

    #Require la clase Initializer
    def requires(self):        
        return Initializer()



#Comando de ejecucion
#PYTHONPATH="." luigi --module pipeline StartPipeline --local-scheduler

'''
Referencias
http://boto3.readthedocs.io/en/latest/reference/services/emr.html
http://luigi.readthedocs.io/en/stable/api/luigi.task.html
http://boto3.readthedocs.io/en/latest/reference/services/emr.html
'''