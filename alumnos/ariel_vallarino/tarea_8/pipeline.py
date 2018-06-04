# python3 pipeline.py --local-scheduler run_pipeline

import luigi
import boto3
import time
import os
import sys

default_region='us-east-1'
team_name = "equipo_05"

#----------[ Crear cluster:
class create_cluster(luigi.Task):

    def requires(self):
        return None

    def run(self):

        emr_connection = boto3.client('emr', region_name=default_region)

        # Valido si existe algún cluster activo:            
        JobFlowId = utils.cluster_activo()
        
        if JobFlowId == '':
            print('# -------------------------------------------------- #\n')
            print('Creando Cluster...')
            print('# -------------------------------------------------- #\n')
            response = emr_connection.run_job_flow(
                Name=team_name, #cluster_tarea_8_02
                ReleaseLabel='emr-5.13.0',
                Instances={
                    'MasterInstanceType': 'm3.xlarge',
                    'SlaveInstanceType': 'm3.xlarge',
                    'InstanceCount': 3,
                    'KeepJobFlowAliveWhenNoSteps': True,
                    'TerminationProtected': False,
                    #'Ec2SubnetId': 'subnet-b9ba47d1',
                    'Ec2KeyName': 'itam-aws'
                },
                Applications=[{'Name': 'Hadoop'},{'Name': 'Spark'},{'Name': 'Zeppelin'}],
                Configurations=[{"Classification": "spark",
                                 "Properties": {"maximizeResourceAllocation": "true"}
                                }],
                VisibleToAllUsers=True,
                JobFlowRole='EMR_EC2_DefaultRole',
                ServiceRole='EMR_DefaultRole',
                LogUri= "s3://aws-logs-927932162967-us-east-1/elasticmapreduce/"
            )
            JobFlowId = response["JobFlowId"]
            print('# -------------------------------------------------- #\n')
            print('Creando Cluster:' + JobFlowId)
            print('# -------------------------------------------------- #\n')
        else:
            print('# -------------------------------------------------- #\n')
            print('Cluster existente:' + JobFlowId)
            print('# -------------------------------------------------- #\n')
    
        with open("luigi_trace", "w") as f:
            f.write(JobFlowId + '\n' + "TASK1" + ',' "CREATE_CLUSTER\n")


    def output(self):
        return luigi.LocalTarget("luigi_trace")
#---------- Crear cluster ].



#----------[ Validar status de cluster:
class status_cluster(luigi.Task):

    def requires(self):
        return create_cluster()

    def run(self):
        # aws emr describe-cluster --cluster-id xxxx
        print('# -------------------------------------------------- #\n')
        print('Validando status de creación de cluster...')
        print('# -------------------------------------------------- #\n')

        # Obtengo datos de ejecucuion de Pipeline y genero diccionario:
        jobflow_id, dict_trace = utils.get_trace("luigi_trace")
                
       
        emr_connection = boto3.client('emr', region_name=default_region)        
        
        # Obtengo status del cluster:
        response = emr_connection.describe_cluster(ClusterId=jobflow_id)
        state = response["Cluster"]["Status"]["State"]
    
        elapsed_seconds = 0
        while state == 'STARTING' and elapsed_seconds <= 600:  
            
            # Obtengo status del cluster:
            response = emr_connection.describe_cluster(ClusterId=jobflow_id)
            state = response["Cluster"]["Status"]["State"]
            
            print('# -------------------------------------------------- #\n')
            print(response)
            print('# -------------------------------------------------- #\n')

            starting_time = response["Cluster"]["Status"]["Timeline"]["CreationDateTime"]
            elapsed_seconds = int(time.time())-int(starting_time.strftime('%s'))

            print(str(state) + " - Tiempo transcurrido: " + str(elapsed_seconds) + "\n")
            time.sleep(10)                   

        print('# -------------------------------------------------- #\n')
        print(state)
        print('# -------------------------------------------------- #\n')
        # Luego de transcurrido x min, valido status y determino siguiente paso:
        if state == 'STARTING':
            print('# -------------------------------------------------- #\n')
            print("Se ha superado el tiempo de espera. Se detendrá el pipeline. \n")
            print('# -------------------------------------------------- #\n')
            next_task = 'TERMINATED'
            # Termino el cluster. terminate_job_flows recibe una lista:
            jobflow_id_list = [jobflow_id]
            emr_connection.terminate_job_flows(JobFlowIds=jobflow_id_list)
            # Finalizo pipeline:
            sys.exit("Se ha superado el tiempo de espera. Se detendrá el pipeline.")

        else:
            print('# -------------------------------------------------- #\n')
            print("Cluster creado. Se procederá a lanzar el step. \n")
            print('# -------------------------------------------------- #\n')
            next_task = 'STEPa'
       
        with open("luigi_trace", "a") as f:
            f.write("TASK2" + ',' + next_task + '\n')


    def output(self):        
        return luigi.LocalTarget("luigi_trace")        
#---------- Validar status de cluster ].

#----------[ Ejecuta Step A:
class launch_stepA(luigi.Task):  
   
    def requires(self):
        return status_cluster()

    def run(self):        
        # Obtengo datos de ejecucuion de Pipeline y genero diccionario:
        jobflow_id, dict_trace = utils.get_trace("luigi_trace")
        print('# -------------------------------------------------- #\n')
        print('Lanzando Step A: parqueteo en %s...'%jobflow_id)
        print('# -------------------------------------------------- #\n')
        if not utils.parqueteo_pending(jobflow_id) and not utils.sumarizacion_completed(jobflow_id):
            print(u'We are going to run parqueteo...')
        else:
            print(u"I think we ain't going to run parqueteo. Pulling forward next steps.")
        # Valido que el estado del paso anterior sea STEP:        
        if "TASK2" in dict_trace and dict_trace["TASK2"] == 'STEPa' and \
            not utils.parqueteo_pending(jobflow_id) and not utils.parqueteo_completed(jobflow_id):
            #aws emr add-steps --cluster-id JobFlowId --steps Type=spark,Name=parqueteo,Args=[--deploy-mode,cluster,--master,yarn,--conf,spark.yarn.submit.waitAppCompletion=true,s3a://jorge-altamirano/profeco/parquet.j3a.py],ActionOnFailure=CONTINUE
            emr_connection = boto3.client('emr', region_name=default_region)
            response = emr_connection.add_job_flow_steps(JobFlowId=jobflow_id,                
                                                         Steps=[{
                                                             'Name': 'parqueteo',
                                                             'ActionOnFailure': 'TERMINATE_JOB_FLOW',
                                                             'HadoopJarStep': {
                                                                 'Jar': 'command-runner.jar',
                                                                 'Args': [
                                                                     "spark-submit",
                                                                     "s3a://jorge-altamirano/profeco/parquet.j3a.py",
                                                                 ]
                                                             }
                                                         }]
                                                        )

        with open("luigi_trace", "a") as f:
            f.write("TASK3" + ',' + 'STEPb' + '\n')
    def output(self):
        return luigi.LocalTarget("luigi_trace")
#---------- Ejecuta Step A].

#----------[ Ejecuta Step B:
class launch_stepB(luigi.Task):  
   
    def requires(self):
        return launch_stepA()

    def run(self):        
        # Obtengo datos de ejecucuion de Pipeline y genero diccionario:
        jobflow_id, dict_trace = utils.get_trace("luigi_trace")
        print('# -------------------------------------------------- #\n')
        print('Lanzando Step B sumarización en %s...'%jobflow_id)
        print('# -------------------------------------------------- #\n')
        if not utils.sumarizacion_pending(jobflow_id) and not utils.sumarizacion_completed(jobflow_id):
            print(u'We are going to run sumarización...')
        else:
            print(u"I think we ain't going to run sumarización. Pulling forward next steps.")
        print("Running summary... %s \n"%str(dict_trace))
        # Valido que el estado del paso anterior sea STEP:        
        if "TASK3" in dict_trace and dict_trace["TASK3"] == 'STEPb' and \
            not utils.sumarizacion_pending(jobflow_id) and not utils.sumarizacion_completed(jobflow_id):
            
            emr_connection = boto3.client('emr', region_name=default_region)
            try:
                response = emr_connection.add_job_flow_steps(JobFlowId=jobflow_id,                
                                                         Steps=[{
                                                             'Name': 'sumarizacion',
                                                             'ActionOnFailure': 'CONTINUE',
                                                             'HadoopJarStep': {
                                                                 'Jar': 'command-runner.jar',
                                                                 'Args': [
                                                                     "spark-submit",
                                                                     "s3a://jorge-altamirano/profeco/agg.j3a.py",
                                                                 ]
                                                             }
                                                         }]
                                                        )
            except:
                print("Retrying Pandas install :-)")
                response = emr_connection.add_job_flow_steps(JobFlowId=jobflow_id,                
                                                     Steps=[{
                                                         'Name': 'sumarizacion',
                                                         'ActionOnFailure': 'TERMINATE_JOB_FLOW',
                                                         'HadoopJarStep': {
                                                         'Jar': 'command-runner.jar',
                                                         'Args': [
                                                             "spark-submit",
                                                             "s3a://jorge-altamirano/profeco/agg.j3a.py"
                                                                 ]
                                                             }
                                                         }]
                                                        )
                pass
            
            JobFlowId = utils.cluster_activo()

            with open("luigi_trace", "a") as f:
                f.write("TASK3" + ',' + JobFlowId + '\n')       
    def output(self):
        return luigi.LocalTarget("luigi_trace")
#---------- Ejecuta Step B].

#----------[ Validar status del step:
class status_step(luigi.Task):
    def requires(self):
        return launch_stepB()
    def run(self):
        # aws emr describe-step --cluster-id xxxxx --step-id yyyyyy
        print('# -------------------------------------------------- #\n')
        print('Validando status de ejecución de Step...')
        print('# -------------------------------------------------- #\n')
        print("We're giving a bit of time so the the steps are loaded into aws... ", end="")
        # Obtengo datos de ejecucuion de Pipeline y genero diccionario:
        jobflow_id, dict_trace = utils.get_trace("luigi_trace")
        emr_connection = boto3.client('emr', region_name=default_region)
        time.sleep(10)
        print("Done!")        
        elapsed_seconds = 0             
        state = utils.get_step_state(jobflow_id)
        print('# -------------------------------------------------- #')
        print(str(state) + " - Tiempo transcurrido: " + str(elapsed_seconds))
        print("Parqueteo:    en curso=%s, finalizado=%s"% 
              (utils.parqueteo_pending(jobflow_id), utils.parqueteo_completed(jobflow_id)))
        print(u"Sumarización: en curso=%s, finalizado=%s"% 
              (utils.sumarizacion_pending(jobflow_id), utils.sumarizacion_completed(jobflow_id)))
        print('# -------------------------------------------------- #\n')
        if ("TASK3" in dict_trace) and (dict_trace["TASK3"] != 'ERROR'):
            state = utils.get_step_state(jobflow_id)
            print("Starting while loop in %s!"%jobflow_id)
            print("Cluster state: %s"%utils.get_cluster_state(jobflow_id))
            while utils.get_cluster_state(jobflow_id) != 'TERMINATED' and elapsed_seconds <= 3600 and \
                (utils.parqueteo_completed(jobflow_id) == False or \
                     utils.sumarizacion_completed(jobflow_id) == False):
                #en caso de fallo ejecutar paso b (libreria pandas)
                if utils.get_cluster_state(jobflow_id) == "WAITING" \
                        and utils.sumarizacion_completed(jobflow_id) == False \
                        and utils.sumarizacion_pending(jobflow_id) == False \
                        and utils.sumarizacion_failed(jobflow_id) == True:
                            print(u"Relaunching sumarización...")
                            launch_stepB()
                # Obtengo status del cluster:
                response = emr_connection.describe_step(ClusterId=jobflow_id,StepId=utils.get_step_id(jobflow_id))
                state = utils.get_step_state(jobflow_id)
                starting_time = response["Step"]["Status"]["Timeline"]["CreationDateTime"]
                elapsed_seconds = int(time.time())-int(starting_time.strftime('%s'))
                print(u"\r%s - Tiempo transcurrido: %d, Parqueteo: en curso=%s, finalizado=%s, Sumarización: en curso=%s, finalizado=%s"%
                        (state, elapsed_seconds, utils.parqueteo_pending(jobflow_id), utils.parqueteo_completed(jobflow_id),
                            utils.sumarizacion_pending(jobflow_id), utils.sumarizacion_completed(jobflow_id)), end='')
                #print('# -------------------------------------------------- #')
                #print(str(state) + " - Tiempo transcurrido: " + str(elapsed_seconds))
                #print("Parqueteo:    en curso=%s, finalizado=%s"% 
                #      (utils.parqueteo_pending(jobflow_id), utils.parqueteo_completed(jobflow_id)))
                #print(u"Sumarización: en curso=%s, finalizado=%s"% 
                #      (utils.sumarizacion_pending(jobflow_id), utils.sumarizacion_completed(jobflow_id)))
                #print(u'# -------------------------------------------------- #')
                time.sleep(10)                   
            # Luego de transcurrido x min, valido status y determino siguiente paso:
            if state == 'RUNNING':
                print('# -------------------------------------------------- #')
                print("Se ha superado el tiempo de espera. Se detendrá el pipeline.")
                print('# -------------------------------------------------- #\n')
                # Finalizo pipeline:
                sys.exit("Se ha superado el tiempo de espera. Se detendrá el pipeline.")
            else:
                print('# -------------------------------------------------- #')
                print("Step finalizado exitosamente.")
                print('# -------------------------------------------------- #\n')                
            print("Finalizando steps del cluster: %s"%str(jobflow_id))
            steps = list()
            for step in emr_connection.list_steps(ClusterId=jobflow_id)["Steps"]:
                if step["Status"]["State"] == "RUNNING" or step["Status"]["State"] == "PENDING":
                    steps.append(step["Id"])
            if len(steps):
                print("Finalizando steps del cluster: [%s]"%str(jobflow_id))
                print(str(steps))
                emr_connection.cancel_steps(ClusterId=jobflow_id, StepIds=steps)
            emr_connection.terminate_job_flows(JobFlowIds=[jobflow_id])
            instancias = list()
            for instance in emr_connection.list_instances(ClusterId=jobflow_id)["Instances"]:
                instancias.append(instance["Ec2InstanceId"])
            ec2 = boto3.client("ec2")
            print("Finalizando instancias:\n%s"%str(instancias))
            ec2.terminate_instances(InstanceIds=instancias)
            with open("luigi_trace", "a") as f:
                f.write("TASK4" + ',' + state + '\n')
    def output(self):        
        return luigi.LocalTarget("luigi_trace")        
#---------- Validar status del step ].

           

#----------[ Clase con funcionalidades generales:
class utils():
    
    def get_trace(file):
        # Obtiene datos del archivo de Trace y retorna un diccionario:
        dict_trace = {}
        cluster_id = ''
        with open(file) as trace:
            for line in trace:
                if cluster_id == '':
                    cluster_id =  line.strip()
                else:       
                    task, task_status = line.split(",")
                    dict_trace[task.strip()] = task_status.strip()
        return cluster_id, dict_trace

    def cluster_activo():
        # Valida si existe un cluster acitvo y retorna su ID:
        emr_connection = boto3.client('emr', region_name=default_region)
        # Obtiene listado de clusters según status:
        response = emr_connection.list_clusters(ClusterStates=['RUNNING','WAITING','STARTING']) # 'TERMINATED'
        if len(response["Clusters"]) > 0:
            # Retorna el 1er. Id del/los cluster activo(s):
            JobFlowId = response["Clusters"][0]["Id"]
            #--- Se deberia validar si el cluster tienen las app necesarias!!!
        else:
            JobFlowId = ''
        return JobFlowId

    # Obtengo status del cluster:
    def get_step_id(jobflow_id):
        emr_connection = boto3.client('emr', region_name=default_region)        
        id = None
        for step in emr_connection.list_steps(ClusterId=jobflow_id)["Steps"]:
            id = step["Id"]
        return id
                                                         
    # Obtengo status del cluster:
    def get_step_state(jobflow_id):
        emr_connection = boto3.client('emr', region_name=default_region)        
        state = "PENDING"
        for step in emr_connection.list_steps(ClusterId=jobflow_id)["Steps"]:
            state = step["Status"]["State"][0]
        return state

    #checar con el estado del cluster
    def get_cluster_state(jobflow_id):
        try:
            emr_connection = boto3.client('emr', region_name=default_region)        
            cluster_state = "PENDING"
            for cluster in emr_connection.list_clusters()["Clusters"]:
                if cluster["Id"] == jobflow_id:
                    cluster_state = cluster["Status"]["State"]
            return cluster_state
        except:
            print("We've hit aws throttle exception. Faking response for cluster_state")
            pass
        return "PENDING"
    def parqueteo_completed(jobflow_id): 
        try:
            emr_connection = boto3.client('emr', region_name=default_region)
            for step in emr_connection.list_steps(ClusterId=jobflow_id)["Steps"]:
                if step["Name"] == "parqueteo" and step["Status"]["State"] == "COMPLETED":
                    return True
                else:
                    continue
            return False
        except:
            print("We've hit aws throttle exception. Faking response for parqueteo completed")
            pass
        return False
    def parqueteo_pending(jobflow_id): 
        try:
            emr_connection = boto3.client('emr', region_name=default_region)
            for step in emr_connection.list_steps(ClusterId=jobflow_id)["Steps"]:
                if step["Name"] == "parqueteo" and step["Status"]["State"] == "PENDING":
                    return True
                else:
                    continue
            return False
        except:
            print("We've hit aws throttle exception. Faking response parqueteo_pending")
            pass
        return False
    def sumarizacion_completed(jobflow_id):
        try:
            emr_connection = boto3.client('emr', region_name=default_region)
            for step in emr_connection.list_steps(ClusterId=jobflow_id)["Steps"]:
                if (step["Name"] == "sumarizacion" or step["Name"] == "sumarizacion-2223") and \
                  step["Status"]["State"] == "COMPLETED":
                    return True
                else:
                    continue
            return False
        except:
            print("We've hit aws throttle exception. Faking response for sumarizacion_completed")
            pass
        return False
    def parqueteo_pending(jobflow_id): 
        try:
            emr_connection = boto3.client('emr', region_name=default_region)
            for step in emr_connection.list_steps(ClusterId=jobflow_id)["Steps"]:
                if step["Name"] == "parqueteo" and (\
                    step["Status"]["State"] == "RUNNING" or step["Status"]["State"] == "PENDING") :
                        return True
                else:
                    continue
            return False 
        except:
            print("We've hit aws throttle exception. Faking response for parqueteo_pending")
            pass
        return False
    def sumarizacion_pending(jobflow_id): 
        try:
            emr_connection = boto3.client('emr', region_name=default_region)
            for step in emr_connection.list_steps(ClusterId=jobflow_id)["Steps"]:
                if (step["Name"] == "sumarizacion" or step["Name"] == "sumarizacion-2223") and (\
                    step["Status"]["State"] == "RUNNING" or step["Status"]["State"] == "PENDING") :
                        return True
                else:
                    continue
            return False 
        except:
            print("We've hit aws throttle exception. Faking response for sumarizacion_pending")
            pass
        return False
    def sumarizacion_failed(jobflow_id): 
        try:
            emr_connection = boto3.client('emr', region_name=default_region)
            for step in emr_connection.list_steps(ClusterId=jobflow_id)["Steps"]:
                if (step["Name"] == "sumarizacion" or step["Name"] == "sumarizacion-2223") and \
                        step["Status"]["State"] == "FAILED":
                        return True
                else:
                    continue
            return False 
        except:
            print("We've hit aws throttle exception. Faking response for sumarizacion_pending")
            pass
        return False
#---------- Clase con funcionalidades generales ]



#----------[ Inicia pipeline:
class run_pipeline(luigi.Task):

    def requires(self):       
        return status_step()
        #return status_cluster()

#---------- Inicia pipeline ].



if __name__ == '__main__':
    luigi.run()
