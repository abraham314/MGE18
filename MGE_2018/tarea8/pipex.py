import luigi
import os
import boto3
import time
import sys
 
os.remove('tmp_cluster') if os.path.exists('tmp_cluster') else None
os.remove('tmp_waiting') if os.path.exists('tmp_waiting') else None
os.remove('tmp_terminate') if os.path.exists('tmp_terminate') else None
os.remove('tmp_parquete') if os.path.exists('tmp_parquete') else None
os.remove('tmp_agg') if os.path.exists('tmp_agg') else None
os.remove('tmp_download') if os.path.exists('tmp_download') else None
 
 
 
class InitializeEmrCluster(luigi.Task):
     cluster_name = luigi.Parameter(default='an_51556')
     ec2_keyname = luigi.Parameter(default='newspark')	#tu llave.pem sin el .pem, no hace falta estar en donde la tienes
     log_url = luigi.Parameter(default='s3://aws-logs-173666346447-us-west-2/elasticmapreduce/')	#carpeta previamente creada en S3 para depositar logs
 
     def requires(self):
         return None
 
     def output(self):
         return luigi.LocalTarget('tmp_cluster')
 
     def run(self):
         client = boto3.client('emr', region_name='us-west-2')
 
         cluster_info = client.run_job_flow(
             Name="an_51556",
             ReleaseLabel='emr-5.13.0',
             LogUri="s3://aws-logs-173666346447-us-west-2/elasticmapreduce/",	#carpeta previamente creada en S3 para depositar logs
             Applications=[
                 {'Name': 'Spark'},
                 {'Name': 'Zeppelin'}
                 ],
             Instances={
                 'MasterInstanceType': 'm4.10xlarge',
                 'SlaveInstanceType': 'm4.4xlarge',
                 'InstanceCount': 3,
                 'KeepJobFlowAliveWhenNoSteps': True,
                 'TerminationProtected': False,
                 'Ec2SubnetId': 'subnet-12b14f6b',	#ID del subnet que vayas a usar de tu cuenta
                 'Ec2KeyName': 'newspark',	#tu llave.pem sin el .pem, no hace falta estar en donde la tienes
             },
             VisibleToAllUsers=True,
             JobFlowRole='EMR_EC2_DefaultRole',
             ServiceRole='EMR_DefaultRole',
             Configurations=[
                     {
 
                         "Classification": "spark",
 
                         "Properties": {
 
                         "maximizeResourceAllocation": "true"
 
                      }
 
                     }
 
                    ]
         )
 
         response = client.describe_cluster(ClusterId=cluster_info["JobFlowId"])
         status = response["Cluster"]["Status"]["State"]
 
         with self.output().open('w') as outfile:
             outfile.write(cluster_info["JobFlowId"])
 
    
class WaitEmrCluster(luigi.Task):
     
     def requires(self):
         return InitializeEmrCluster()
 
     def output(self):
         return luigi.LocalTarget('tmp_waiting')
 
     def run(self):
         with self.input().open('r') as file:
             cluster_id=file.read()
 
         client = boto3.client('emr', region_name='us-west-2')
 
         status='STARTING'
         my_time = 0
 
         while status == 'STARTING' and my_time < 600:
             description = client.describe_cluster(ClusterId = cluster_id)
             status = description["Cluster"]["Status"]["State"]
             starting_time = description["Cluster"]["Status"]["Timeline"]["CreationDateTime"]
             my_time = int(time.time()) - int(starting_time.strftime('%s'))
             print(status)
             #print(my_time)
             time.sleep(30)
 
         if my_time > 600:
             print("-----------------TIEMPO LIMITE CREACION DEL CLUSTER: CANCELANDO PIPELINE------------------")
             time.sleep(10)
             sys.exit()
 
         if status == 'STARTING':
             final_status = 'SHUTTING_DOWN'
             #yield TerminateEmrCluster()
         else:
             final_status = 'SUBMITTING_STEP'
 
         with self.output().open('w') as outfile:
             outfile.write(str(cluster_id+','+final_status))
 
 
class DataFrameToParquete(luigi.Task):
 
    def requires(self):
        return WaitEmrCluster()
 
    def output(self):
        return luigi.LocalTarget('tmp_parquete')
 
    def run(self):
        with self.input().open('r') as file_id:
            cluster_id,status = file_id.read().split(',')
 
        client = boto3.client("emr", region_name='us-west-2')
 
        step_args = ["spark-submit",
                      "s3a://tar7/parquet.py"]	#Ubicación exacta del parquete.py en tu S3
                                     
        step={'Name':'parqueteo','ActionOnFailure':'TERMINATE_JOB_FLOW','HadoopJarStep':{ 'Jar':'command-runner.jar','Args':step_args}}
        
 
        action = client.add_job_flow_steps(JobFlowId=cluster_id, Steps=[step])
 
        step_status = client.describe_step(ClusterId = cluster_id,StepId = action["StepIds"][0])
 
        step_status = step_status["Step"]["Status"]["State"]
 
        print(step_status)
 
        while step_status == 'PENDING' or step_status == 'RUNNING':
            print(step_status)
            step_status = client.describe_step(ClusterId = cluster_id,StepId = action["StepIds"][0])
            step_status = step_status["Step"]["Status"]["State"]
            time.sleep(30)
 
        print(step_status)
 
        with self.output().open('w') as outfile:
            outfile.write(str(cluster_id+','+action["StepIds"][0])+','+step_status)
 
class AggProcessing(luigi.Task):
     def  requires(self):
         return DataFrameToParquete()
 
     def output(self):
         return luigi.LocalTarget('tmp_agg')
 
     def run(self):
         with self.input().open('r') as file:
             cluster_id, step_id, parquet_status = file.read().split(',')
 
         client = boto3.client("emr", region_name='us-west-2')
 
         step_args = ["spark-submit",
                      "s3a://tar7/agg.py"]	#Ubicación exacta del agg.py en tu S3

         step={'Name':'agregado','ActionOnFailure':'TERMINATE_JOB_FLOW','HadoopJarStep':{ 'Jar':'command-runner.jar','Args':step_args}}
 

 
         action = client.add_job_flow_steps(JobFlowId=cluster_id, Steps=[step])
 
         step_status = client.describe_step(ClusterId = cluster_id,StepId = action["StepIds"][0])
 
         step_status = step_status["Step"]["Status"]["State"]
 
         print(step_status)
 
         while step_status == 'PENDING' or step_status == 'RUNNING':
             print(step_status)
             step_status = client.describe_step(ClusterId = cluster_id,StepId = action["StepIds"][0])
             step_status = step_status["Step"]["Status"]["State"]
             time.sleep(30)
 
         print(step_status)
 
         with self.output().open('w') as outfile:
             outfile.write(str(cluster_id+','+action["StepIds"][0])+','+step_status)
 
class DownloadS3(luigi.Task):
     
     def requires(self):
         return AggProcessing()
 
     def output(self):
         return(luigi.LocalTarget("tmp_download"))
 
     def run(self):
         with self.input().open('r') as file:
             cluster_id, step_id, agg_status = file.read().split(',')
 
         os.system("aws s3 cp --recursive s3://tar7/promedio.parquet ./")	#Carpeta que creas en agg.py para depositar resultado
 
         with self.output().open('w') as outfile:
             outfile.write(str(cluster_id+','+step_id+','+agg_status))
 
 
class TerminateEmrCluster(luigi.Task):
 
     def requires(self):
         return DownloadS3()
 
     def output(self):
         return(luigi.LocalTarget("tmp_terminate"))
 
     def run(self):
         with self.input().open('r') as file:
             cluster_id, step_id, agg_status = file.read().split(',')
 
         client = boto3.client('emr', region_name='us-west-2')
         
         response = client.terminate_job_flows(JobFlowIds=[cluster_id])
 
         status='TERMINATING'
         my_time = 0
 
         while status == 'TERMINATING' and my_time < 600:
             description = client.describe_cluster(ClusterId = cluster_id)
             status = description["Cluster"]["Status"]["State"]
             starting_time = description["Cluster"]["Status"]["Timeline"]["CreationDateTime"]
             my_time = int(time.time()) - int(starting_time.strftime('%s'))
             print(status)
             print(my_time)
             time.sleep(30)
 
 
         if my_time > 600:
             print("-----------------TIEMPO LIMITE DESTRUCCION DEL CLUSTER: CANCELANDO PIPELINE------------------")
             time.sleep(10)
             #sys.exit()
 
         print(response)
 
         with self.output().open('w') as outfile:
             outfile.write(cluster_id)
 
class Pipeline(luigi.Task):
     def requires(self):
         return TerminateEmrCluster()
 
if __name__ == '__main__':
    luigi.run()
