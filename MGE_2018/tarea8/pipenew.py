import boto3
import time 
import luigi 
import os 
import sys 

class StartPipeline(luigi.Task):

 
     def run(self):
         client = boto3.client('emr', region_name='us-west-2')
 
         cluster = client.run_job_flow(
	    Name="Boto3 test cluster",
	    ReleaseLabel='emr-5.13.0',
	    Instances={
		'MasterInstanceType': 'm4.10xlarge',
		'SlaveInstanceType': 'm4.4xlarge',
		'InstanceCount': 3,
		'KeepJobFlowAliveWhenNoSteps': True,
		'TerminationProtected': False,
		'Ec2SubnetId': 'subnet-12b14f6b',
		'Ec2KeyName': 'newspark',
	    },

	    Applications=[{'Name': 'Hadoop'},{'Name': 'Spark'},{'Name': 'Zeppelin'}],
		        Configurations=[{"Classification": "spark",
		                         "Properties": {"maximizeResourceAllocation": "true"}
		                        }],



	    VisibleToAllUsers=True,
	    JobFlowRole='EMR_EC2_DefaultRole',
	    ServiceRole='EMR_DefaultRole',
	    LogUri= "s3://aws-logs-173666346447-us-west-2/elasticmapreduce/"
	)
 
         response = client.describe_cluster(ClusterId=cluster["JobFlowId"])
         status = response["Cluster"]["Status"]["State"]
 
         with self.output().open('w') as outfile:
             outfile.write(cluster["JobFlowId"]) 

     def requires(self):
         return None
 
     def output(self):
         return luigi.LocalTarget('clus')
 
    
class espera(luigi.Task):
     
 
     def run(self):
         with self.input().open('r') as file:
             idclus=file.read()
 
         client = boto3.client('emr', region_name='us-west-2')
         

         dd = client.describe_cluster(ClusterId = idclus)
         st=dd["Cluster"]["Status"]["State"]
        
         start_time = time.time()
 
         while st == 'STARTING' and (time.time() - start_time) < 600:
             dd = client.describe_cluster(ClusterId = idclus)
             st = dd["Cluster"]["Status"]["State"]

             print(st)
            
             time.sleep(60)
 
         if (time.time() - start_time) > 600:
             print("tiempo mayor")
             fst='SHUTTING_DOWN'
             sys.exit()

         else:
             fst='SUBMITTING_STEP' 
 

 
         with self.output().open('w') as outfile:
             outfile.write(str(idclus+','+fst)) 

     def requires(self):
         return StartPipeline()
 
     def output(self):
         return luigi.LocalTarget('wt')
 
 
class parquetear(luigi.Task):
 
    def run(self):
        with self.input().open('r') as file_id:
            idclus,st = file_id.read().split(',')
 
        client = boto3.client("emr", region_name='us-west-2')
 

                                     
        step={'Name':'parqueteo','ActionOnFailure':'TERMINATE_JOB_FLOW','HadoopJarStep':{ 'Jar':'command-runner.jar','Args':["spark-submit","s3a://tar7/parquet.py"]}}
        
 
        lista = client.add_job_flow_steps(JobFlowId=idclus, Steps=[step])
 
        st = client.describe_step(ClusterId = idclus,StepId = lista["StepIds"][0])
 
        st = st["Step"]["Status"]["State"]
 
        print(st)
 
        while st in ['PENDING','RUNNING']:
            
            st = client.describe_step(ClusterId = idclus,StepId = lista["StepIds"][0])
            st = st["Step"]["Status"]["State"]
            time.sleep(30)
 
        print(st)
 
        with self.output().open('w') as outfile:
            outfile.write(str(idclus+','+lista["StepIds"][0])+','+st) 

    def requires(self):
        return espera()
 
    def output(self):
        return luigi.LocalTarget('pqt')


 
class agg(luigi.Task):


 
     def run(self):
         with self.input().open('r') as file:
             idclus, idstp, pst = file.read().split(',')
 
         client = boto3.client("emr", region_name='us-west-2')
 


         step={'Name':'agregado','ActionOnFailure':'TERMINATE_JOB_FLOW','HadoopJarStep':{ 'Jar':'command-runner.jar','Args':["spark-submit","s3a://tar7/agg.py"]}}
 

 
         lista = client.add_job_flow_steps(JobFlowId=idclus, Steps=[step])
 
         st = client.describe_step(ClusterId = idclus,StepId = lista["StepIds"][0])
 
         st = st["Step"]["Status"]["State"]
 
         print(st)
 
         while st in ['PENDING','RUNNING']:
             #print(st)
             st = client.describe_step(ClusterId = idclus,StepId = lista["StepIds"][0])
             st = st["Step"]["Status"]["State"]
             time.sleep(60)
 
         print(st)
 
         with self.output().open('w') as outfile:
             outfile.write(str(idclus+','+lista["StepIds"][0])+','+st)

     def  requires(self):
         return parquetear()
 
     def output(self):
         return luigi.LocalTarget('ag')
 
class descarga(luigi.Task):

     def run(self):
         with self.input().open('r') as file:
             idclus, st, ag= file.read().split(',')
 
         os.system("aws s3 cp --recursive s3://tar7/promedio.parquet ./")
 
         with self.output().open('w') as outfile:
             outfile.write(str(idclus+','+st+','+ag))
     
     def requires(self):
         return agg()
 
     def output(self):
         return(luigi.LocalTarget("dwl"))
 
 
class mata(luigi.Task):
  
     def run(self):
         with self.input().open('r') as file:
             idclus, idstp, ag = file.read().split(',')
 
         client = boto3.client('emr', region_name='us-west-2')
         
         response = client.terminate_job_flows(JobFlowIds=[idclus])
         dd = client.describe_cluster(ClusterId = idclus)
         st=dd["Cluster"]["Status"]["State"]
         
         start_time = time.time()
 
         while st == 'TERMINATING' and (time.time() - start_time) < 600:
             dd = client.describe_cluster(ClusterId = idclus)
             st = dd["Cluster"]["Status"]["State"]

             print(st)

             time.sleep(60)
 
 
         if (time.time() - start_time) > 600:
             print("tiempo mayor")
             time.sleep(10)
             
 
         print(response)
 
         with self.output().open('w') as outfile:
             outfile.write(idclus)

     def requires(self):
         return descarga()
 
     def output(self):
         return(luigi.LocalTarget("mt")) 
 
class Pipeline(luigi.Task):
     def requires(self):
         return mata()
 
if __name__ == '__main__':
    luigi.run()
