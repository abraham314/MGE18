import luigi
import boto3
import botocore
import logging
import time
import os
import csv
import json
import random
import requests
import subprocess

class levantarcluster(luigi.Task):
    
    region = luigi.Parameter()
    keyid = luigi.Parameter()
    accesskey = luigi.Parameter()
    carpetas3 = luigi.Parameter()
    keyname = luigi.Parameter()
    subnet = luigi.Parameter()
    
    def run(self):
        
        start_time = time.time() #tiempo de inicio del cluster
        
        connection = boto3.client(
            'emr',
            region_name = self.region,
            aws_access_key_id = self.keyid,
            aws_secret_access_key = self.accesskey,
        )    
        #levantamos el cluster con todo y configuraciones
        cluster_id = connection.run_job_flow(
            Name='LizMarianaNivi',
            LogUri=self.carpetas3,
            ReleaseLabel='emr-5.13.0',
            Instances={
                'InstanceGroups': [
                    {
                        'Name': "Master nodes",
                        'Market': 'ON_DEMAND',
                        'InstanceRole': 'MASTER',
                        'InstanceType': 'm3.xlarge',
                        'InstanceCount': 1,
                    },
                    {
                        'Name': "Slave nodes",
                        'Market': 'ON_DEMAND',
                        'InstanceRole': 'CORE',
                        'InstanceType': 'm4.large',
                        'InstanceCount': 2,}
                ],
                'Ec2KeyName': self.keyname,
                'KeepJobFlowAliveWhenNoSteps': True,
                'TerminationProtected': False,
                'Ec2SubnetId': self.subnet,
            },
            Steps=[],
            VisibleToAllUsers=True,
            JobFlowRole='EMR_EC2_DefaultRole',
            ServiceRole='EMR_DefaultRole',
            Tags=[],
            Applications=[
                {
                    'Name': 'Spark',
                    'Name' : 'Hadoop',
                    'Name' : 'Zeppelin'
                }],
            Configurations = [
                {
                    "Classification": "spark",
                    "Properties": {
                        "maximizeResourceAllocation": "true"
                    }
                },
                {
                    "Classification": "spark-env",
                    "Configurations": [
                        {
                            "Classification": "export",
                            "Properties": {
                                "PYSPARK_PYTHON": "/usr/bin/python3"
                            }
                        }]
                }]
        )

        jobid = cluster_id['JobFlowId']

        state = connection.describe_cluster(ClusterId=jobid)['Cluster']['Status']['State'] 
        tiempo = time.time() - start_time
        
        #10 minutos y lo termina
        #monitoreo
        while(state == 'STARTING' and tiempo < 600):
            time.sleep(60)
            state = connection.describe_cluster(ClusterId=jobid)['Cluster']['Status']['State'] 
            tiempo = time.time() - start_time
            
        if(state == 'STARTING' and tiempo > 600):
            connection.terminate_job_flows(JobFlowIds=[jobid])
            
        if(state == 'WAITING'):
            with open('clusterid','w') as f:
                f.write(jobid)
        
    def output(self):
            return luigi.LocalTarget('clusterid')

class parquet(luigi.Task):
    
    region = luigi.Parameter()
    keyid = luigi.Parameter()
    accesskey = luigi.Parameter()
    carpetas3 = luigi.Parameter()
    keyname = luigi.Parameter()
    subnet = luigi.Parameter()
    
    def requires(self):
        return levantarcluster(self.region, self.keyid, self.accesskey, self.carpetas3, self.keyname, self.subnet)
    
    def output(self):
            return luigi.LocalTarget('step1')
        
    def run(self):
       #step parquet si falla termina el cluster 
        with self.input().open('r') as file_id:
            jobid = file_id.read()
            
        connection = boto3.client('emr',
                                  region_name = self.region,
                                  aws_access_key_id= self.keyid,
                                  aws_secret_access_key= self.accesskey)
        
        state = connection.describe_cluster(ClusterId=jobid)['Cluster']['Status']['State'] 

        step_args = ["spark-submit","s3://luigiclasemge/parquet.py"]
        step = {"Name": "parquet" + time.strftime("%Y%m%d-%H:%M"),
                'ActionOnFailure': 'TERMINATE_CLUSTER',
                'HadoopJarStep': {
                    'Jar': 'command-runner.jar',
                    'Args': step_args
                    }
                }
                        
        action = connection.add_job_flow_steps(JobFlowId=jobid, Steps=[step])
        
        stepid = action['StepIds'][0]
        state_step = connection.describe_step(ClusterId = jobid, StepId=stepid)['Step']['Status']['State']
        #monitoreo
        while(state_step != 'COMPLETED'):
            time.sleep(20)
            state_step = connection.describe_step(ClusterId = jobid, StepId=stepid)['Step']['Status']['State']
        
        if(state_step == 'COMPLETED'):
            with open('step1','w') as f:
                f.write(jobid)
                
class agg(luigi.Task):
    
    region = luigi.Parameter()
    keyid = luigi.Parameter()
    accesskey = luigi.Parameter()
    carpetas3 = luigi.Parameter()
    keyname = luigi.Parameter()
    subnet = luigi.Parameter()
    
    def requires(self):
        return parquet(self.region, self.keyid, self.accesskey, self.carpetas3, self.keyname, self.subnet)
    
    def output(self):
            return luigi.LocalTarget('step2')
        
    def run(self):
        #step2 agg si falla termina el cluster

        with self.input().open('r') as file_id:
            jobid = file_id.read()
            
        connection = boto3.client('emr',
                                  region_name = self.region,
                                  aws_access_key_id= self.keyid,
                                  aws_secret_access_key= self.accesskey)
        
        state = connection.describe_cluster(ClusterId=jobid)['Cluster']['Status']['State'] 

        step_args = ["spark-submit","s3://luigiclasemge/agg.py"]
        step = {"Name": "agg" + time.strftime("%Y%m%d-%H:%M"),
                'ActionOnFailure': 'TERMINATE_CLUSTER',
                'HadoopJarStep': {
                    'Jar': 'command-runner.jar',
                    'Args': step_args
                    }
                }
                        
        action = connection.add_job_flow_steps(JobFlowId=jobid, Steps=[step])
        
        stepid = action['StepIds'][0]
        state_step = connection.describe_step(ClusterId = jobid, StepId=stepid)['Step']['Status']['State']
        #monitoreo
        while(state_step != 'COMPLETED'):
            time.sleep(20)
            state_step = connection.describe_step(ClusterId = jobid, StepId=stepid)['Step']['Status']['State']
        #terminamos el cluster una vez que termina el step de agg
        if(state_step == 'COMPLETED'):
            connection.terminate_job_flows(JobFlowIds=[jobid])
            with open('step2','w') as f:
                f.write(state_step)                
                
class MyS3File(luigi.ExternalTask):
    
    downloadComplete = False
    
    region = luigi.Parameter()
    keyid = luigi.Parameter()
    accesskey = luigi.Parameter()
    carpetas3 = luigi.Parameter()
    keyname = luigi.Parameter()
    subnet = luigi.Parameter()
    
    def requires(self):
        return agg(self.region, self.keyid, self.accesskey, self.carpetas3, self.keyname, self.subnet)
    
    def run(self):
        
        with self.input().open('r') as file_id:
            estado= file_id.read()
        #descargamos el resultado
        if(estado == 'COMPLETED'):
            os.system("aws s3 sync s3://luigiclasemge/salida ./salida/")
            self.downloadComplete = True
            
    def complete(self):
        return self.downloadComplete
    
class pipeline(luigi.Task):
    
    region = luigi.Parameter()
    keyid = luigi.Parameter()
    accesskey = luigi.Parameter()
    carpetas3 = luigi.Parameter()
    keyname = luigi.Parameter()
    subnet = luigi.Parameter()
    
    def requires(self):
        return MyS3File(self.region, self.keyid, self.accesskey, self.carpetas3, self.keyname, self.subnet)
    
