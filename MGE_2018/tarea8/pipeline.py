import boto3
import time 
import luigi 
import os 
import sys 

class StartPipeline (luigi.Task):
	# AQUI ADENTRO VA EL CODIGO -------------------------------------------------
	client = boto3.client('emr', region_name='us-west-2')

	response = client.run_job_flow(
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

	#clusters = client.list_clusters()

	start_time = time.time()

	while(time.time() - start_time<360):
	      
	      flag=1 

	     
	clusters = client.list_clusters()

	res="" 

	if (clusters['Clusters'][0]['Status']['State'] in ['STARTING']): 
	     
	    client.terminate_job_flows(JobFlowIds=[clusters['Clusters'][0]['Id']])
	     
	    print(time.time() - start_time)

	else:
	     res="ok"
	     print('cluster listo')


	if res=="ok":

	   emr_connection = boto3.client('emr', region_name='us-west-2')
	   response = emr_connection.add_job_flow_steps(JobFlowId=clusters['Clusters'][0]['Id'], #clusters['Clusters'][0]['Id']                
		                                                 Steps=[{
		                                                     'Name': 'parqueteo',
		                                                     'ActionOnFailure': 'TERMINATE_JOB_FLOW',
		                                                     'HadoopJarStep': {
		                                                         'Jar': 'command-runner.jar',
		                                                         'Args': [
		                                                             "spark-submit",
		                                                             "s3a://tar7/parquet.py",
		                                                         ]
		                                                     }
		                                                 }]
		                                                )







	   emr_connection = boto3.client('emr', region_name='us-west-2')
	   response = emr_connection.add_job_flow_steps(JobFlowId=clusters['Clusters'][0]['Id'], #clusters['Clusters'][0]['Id']                
		                                                 Steps=[{
		                                                     'Name': 'agregado',
		                                                     'ActionOnFailure': 'TERMINATE_JOB_FLOW',
		                                                     'HadoopJarStep': {
		                                                         'Jar': 'command-runner.jar',
		                                                         'Args': [
		                                                             "spark-submit",
		                                                             "s3a://tar7/agg.py",
		                                                         ]
		                                                     }
		                                                 }]
		                                                )



        
        #falta un paso para que espere a que terminen los steps
	#client.terminate_job_flows(JobFlowIds=[clusters['Clusters'][0]['Id']])


	while(time.time() - start_time<360):
	      
	      flag=1 


	clusters = client.list_clusters()

	 

	if (clusters['Clusters'][0]['Status']['State'] in ['TERMINATING']): 
	     
	    #client.terminate_job_flows(JobFlowIds=[clusters['Clusters'][0]['Id']])
	     
	    print('cluster termino')

	else:
            sys.exit('tiempo mayor')  




	os.system("aws s3 cp --recursive s3://tar7/promedio.parquet ./")

	# AQUI ADENTRO VA EL CODIGO -------------------------------------------------

    
luigi.build ([StartPipeline ()])
