import luigi
import json
import subprocess
from time import sleep


class CreateCluster(luigi.Task):

	def require(self):
		return None

	def output(self):
		return luigi.LocalTarget("CreateCluster.txt")

	def run(self):
		# Levantamos un cluster en AWS mediante la interfaz de linea de comando (CLI)
		createCluster = "aws emr create-cluster \
								--name 'oh_107863' \
								--release-label emr-5.13.0 \
								--applications Name=Spark Name=Hadoop Name=Zeppelin \
								--instance-groups Name=Master,InstanceGroupType=MASTER,InstanceType=m4.xlarge,InstanceCount=1 \
												  Name=Core,InstanceGroupType=CORE,InstanceType=m4.xlarge,InstanceCount=4 \
								--ec2-attributes SubnetId=subnet-7afb1631,KeyName=tutorial_key \
								--use-default-roles \
								--region us-west-2 \
								--log-uri 's3://oliabherrera/' \
								--no-termination-protected"

		output = subprocess.Popen(createCluster,
								  shell = True,
								  stdout = subprocess.PIPE)

		out = output.communicate()[0]
		out = str(out, 'utf-8')
		with self.output().open('w') as outfile:
				json.dump(out, outfile)

class StatusCreateCluster(luigi.Task):

	def requires(self):
		return CreateCluster()
		#return None

	def output(self):
		return luigi.LocalTarget("stateCreateCluster.txt")

	def run(self):
		createCluster_file = json.loads(json.load(open("CreateCluster.txt", "r")))
		clusterId = createCluster_file['ClusterId']

		statusCluster = "aws emr describe-cluster --cluster-id "
		statusCluster += clusterId

		output = subprocess.Popen(statusCluster,
		 						  shell = True,
		 						  stdout = subprocess.PIPE)

		out = output.communicate()[0]
		out = str(out, 'utf-8')
		out = json.loads(out)


		sleep(30)

		stateCreateCluster = out['Cluster']['Status']['State']

		while stateCreateCluster != "WAITING":
			sleep(30)
			output = subprocess.Popen(statusCluster,
		 						  shell = True,
		 						  stdout = subprocess.PIPE)

			out = output.communicate()[0]
			out = str(out, 'utf-8')
			out = json.loads(out)
			stateCreateCluster = out['Cluster']['Status']['State']

		else:
			with self.output().open('w') as outfile:
		 		outfile.write(stateCreateCluster)




class CSVtoParquet(luigi.Task):
	sleep(60)

	def requires(self):
		return StatusCreateCluster()

	def output(self):
		return luigi.LocalTarget("stateCSVtoParquet.txt")

	def run(self):

		createCluster_file = json.loads(json.load(open("CreateCluster.txt", "r")))
		clusterId = createCluster_file['ClusterId']

		stepFile='aws emr add-steps --cluster-id ' 
		stepFile += clusterId

		stepFile += ' --steps Type=spark,Name=Parqueteo,Args=[--deploy-mode,cluster,--master,yarn,--conf,spark.yarn.submit.waitAppCompletion=false,--num-executors,4,--executor-cores,4,s3://oliabherrera/00_luigi/parquet.py,s3://oliabherrera/00_luigi/profeco.csv,s3://oliabherrera/00_luigi/profeco.parquet],ActionOnFailure=CONTINUE' 


		output = subprocess.Popen(stepFile,
			shell = True,stdout = subprocess.PIPE)


		out = output.communicate()[0]
		out = str(out, 'utf-8')
		with self.output().open('w') as outfile:
				json.dump(out, outfile)

class StatusParquet(luigi.Task):
	def requires(self):
		return CSVtoParquet()

	def output(self):
		return luigi.LocalTarget("statusStepParquet.txt")

	def run(self):

		createStep_file = json.loads(json.load(open("stateCSVtoParquet.txt", "r")))
		stepId = createStep_file['StepIds'][0]

		createCluster_file = json.loads(json.load(open("CreateCluster.txt", "r")))
		clusterId = createCluster_file['ClusterId']

		statusStep = "aws emr describe-step --cluster-id "
		statusStep += clusterId
		statusStep += " --step-id "
		statusStep += stepId


		output = subprocess.Popen(statusStep,
		 						  shell = True,
		 						  stdout = subprocess.PIPE)

		out = output.communicate()[0]
		out = str(out, 'utf-8')
		out = json.loads(out)


		sleep(30)


		stateStepParquet = out['Step']['Status']['State']

		while stateStepParquet != "COMPLETED":
			sleep(30)
			output = subprocess.Popen(statusStep,
		 						  shell = True,
		 						  stdout = subprocess.PIPE)

			out = output.communicate()[0]
			out = str(out, 'utf-8')
			out = json.loads(out)

			stateStepParquet = out['Step']['Status']['State']

		else:
			with self.output().open('w') as outfile:
		 		outfile.write(stateStepParquet)


class ParquetCount(luigi.Task):

	sleep(300)

	def requires(self):
		return StatusParquet()

	def output(self):
		return luigi.LocalTarget("Count.txt")

	def run(self):

		sleep(30)

		createCluster_file = json.loads(json.load(open("CreateCluster.txt", "r")))
		clusterId = createCluster_file['ClusterId']

		stepFile='aws emr add-steps --cluster-id ' 
		stepFile += clusterId

		stepFile += ' --steps Type=spark,Name=Cuenta,Args=[--deploy-mode,cluster,--master,yarn,--conf,spark.yarn.submit.waitAppCompletion=false,--num-executors,4,--executor-cores,4,s3://oliabherrera/00_luigi/agg.py,s3://oliabherrera/00_luigi/profeco.parquet,s3://oliabherrera/00_luigi/cuenta.csv],ActionOnFailure=CONTINUE' 


		output = subprocess.Popen(stepFile,
			shell = True,stdout = subprocess.PIPE)


		out = output.communicate()[0]
		out = str(out, 'utf-8')
		with self.output().open('w') as outfile:
				json.dump(out, outfile)

class StatusCount(luigi.Task):
	def requires(self):
		return ParquetCount()

	def output(self):
		return luigi.LocalTarget("statusStepCount.txt")

	def run(self):

		createStep_file = json.loads(json.load(open("Count.txt", "r")))
		stepId = createStep_file['StepIds'][0]

		createCluster_file = json.loads(json.load(open("CreateCluster.txt", "r")))
		clusterId = createCluster_file['ClusterId']

		statusStep = "aws emr describe-step --cluster-id "
		statusStep += clusterId
		statusStep += " --step-id "
		statusStep += stepId


		output = subprocess.Popen(statusStep,
		 						  shell = True,
		 						  stdout = subprocess.PIPE)


		out = output.communicate()[0]
		out = str(out, 'utf-8')
		out = json.loads(out)


		sleep(30)


		stateStepParquet = out['Step']['Status']['State']

		while stateStepParquet != "COMPLETED":
			sleep(30)
			output = subprocess.Popen(statusStep,
		 						  shell = True,
		 						  stdout = subprocess.PIPE)

			out = output.communicate()[0]
			out = str(out, 'utf-8')
			out = json.loads(out)

			stateStepParquet = out['Step']['Status']['State']

		else:
			with self.output().open('w') as outfile:
		 		outfile.write(stateStepParquet)


		

class TerminateCluster(luigi.Task):

	sleep(90)

	def requires(self):
		return StatusCount()

	def output(self):
		return luigi.LocalTarget("TerminateCluster.txt")

	def run(self):
		createCluster_file = json.loads(json.load(open("CreateCluster.txt", "r")))
		clusterId = createCluster_file['ClusterId']

		statusCluster = "aws emr describe-cluster --cluster-id "
		statusCluster += clusterId

		terminateCluster = "aws emr terminate-clusters --cluster-ids "
		terminateCluster += clusterId

		output = subprocess.Popen(terminateCluster,
								  shell = True,
								  stdout = subprocess.PIPE)
		with self.output().open('w') as outfile:
				outfile.write('send termnate-cluster\n')

class StatusTerminateCluster(luigi.ExternalTask):

	def requires(self):
		return TerminateCluster()

	def output(self):
		return luigi.LocalTarget("stateTerminateCluster.txt")

	def run(self):
		createCluster_file = json.loads(json.load(open("CreateCluster.txt", "r")))
		clusterId = createCluster_file['ClusterId']

		statusCluster = "aws emr describe-cluster --cluster-id "
		statusCluster += clusterId

		output = subprocess.Popen(statusCluster,
								  shell = True,
								  stdout = subprocess.PIPE)

		out = output.communicate()[0]
		out = str(out, 'utf-8')
		out = json.loads(out)
		stateTerminateCluster = out['Cluster']['Status']['State']
		with self.output().open('w') as outfile:
				outfile.write(stateTerminateCluster)

class Descarga(luigi.Task):

	sleep(300)

	def requires(self):
		return StatusTerminateCluster()

	def output(self):
		return luigi.LocalTarget("descargaTerminada.txt")

	def run(self):

		descarga= "aws s3 sync s3://oliabherrera/00_luigi/cuenta.csv /Users/usuario/Documents/MaestriaCD/MetodosGE/prueba_luigi/resultados"

		out=subprocess.Popen(descarga,shell = True,stdout = subprocess.PIPE)
		with self.output().open('w') as outfile:
				outfile.write('termino descarga\n')




if __name__ == '__main__':
    luigi.run()





