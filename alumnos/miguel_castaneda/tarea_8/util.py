import os
import subprocess
import json
import time

# Parametros
# 0 ClusterName
# 1 SubnetId
# 2 KeyName
# 3 Region
# 4 log-uri
CMD_CREA_CLUSTER = "aws emr create-cluster --name \"{0}\" --release-label emr-5.13.0 " \
                   "--applications Name=Spark Name=Hadoop Name=Zeppelin " \
                   "--instance-groups Name=Master,InstanceGroupType=MASTER," \
                   "InstanceType=m3.xlarge,InstanceCount=1 Name=Core,InstanceGroupType=CORE," \
                   "InstanceType=m3.xlarge,InstanceCount=2 --ec2-attributes SubnetId={1}," \
                   "KeyName={2} --use-default-roles " \
                   "--region {3} " \
                   "--log-uri \"{4}\" " \
                   "--no-termination-protected --configurations file://emr-config.json "
# Parametros
# ClusterId
CMD_TERMINA_CLUSTER = "aws emr terminate-clusters --cluster-ids {0}"
# Parametros
CMD_CREA_STEP = "aws emr add-steps --cluster-id {0} --steps Type=spark,Name=StepSpark," \
                "Args=[--deploy-mode,cluster,--master,yarn," \
                "--conf,spark.yarn.submit.waitAppCompletion=true," \
                "{1}," \
                "{2}," \
                "{3}]," \
                "ActionOnFailure=CONTINUE"
CMD_LISTA_CLUSTERS_ACTIVOS = "aws emr list-clusters --active"
# Parametros
# CarpetaOrigen
# CarpetaDestino
CMD_SINCRONIZA_S3_ORIGEN_DESTINO = "aws s3 sync {0} {1} "
# Parametros
# ClusterId
# StepId
CMD_ESTADO_STEP_ACTIVO = "aws emr  list-steps --cluster-id  {0} --step-ids {1}"
# Parametros, se obtiene la lista de steps en el cluster
# ClusterId
CMD_ESTADO_STEP = "aws emr  list-steps --cluster-id  {0}"
# Parametros
# nombreBucket
CMD_LISTA_S3_BUCKET = "aws s3 list {0}"
# Parametros
# idCluster, estado del cluster sin importar si esta activo
CMD_DESC_CLUSTER = "aws emr describe-cluster --cluster-id {0}"
# Parametros
# archivo origen
# archivo desinto
CMD_CP_S3_ORIGEN_DESTINO = "aws s3 cp {0} {1} "


def ejecuta_cmd(cmd):
    resultado = ""
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=True)
    (output, err) = p.communicate()
    p_status = p.wait()

    if p_status == 0:
        if len(output) > 0:
            try:
                resultado = json.loads(output)
            except:
                resultado = ""
    return resultado


def crea_cluster(clusterName, subnetId, keyName, region, logUri):
    idCluster = ""
    cmd = CMD_CREA_CLUSTER.format(clusterName, subnetId, keyName, region, logUri)    
    print(cmd)
    resultado = ejecuta_cmd(cmd)    
    print(resultado)
    try:
        idCluster = resultado['ClusterId']
        print(idCluster)
    except:
        idCluster = ""
    return idCluster


def termina_cluster(idCluster):
    # No genera resultado
    cmd = CMD_TERMINA_CLUSTER.format(idCluster)
    print(cmd)
    ejecuta_cmd(cmd)


def estadoCluster(idCluster):

    # Posibles estados:
    # STARTING
    # BOOTSTRAPPING
    # RUNNING
    # WAITING
    # TERMINATING
    # TERMINATED
    # TERMINATED_WITH_ERRORS

    cmd = CMD_DESC_CLUSTER.format(idCluster)    
    estadoCluster = ""
    resultado = ejecuta_cmd(cmd)    
    if len(resultado) > 0:
        estadoCluster = resultado['Cluster']['Status']['State']
    return estadoCluster

def copiaArchivoS3(archivoOrigen,archivoDestino):
    cmd = CMD_CP_S3_ORIGEN_DESTINO.format(archivoOrigen, archivoDestino)
    #print(cmd)
    resultado = ejecuta_cmd(cmd)
    #print(resultado)


def creaStep(idCluster,nombreScript,entrada,salida):
    idStep  = ""
    cmd = CMD_CREA_STEP.format(idCluster,nombreScript,entrada,salida)
    #print(cmd)
    resultado = ejecuta_cmd(cmd)
    #print(resultado)
    try:
        idStep = resultado['StepIds'][0]
    except:
        idStep = ""
    return idStep


def estadoStep(idCluster,idStep):
    estado = ""
    cmd = CMD_ESTADO_STEP_ACTIVO.format(idCluster,idStep)    
    resultado = ejecuta_cmd(cmd)    
    try:
        estado = resultado['Steps'][0]['Status']['State']        
    except:
        estado = ""
    return estado

def sincronizaCarpeta(origen, destino):
    cmd = CMD_SINCRONIZA_S3_ORIGEN_DESTINO.format(origen,destino)    
    resultado = ejecuta_cmd(cmd)


#crea_cluster("itam", "subnet-ade70de6", "mno_key", "us-west-2", "s3://metodosgranescala/tarea8")