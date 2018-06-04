# Daniel Sharp, Cristian Challu y Francisco Bahena
FROM ubuntu:16.04

RUN apt-get update

## ESTE Dockerfile se configura de tal forma que ds138176 siempre es el master y cc120652 y pb123084 son los esclavos.

# Instalación de Java 
RUN apt-get install -y default-jdk
ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64/jre
ENV PATH $PATH:$JAVA_HOME/bin
RUN useradd -m hadoop && echo "hadoop:hadoop" | chpasswd && adduser hadoop sudo

# Instalación de Hadoop y configuración de variables de entorno
RUN apt-get install -y curl tar sudo
COPY hadoop-2.7.4.tar.gz /
RUN tar -xzf hadoop-2.7.4.tar.gz -C /home/hadoop
ENV HADOOP_PREFIX /home/hadoop/hadoop 
ENV HADOOP_COMMON_HOME /home/hadoop/hadoop 
ENV HADOOP_HDFS_HOME /home/hadoop/hadoop 
ENV HADOOP_MAPRED_HOME /home/hadoop/hadoop 
ENV HADOOP_YARN_HOME /home/hadoop/hadoop 
ENV HADOOP_CONF_DIR /home/hadoop/hadoop/etc/hadoop 
ENV YARN_CONF_DIR $HADOOP_PREFIX/etc/hadoop


# Instalación de Python
RUN apt-cache show python3
RUN apt-get install -y python3.5

# Instalación de Nano para poder modificar algunos de los archivos
RUN apt-get install -y nano

# Instalación de SSH para poder correr Hadoop
RUN apt-get install -y openssh-server ssh

# Configuración de los archivos de Hadoop
# Core Site
RUN sed -i "s|<configuration>|<configuration>\n\t<property>\n\t\t<name>fs.default.name</name>\n\t\t<value>hdfs://ds138176:9000</value>\n\t</property>|g" /home/hadoop/hadoop/etc/hadoop/core-site.xml

# HDFS
RUN sed -i "s|<configuration>|<configuration>\n\t<property>\n\t\t<name>dfs.namenode.name.dir</name>\n\t\t<value>/home/hadoop/data/nameNode</value>\n\t</property>\n\t<property>\n\t\t<name>dfs.datanode.data.dir</name>\n\t\t<value>/home/hadoop/data/dataNode</value>\n\t</property>\n\t<property>\n\t\t<name>dfs.replication</name>\n\t\t<value>2</value>\n\t</property>|g" /home/hadoop/hadoop/etc/hadoop/hdfs-site.xml

# Set YARN as Job Scheduler
RUN mv /home/hadoop/hadoop/etc/hadoop/mapred-site.xml.template /home/hadoop/hadoop/etc/hadoop/mapred-site.xml
RUN sed -i "s|<configuration>|<configuration>\n\t<property>\n\t\t<name>mapreduce.framework.name</name>\n\t\t<value>yarn</value>\n\t</property>\n\t<property>\n\t\t<name>yarn.app.mapreduce.am.resource.mb</name>\n\t\t<value>1024</value>\n\t</property>\n\t<property>\n\t\t<name>mapreduce.map.memory.mb</name>\n\t\t<value>512</value>\n\t</property>\n\t<property>\n\t\t<name>mapreduce.reduce.memory.mb</name>\n\t\t<value>512</value>\n\t</property>|g" /home/hadoop/hadoop/etc/hadoop/mapred-site.xml

# Configure YARN
RUN sed -i "s|<configuration>|<configuration>\n\t<property>\n\t\t<name>yarn.acl.enable</name>\n\t\t<value>0</value>\n\t</property>\n\t<property>\n\t\t<name>yarn.resourcemanager.hostname</name>\n\t\t<value>ds138176</value>\n\t</property>\n\t<property>\n\t\t<name>yarn.nodemanager.aux-services</name>\n\t\t<value>mapreduce_shuffle</value>\n\t</property>\n\t<property>\n\t\t<name>yarn.nodemanager.resource.memory-mb</name>\n\t\t<value>3072</value>\n\t</property>\n\t<property>\n\t\t<name>yarn.scheduler.maximum-allocation-mb</name>\n\t\t<value>3072</value>\n\t</property>\n\t<property>\n\t\t<name>yarn.scheduler.minimum-allocation-mb</name>\n\t\t<value>256</value>\n\t</property>\n\t<property>\n\t\t<name>yarn.nodemanager.vmem-check-enabled</name>\n\t\t<value>false</value>\n\t</property>|g" /home/hadoop/hadoop/etc/hadoop/yarn-site.xml

# Configure slaves
RUN sed -i "s|localhost|cc120652\npb123084|g" /home/hadoop/hadoop/etc/hadoop/slaves

RUN sudo echo 'PATH="/home/hadoop/hadoop/bin:/home/hadoop/hadoop/sbin/:$PATH"' >> /home/hadoop/.profile
#RUN source /home/hadoop/.profile
RUN sed -i "s|export JAVA_HOME=${JAVA_HOME}|export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64/jre|g" /home/hadoop/hadoop/etc/hadoop/hadoop-env.sh

RUN chmod 777 /home/hadoop/hadoop

USER hadoop
ENV PATH $PATH:$JAVA_HOME/bin

EXPOSE 22

# Posteriormente se debe ingresar a la imagen con el comnado docker run -it --network host <nombre_imagen>
# Dentro del docker se deben enviar las llaves ssh del master a los esclavos para poder levantar el cluster



