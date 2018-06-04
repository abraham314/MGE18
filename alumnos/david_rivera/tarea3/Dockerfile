#Tomamos ubuntu 16.04 como imagen base
FROM ubuntu:16.04

#Actualizar las referencias de los repositorios
RUN apt-get update

#instalar java, ssh y ajustar env basandome en la imagen creada por sequenceiq
RUN apt-get -y install openjdk-8-jdk-headless openssh-server openssh-client 

#Esto no es necesario, sin embargo es util si necesitaras interactuar con contenedor/
RUN apt-get -y install nano inetutils-ping

#instalar python3.5.2 y sus dependencias
RUN apt-get -y install build-essential checkinstall curl
RUN apt-get -y install libreadline-gplv2-dev libncursesw5-dev libssl-dev libsqlite3-dev tk-dev libgdbm-dev libc6-dev libbz2-dev
RUN wget https://www.python.org/ftp/python/3.5.2/Python-3.5.2.tgz -O /tmp/Python-3.5.2.tgz
RUN tar xzf /tmp/Python-3.5.2.tgz -C /tmp/  
RUN cd /tmp/Python-3.5.2 && ./configure && make install

#Descargar y descomprimir hadoop y yarn
RUN wget http://www-eu.apache.org/dist/hadoop/common/hadoop-2.7.4/hadoop-2.7.4.tar.gz -O /tmp/hadoop-2.7.4
RUN tar -zxvf /tmp/hadoop-2.7.4 -C /usr/local/
#RUN ln -sf /usr/local/hadoop-2.7.4 /usr/local/hadoop 


#Declaramos las variables de ambiente de hadoop-yarn 
ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64/jre/
ENV HADOOP_PREFIX /usr/local/hadoop-2.7.4
ENV HADOOP_COMMON_HOME /usr/local/hadoop-2.7.4
ENV HADOOP_HDFS_HOME /usr/local/hadoop-2.7.4
ENV HADOOP_MAPRED_HOME /usr/local/hadoop-2.7.4
ENV HADOOP_YARN_HOME /usr/local/hadoop-2.7.4
ENV HADOOP_CONF_DIR $HADOOP_PREFIX/etc/hadoop-2.7.4
ENV YARN_CONF_DIR $HADOOP_PREFIX/etc/hadoop-2.7.4

#Copiamos archivos de configuraciones *.xml y masters-slaves de maquina local a imagen
COPY ./configs/*.xml $HADOOP_PREFIX/etc/hadoop-2.7.4/
COPY ./configs/masters $HADOOP_PREFIX/etc/hadoop-2.7.4/
COPY ./configs/slaves $HADOOP_PREFIX/etc/hadoop-2.7.4/

#Creamos los directorios para logs
RUN echo "JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64/jre/" >> /etc/environment
RUN mkdir -p /usr/local/hadoop_work/hdfs/namenode
RUN mkdir -p /usr/local/hadoop_work/hdfs/datanode
RUN mkdir -p /usr/local/hadoop_work/hdfs/namesecondary

#Especificamos a bash que inicie servicio ssh cada que levante una sesion
RUN echo "service ssh start" >> /root/.bashrc

# Hdfs ports
EXPOSE 50010 50020 50070 50075 50090 8020 9000

# Mapred ports
EXPOSE 10020 19888

#Yarn ports
EXPOSE 8030 8031 8032 8033 8040 8042 8088

#Other ports
EXPOSE 49707 2122
