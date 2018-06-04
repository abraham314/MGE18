

## Imagen base

FROM ubuntu:16.04

## Instalamos lo básico
RUN apt-get update
RUN apt-get install sudo
RUN apt-get install nano
RUN apt-get install curl

## Instalamos JAVA
RUN apt-get install -y default-jdk


# Instalamos SSH
RUN apt-get install -y openssh-client
RUN apt-get install -y openssh-server

#Instalamos Pythn
RUN apt-get install -y python3.5

## Instalamos hadoop
RUN curl -s https://archive.apache.org/dist/hadoop/core/hadoop-2.7.3/hadoop-2.7.3.tar.gz | tar -xz -C /usr/local/
RUN sudo mv usr/home/hadoop-2.7.3 usr/local/hadoop

### Configuraciones
## Configuración de JAVA

ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64

## Configuraciones Hadoop

ENV HADOOP_PREFIX /home/usr/local/hadoop
ENV HADOOP_COMMON_HOME /home/usr/local/hadoop
ENV HADOOP_HDFS_HOME /home/usr/local/hadoop
ENV HADOOP_MAPRED_HOME /home/usr/local/hadoop
ENV HADOOP_YARN_HOME/ home/usr/local/hadoop
ENV HADOOP_CONF_DIR /home/usr/local/hadoop/etc/hadoop
ENV YARN_CONF_DIR /home/usr/local/hadoop/etc/hadoop

### Puertos
 
# SSH
EXPOSE 22

# Hdfs 
EXPOSE 50010 50020 50070 50075 50090 8020 9000

#Yarn
EXPOSE 8030 8031 8032 8033 8040 8042 8088