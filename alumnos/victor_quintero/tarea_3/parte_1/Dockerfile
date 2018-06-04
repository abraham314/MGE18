#Instalar SO y hacer update
FROM ubuntu:16.04
USER root
RUN apt-get update

#Basandonos en la imagen creada por sequenceiq

#Instalar Java, ssh cliente y servidor
RUN apt-get -y install openjdk-8-jdk-headless openssh-server openssh-client

#Instalar Hadoop y variables de ambiente necesarias
RUN wget http://www-us.apache.org/dist/hadoop/common/hadoop-2.7.4/hadoop-2.7.4.tar.gz -O /temp/hadoop-2.7.4
RUN tar -zxvf /temp/hadoop-2.7.4 -C /usr/local
 
ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64/jre/
ENV HADOOP_PREFIX /usr/local/hadoop-2.7.4
ENV HADOOP_COMMON_HOME /usr/local/hadoop-2.7.4
ENV HADOOP_HDFS_HOME /usr/local/hadoop-2.7.4
ENV HADOOP_MAPRED_HOME /usr/local/hadoop-2.7.4
ENV HADOOP_YARN_HOME /usr/local/hadoop-2.7.4
ENV HADOOP_CONF_DIR $HADOOP_PREFIX/etc/hadoop-2.7.4
ENV YARN_CONF_DIR $HADOOP_PREFIX/etc/hadoop-2.7.4

#Copiar configuraciones 
COPY ./configuraciones/* $HADOOP_PREFIX/etc/hadoop-2.7.4/

#Levantamos servidor ssh
RUN systemctl enable ssh

#Instalar python3.5.2
RUN apt-get -y install build-essential checkinstall curl libreadline-gplv2-dev libncursesw5-dev libssl-dev libsqlite3-dev tk-dev libgdbm-dev libc6-dev libbz2-dev
RUN wget https://www.python.org/ftp/python/3.5.2/Python-3.5.2.tgz -O /tmp/Python-3.5.2.tgz
RUN tar xzf /tmp/Python-3.5.2.tgz -C /tmp/ && cd /tmp/Python-3.5.2 && ./configure && make install

#Puertos necesarios Hdfs,MapReduce
EXPOSE 50010 50020 50070 50075 50090 8020 9000 10020 19888

#Puertos necesarios Yarn
EXPOSE 8030 8031 8032 8033 8040 8042 8088 49707 2122
