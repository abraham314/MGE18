FROM ubuntu:16.04

label maintainer="metodos_gran_escala_2018"

# Instalar JDK y OpenSSH
RUN apt-get update \
 && apt-get install -y openssh-server \
    openjdk-8-jdk-headless \ 
    python3 \
    locales \
    iputils-ping \
    net-tools \
    iproute2 \
    vim \
    supervisor \
    wget \
    curl

# Ruta de instalación de HADOOP
ENV INSTALLATION_PATH /usr/local

# Versión de hadoop
ENV HADOOP_VERSION 2.7.4
ENV URL http://www-eu.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz


RUN sed -i -e 's/# en_US.UTF-8 UTF-8/en_US.UTF-8 UTF-8/' /etc/locale.gen && \
    locale-gen
ENV LANG en_US.UTF-8  
ENV LANGUAGE en_US:en  
ENV LC_ALL en_US.UTF-8     

RUN mkdir /var/run/sshd \
 && echo 'root:root' | chpasswd \
 && sed -i 's/PermitRootLogin prohibit-password/PermitRootLogin yes/' /etc/ssh/sshd_config \ 
 && sed 's@session\s*required\s*pam_loginuid.so@session optional pam_loginuid.so@g' -i /etc/pam.d/sshd \ 
 && ssh-keygen -q -N "" -t rsa -f /root/.ssh/id_rsa \
 && cat /root/.ssh/id_rsa.pub >> /root/.ssh/authorized_keys

ADD conf/ssh_config /root/.ssh/config


ENV VISIBLE now
EXPOSE 22

RUN wget ${URL} -O /tmp/hadoop-${HADOOP_VERSION}.tar.gz \
 && cd /tmp \
 && tar xzf hadoop-${HADOOP_VERSION}.tar.gz \
 && rm hadoop-${HADOOP_VERSION}.tar.gz \
 && mv hadoop-${HADOOP_VERSION} ${INSTALLATION_PATH}/hadoop


ENV HADOOP_HOME ${INSTALLATION_PATH}/hadoop
ENV HADOOP_PREFIX ${INSTALLATION_PATH}/hadoop
ENV HADOOP_COMMON_HOME ${INSTALLATION_PATH}/hadoop
ENV HADOOP_HDFS_HOME ${INSTALLATION_PATH}/hadoop
ENV HADOOP_MAPRED_HOME ${INSTALLATION_PATH}/hadoop
ENV HADOOP_YARN_HOME ${INSTALLATION_PATH}/hadoop
ENV HADOOP_CONF_DIR ${INSTALLATION_PATH}/hadoop/etc/hadoop
ENV YARN_CONF_DIR ${INSTALLATION_PATH}/hadoop/etc/hadoop
RUN sed -i '/^export JAVA_HOME/ s:.*:export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64\nexport HADOOP_PREFIX=/usr/local/hadoop\nexport HADOOP_HOME=/usr/local/hadoop\n:' $HADOOP_PREFIX/etc/hadoop/hadoop-env.sh
RUN sed -i '/^export HADOOP_CONF_DIR/ s:.*:export HADOOP_CONF_DIR=/usr/local/hadoop/etc/hadoop/:' $HADOOP_PREFIX/etc/hadoop/hadoop-env.sh

#Agregar archivos de configuracion 
ADD conf/core-site.xml          ${HADOOP_CONF_DIR}/core-site.xml
ADD conf/hdfs-site.xml          ${HADOOP_CONF_DIR}/hdfs-site.xml
ADD conf/mapred-site.xml        ${HADOOP_CONF_DIR}/mapred-site.xml
ADD conf/yarn-site.xml          ${HADOOP_CONF_DIR}/yarn-site.xml
ADD conf/capacity-scheduler.xml ${HADOOP_CONF_DIR}/capacity-scheduler.xml

# Puertos 
# HDFS
EXPOSE 9000 50070

# YARN ports
EXPOSE 8040 8042 8088 8030 8031 8032 8033


ADD startup.sh ${INSTALLATION_PATH}/bin/startup.sh
RUN chmod +x ${INSTALLATION_PATH}/bin/startup.sh

WORKDIR ${HADOOP_HOME}

CMD ["startup.sh"]











