From centos:7

MAINTAINER francis francis@francisdu.com

USER root

# Install dependencies
RUN yum update -y && yum install -y wget openssh vim \
    openssh-clients java-1.8.0-openjdk \
    java-1.8.0-openjdk-devel  java-1.8.0-openjdk-headless

# Download Qlick SQL package
RUN  wget https://github.com/Qihoo360/Quicksql/releases/download/v0.7.0/quicksql-0.7.0.tar.gz &&  \
     tar -xzvf quicksql-0.7.0.tar.gz -C /usr/local

# Download Spark package
RUN wget https://archive.apache.org/dist/spark/spark-2.2.0/spark-2.2.0-bin-hadoop2.7.tgz &&  \
    tar -xzvf spark-2.2.0-bin-hadoop2.7.tgz -C /usr/local

# Setting environments
ENV JAVA_HOME /usr/lib/jvm/java-1.8.0-openjdk
ENV QSQL_HOME /usr/local/qsql-0.7.0
ENV SPARK_HOME /usr/local/spark-2.2.0-bin-hadoop2.7
ENV PATH $JAVA_HOME/bin:$SPARK_HOME/bin:$SPARK_HOME/sbin:$QSQL_HOME/bin:$PATH
RUN export PATH QSQL_HOME SPARK_HOME

WORKDIR /usr/local/qsql-0.7.0

EXPOSE 4040 8080

CMD ["/usr/sbin/init"]