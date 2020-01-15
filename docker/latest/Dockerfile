From maven:3.6.2-jdk-8-openj9

MAINTAINER francis francis@francisdu.com

USER root

# Install dependencies
RUN apt-get update -y&& apt-get install -y ssh vim wget git

# Build Qlick SQL source code
RUN cd ~ && git clone https://github.com/Qihoo360/Quicksql.git && \
    cd Quicksql && mvn clean package -DskipTests

# Unzip the Qlick SQL package
RUN cd ~/Quicksql && tar -xzvf target/$(awk -v RS='\r\n' '/<qsql.release>[^<]+<\/qsql.release>/{gsub(/<qsql.release>|<\/qsql.release>/,"",$1);printf $1;exit;}'  ~/Quicksql/pom.xml).tar.gz -C /usr/local && \
    ln -s /usr/local/$(awk -v RS='\r\n' '/<qsql.release>[^<]+<\/qsql.release>/{gsub(/<qsql.release>|<\/qsql.release>/,"",$1);printf $1;exit;}'  ~/Quicksql/pom.xml) /usr/local/qsql

# Download Spark package
RUN cd ~ && wget https://archive.apache.org/dist/spark/spark-2.2.0/spark-2.2.0-bin-hadoop2.7.tgz &&  \
    tar -xzvf spark-2.2.0-bin-hadoop2.7.tgz -C /usr/local && \
    ln -s /usr/local/spark-2.2.0-bin-hadoop2.7.tgz /usr/local/spark

# TODO : Add other services such as ES,Flink,etc...

# Clean up source code and installation packages and dependencies, etc.
RUN rm -rf ~/Quicksql ~/.m2 ~/spark*

# Setting environments
ENV QSQL_HOME /usr/local/qsql
ENV SPARK_HOME /usr/local/spark
ENV PATH $SPARK_HOME/bin:$SPARK_HOME/sbin:$QSQL_HOME/bin:$PATH
RUN export PATH QSQL_HOME SPARK_HOME

WORKDIR /usr/local/qsql

EXPOSE 4040 8080