FROM centos:7

RUN yum -y update && yum -y install java-11-openjdk-headless openssl && yum -y clean all

# Set JAVA_HOME env var
ENV JAVA_HOME /usr/lib/jvm/java

ARG version=latest
ENV VERSION ${version}

#COPY ./scripts/ /bin
COPY src/main/resources /bin/log4j2.properties

ADD target/oldproducer-1.0-SNAPSHOT.jar /

CMD ["java", "-jar" ,  "/oldproducer-1.0-SNAPSHOT.jar"]
