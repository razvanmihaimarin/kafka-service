FROM gradle:8.0.1-jdk17 AS builder
WORKDIR /opt/app/kafka-service

COPY . ./

RUN bash -c 'gradle compileTestJava'

FROM builder as packager

RUN bash -c 'gradle bootJar'

FROM openjdk:17.0.1

COPY --from=packager /opt/app/kafka-service/build/libs/kafka-service-0.0.1-SNAPSHOT.jar app.jar

ENTRYPOINT ["java","-jar","/app.jar"]