Kafka Processing service 
---
Welcome to the Kafka Processing Service

### Introduction

Kafka service processing messages from specific topics and writing the data into a Postgres database, using the 
reactive paradigm. For testing purposes, the KafkaProducerService upon startup produces 100 events. These are processed in the KafkaConsumerService and 
inserted into the database.

Few considerations regarding the consumer:
- since the requirement did not mention anything about idempotency, that was **not** insured in the consumer. 
- if that had been required, then additional database searches would have been necessary alongside with additional business logic concerning the way the data needs to be updated. 

**Prerequisites:**
[Java 17](https://adoptopenjdk.net/),
[Docker](https://www.docker.com/),
[Postgres](https://www.postgresql.org/),
[Apache Kafka](https://kafka.apache.org/),
[Gradle 8.0.1](https://gradle.org/).

* [Getting Started](#getting-started)
* [Help](#help)

## Getting Started

To run this application, fist download the archive containing the application, unzip it and  run the following commands:

```bash
cd kafka-service
docker build -t nordcloud/kafka-service:0.0.1-SNAPSHOT .
docker compose up
```

This example uses the following open source projects:
* [Spring Boot](https://spring.io/projects/spring-boot)
* [Spring Data R2DBC](https://docs.spring.io/spring-data/r2dbc/docs/current/reference/html/)
* [Project reactor](https://projectreactor.io/)
* [Reactor Kafka](https://projectreactor.io/docs/kafka/release/reference/)
* [TestContainers](https://www.testcontainers.org/)

## Help
For any questions you might have regarding this project, please email them at: razvanmihaimarin@gmail.com .