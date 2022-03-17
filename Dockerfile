FROM openjdk:11

WORKDIR /app

COPY target/scala-2.13/scala-test-crawler-assembly-0.1.0-SNAPSHOT.jar /app/project.jar

CMD ["java","-jar","project.jar"]