FROM openjdk:11

WORKDIR /app

COPY scala-test-crawler.jar /app/project.jar
COPY /scrapy.sh /app/scrapy.sh

ENTRYPOINT ["./scrapy.sh"]