FROM eclipse-temurin:11-jre

WORKDIR /opt/iceberg

# Download Iceberg REST catalog server
ADD https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-rest-server/1.6.1/iceberg-rest-server-1.6.1.jar iceberg-rest.jar

EXPOSE 8181

CMD ["java", "-jar", "iceberg-rest.jar"]
