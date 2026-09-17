FROM registry.access.redhat.com/ubi9/openjdk-25:latest AS build
USER root
RUN microdnf install -y gzip tar && microdnf clean all
WORKDIR /app
COPY .mvn/ .mvn/
COPY mvnw pom.xml ./
RUN ./mvnw -q -B -DskipTests dependency:resolve
COPY src/ src/
RUN ./mvnw -q -B -DskipTests package

FROM registry.access.redhat.com/ubi9/openjdk-25-runtime:latest
WORKDIR /app
COPY --from=build /app/target/partitioner-0.0.1-SNAPSHOT.jar app.jar
ENV JAVA_TOOL_OPTIONS="-XX:+UseParallelGC -XX:+UseCompactObjectHeaders -XX:MaxRAMPercentage=70.0 -XX:InitialRAMPercentage=70.0 -XX:+PrintCommandLineFlags -Xlog:gc*:file=/tmp/gc.log --sun-misc-unsafe-memory-access=allow"
EXPOSE 8081
ENTRYPOINT ["java", "-jar", "app.jar"]
