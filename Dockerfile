FROM registry.access.redhat.com/ubi9/openjdk-25:latest AS build
USER root
RUN microdnf install -y gzip tar && microdnf clean all
WORKDIR /app
COPY .mvn/ .mvn/
COPY mvnw pom.xml ./
RUN ./mvnw -q -B -DskipTests dependency:resolve
COPY src/ src/
RUN ./mvnw -q -B -DskipTests package

FROM registry.access.redhat.com/ubi9/openjdk-25:latest AS otel
ARG OTEL_AGENT_VERSION=2.31.1
USER root
WORKDIR /agent
RUN curl -sSLo opentelemetry-javaagent.jar \
    https://github.com/open-telemetry/opentelemetry-java-instrumentation/releases/download/v${OTEL_AGENT_VERSION}/opentelemetry-javaagent.jar

FROM registry.access.redhat.com/ubi9/openjdk-25-runtime:latest
WORKDIR /app
COPY --from=build /app/target/partitioner-0.0.1-SNAPSHOT.jar app.jar
COPY --from=otel /agent/opentelemetry-javaagent.jar opentelemetry-javaagent.jar
ENV JAVA_TOOL_OPTIONS="-javaagent:/app/opentelemetry-javaagent.jar -XX:+UseParallelGC -XX:+UseCompactObjectHeaders -XX:MaxRAMPercentage=70.0 -XX:InitialRAMPercentage=70.0 -XX:+PrintCommandLineFlags -Xlog:gc*:file=/tmp/gc.log --sun-misc-unsafe-memory-access=allow"
ENV OTEL_SERVICE_NAME=partitioner \
    OTEL_TRACES_EXPORTER=none \
    OTEL_METRICS_EXPORTER=none \
    OTEL_LOGS_EXPORTER=none \
    OTEL_JAVAAGENT_LOGGING=none
EXPOSE 8081
ENTRYPOINT ["java", "-jar", "app.jar"]
