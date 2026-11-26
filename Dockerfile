# Build: docker build -t <registry>/ip-agg-streams:<tag> .
FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /src
COPY pom.xml .
RUN mvn -q -B dependency:go-offline
COPY src src
RUN mvn -q -B -DskipTests package

# glibc base (not alpine): snappy-java and RocksDB ship glibc native libraries.
FROM eclipse-temurin:25-jre@sha256:4ed132f6832e9804b123e89a849a7b4502756ae64a4ee08ebab939d9d5d0a9fa
RUN useradd --system --uid 10001 app && mkdir /state && chown app /state
WORKDIR /app
COPY --from=build /src/target/ip-agg-streams-*.jar app.jar
USER 10001
# Heap at half the container limit: RocksDB (block cache + memtables, see BoundedRocksDbConfig) and snappy live
# off-heap.
ENV JAVA_TOOL_OPTIONS="-XX:MaxRAMPercentage=50 -XX:+ExitOnOutOfMemoryError" \
    KAFKA_STATE_DIR=/state
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
