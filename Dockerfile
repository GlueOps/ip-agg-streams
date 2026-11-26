# Build: docker build -t <registry>/ip-agg-streams:<tag> .
FROM maven:3.10-eclipse-temurin-21@sha256:af3a049b2808ffaeccd31e2c2eba0d47776468d1e4f2fd6c1f9fd2f89aabf602 AS build
WORKDIR /src
COPY pom.xml .
RUN mvn -q -B dependency:go-offline
COPY src src
RUN mvn -q -B -DskipTests package

# glibc base (not alpine): snappy-java and RocksDB ship glibc native libraries.
FROM eclipse-temurin:21-jre
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
