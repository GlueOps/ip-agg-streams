# Build: docker build -t <registry>/ip-agg-streams:<tag> .
FROM maven:3.9-eclipse-temurin-21@sha256:99e61abcff91a9b1333463bd8451fb18495d6eba9250ac66a338b518f8278320 AS build
WORKDIR /src
COPY pom.xml .
RUN mvn -q -B dependency:go-offline
COPY src src
RUN mvn -q -B -DskipTests package

# glibc base (not alpine): snappy-java and RocksDB ship glibc native libraries.
FROM eclipse-temurin:21-jre@sha256:d77e090ae1f6970c047b443c6f0a6c1358b0426d44432bb5a7753408cd43de0d
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
