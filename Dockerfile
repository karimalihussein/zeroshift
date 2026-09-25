# One image recipe for every runnable module: docker build --build-arg MODULE=order-service .
FROM maven:3.9.11-eclipse-temurin-25 AS build
ARG MODULE=migration-lab
WORKDIR /build
COPY . .
RUN --mount=type=cache,target=/root/.m2 mvn -B -q -pl "$MODULE" -am -DskipTests package \
 && cp "$MODULE/target/$MODULE.jar" /build/app.jar
FROM eclipse-temurin:25-jre
RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/* && useradd --system --uid 10001 app
WORKDIR /app
# OpenTelemetry Java agent: traces and logs without code changes. Inert unless JAVA_TOOL_OPTIONS
# attaches it (docker-compose.yml does), so the image also runs without an observability stack.
ADD --chmod=644 --checksum=sha256:bbf83c151b6400709e2f225bdd07a04f839d9d13b8b93464241333fd25d3e3ba \
  https://repo1.maven.org/maven2/io/opentelemetry/javaagent/opentelemetry-javaagent/2.31.1/opentelemetry-javaagent-2.31.1.jar \
  /app/opentelemetry-javaagent.jar
COPY --from=build /build/app.jar app.jar
USER app
EXPOSE 8080
ENTRYPOINT ["java","-XX:MaxRAMPercentage=65","-jar","app.jar"]
