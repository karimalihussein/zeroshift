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
COPY --from=build /build/app.jar app.jar
USER app
EXPOSE 8080
ENTRYPOINT ["java","-XX:MaxRAMPercentage=65","-jar","app.jar"]
