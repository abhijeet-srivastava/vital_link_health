# One image recipe for the three deployables: docker build --build-arg MODULE=ingest-api -f docker/app/Dockerfile .
FROM eclipse-temurin:21-jdk AS build
WORKDIR /src
COPY mvnw pom.xml ./
COPY .mvn .mvn
COPY readings-alerting readings-alerting
ARG MODULE
RUN --mount=type=cache,target=/root/.m2 ./mvnw -B -q -pl readings-alerting/${MODULE} -am package -DskipTests

FROM eclipse-temurin:21-jre
RUN apt-get update \
 && apt-get install -y --no-install-recommends curl \
 && rm -rf /var/lib/apt/lists/* \
 && useradd --system --uid 10001 app
ARG MODULE
COPY --from=build /src/readings-alerting/${MODULE}/target/${MODULE}.jar /app/app.jar
USER app
ENTRYPOINT ["java", "-XX:MaxRAMPercentage=75", "-jar", "/app/app.jar"]
