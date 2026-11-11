# Multi-stage build for the Controller
FROM maven:3.9-eclipse-temurin-21-alpine AS build
WORKDIR /build

# all module POMs are needed because the parent POM lists every module
COPY pom.xml .
COPY controller/pom.xml controller/
COPY worker/pom.xml worker/
COPY client/pom.xml client/

COPY controller/src controller/src
RUN --mount=type=cache,target=/root/.m2 mvn -B -q -pl controller -am package -DskipTests

# Runtime image
FROM eclipse-temurin:21-jre-alpine
WORKDIR /app

# curl is used by the Docker health check
RUN apk --no-cache add curl

COPY --from=build /build/controller/target/controller-*.jar app.jar

EXPOSE 8080
ENTRYPOINT ["java", "-jar", "app.jar"]
