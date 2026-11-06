# Multi-stage build for Worker
FROM maven:3.9-eclipse-temurin-21-alpine AS build
WORKDIR /build

COPY pom.xml .
COPY controller/pom.xml controller/
COPY worker/pom.xml worker/

# Copy worker sources and build
COPY worker/src worker/src
RUN mvn -pl worker -am package -DskipTests

# Runtime image
FROM eclipse-temurin:21-jre-alpine
WORKDIR /app

# Add curl for health check
RUN apk --no-cache add curl

COPY --from=build /build/worker/target/worker-*.jar app.jar

EXPOSE 8081
ENTRYPOINT ["java", "-jar", "app.jar"]
