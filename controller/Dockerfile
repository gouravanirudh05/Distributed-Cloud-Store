# Multi-stage build for Controller
FROM maven:3.9-eclipse-temurin-21-alpine AS build
WORKDIR /build

COPY pom.xml .
COPY controller/pom.xml controller/
COPY worker/pom.xml worker/

# Copy controller sources and build
COPY controller/src controller/src
RUN mvn -pl controller -am package -DskipTests

# Runtime image
FROM eclipse-temurin:21-jre-alpine
WORKDIR /app

# Add curl for health check
RUN apk --no-cache add curl

COPY --from=build /build/controller/target/controller-*.jar app.jar

EXPOSE 8080
ENTRYPOINT ["java", "-jar", "app.jar"]
