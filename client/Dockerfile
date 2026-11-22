# CLI client container. Usage: docker exec -it client objstore <command> ...
FROM maven:3.9-eclipse-temurin-21-alpine AS build
WORKDIR /build

COPY pom.xml .
COPY controller/pom.xml controller/
COPY worker/pom.xml worker/
COPY client/pom.xml client/

COPY client/src client/src
RUN --mount=type=cache,target=/root/.m2 mvn -B -q -pl client -am package -DskipTests

FROM eclipse-temurin:21-jre-alpine
RUN apk --no-cache add curl jq coreutils
COPY --from=build /build/client/target/client-*.jar /app/client.jar
RUN printf '#!/bin/sh\nexec java -XX:TieredStopAtLevel=1 -jar /app/client.jar "$@"\n' > /usr/local/bin/objstore \
    && chmod +x /usr/local/bin/objstore

ENV OBJSTORE_URL=http://gateway:8080
WORKDIR /files
# stays running so you can "docker exec" commands into it
CMD ["sleep", "infinity"]
