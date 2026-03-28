FROM gradle:jdk17 AS build
WORKDIR /src

COPY gradle gradle
COPY gradlew settings.gradle.kts build.gradle.kts ./
COPY src src

RUN ./gradlew clean jar shadowJar --no-daemon

RUN ls -l /src/build/libs/*.jar

RUN chmod +x /src/build/libs/Spark-1.0-SNAPSHOT-all.jar

FROM apache/spark:4.0.2
WORKDIR /opt/app

USER root

COPY --from=build /src/build/libs/Spark-1.0-SNAPSHOT-all.jar .
COPY script.sh .
RUN chmod +x script.sh

ENTRYPOINT exec ./script.sh