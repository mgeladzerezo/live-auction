# syntax=docker/dockerfile:1
FROM maven:3.9-eclipse-temurin-25 AS build
WORKDIR /build
COPY pom.xml ./
RUN --mount=type=cache,target=/root/.m2 mvn -B -q dependency:go-offline
COPY src ./src
RUN --mount=type=cache,target=/root/.m2 mvn -B -q -DskipTests package \
    && cp target/live-auction-*.jar /build/app.jar

FROM eclipse-temurin:25-jre
RUN useradd --system --uid 10001 --no-create-home auction
WORKDIR /app
COPY --from=build /build/app.jar app.jar
USER auction
EXPOSE 8205
ENV PORT=8205 JAVA_TOOL_OPTIONS="-XX:MaxRAMPercentage=70"
HEALTHCHECK --interval=10s --timeout=3s --start-period=40s --retries=6 \
    CMD ["bash", "-c", "exec 3<>/dev/tcp/localhost/8205 && printf 'GET /actuator/health/readiness HTTP/1.0\r\n\r\n' >&3 && head -n1 <&3 | grep -q ' 200 '"]
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
