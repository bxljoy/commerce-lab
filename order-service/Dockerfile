# syntax=docker/dockerfile:1

# ---- Build stage -------------------------------------------------------------
# Build inside Docker so `make up` is self-contained (no local Maven required).
FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /build

# Cache dependencies: copy the POM first and resolve, so source-only changes
# don't re-download the world on every rebuild.
COPY pom.xml .
RUN mvn -B -q dependency:go-offline

# openapi.yaml is the source of truth — the build generates the API interface + DTOs
# from it, so it must be present before `package`.
COPY openapi.yaml .
COPY src ./src
RUN mvn -B -q clean package -DskipTests

# ---- Runtime stage -----------------------------------------------------------
FROM eclipse-temurin:21-jre AS runtime
WORKDIR /app

# curl is used by the compose healthcheck to hit the Actuator endpoint.
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*

# Run as a non-root user.
RUN useradd --system --uid 1001 appuser
USER appuser

COPY --from=build /build/target/*.jar app.jar

EXPOSE 8080
ENTRYPOINT ["java", "-jar", "app.jar"]
