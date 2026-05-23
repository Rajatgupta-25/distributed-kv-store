# ─────────────────────────────────────────────────────────────────────────────
# Stage 1: Build
# Uses a full JDK + Maven image to compile and package the application.
# This stage is discarded after the build — only the JAR is carried forward.
# ─────────────────────────────────────────────────────────────────────────────
FROM maven:3.9.6-eclipse-temurin-17 AS builder

WORKDIR /build

# Copy dependency descriptors first — Docker caches this layer separately.
# If only source code changes (not pom.xml), Maven dependencies are NOT
# re-downloaded on the next build. This makes rebuilds much faster.
COPY pom.xml .
RUN mvn dependency:go-offline -q

# Copy source and build the fat JAR
COPY src ./src
RUN mvn package -DskipTests -q

# ─────────────────────────────────────────────────────────────────────────────
# Stage 2: Runtime
# Uses a minimal JRE-only image — no compiler, no Maven, no source code.
# Smaller image = faster pull, smaller attack surface.
# ─────────────────────────────────────────────────────────────────────────────
FROM eclipse-temurin:17-jre

# Create a non-root user for security.
# Running as root inside a container is a security risk.
RUN groupadd -r kvstore && useradd -r -g kvstore kvstore

WORKDIR /app

# Copy only the fat JAR from the build stage
COPY --from=builder /build/target/distributed-kv-store-1.0.0.jar app.jar

# Create the data directory for WAL and snapshot files.
# This will be overridden by a Docker volume in production.
RUN mkdir -p /data && chown kvstore:kvstore /data

# Switch to non-root user
USER kvstore

# Expose the application port.
# The actual port is set via NODE_PORT env var (default 8081).
EXPOSE 8081

# Health check — Docker will mark the container unhealthy if this fails.
# Waits 30s before first check (startup time), then checks every 10s.
HEALTHCHECK --interval=10s --timeout=3s --start-period=30s --retries=3 \
  CMD curl -f http://localhost:${NODE_PORT:-8081}/actuator/health || exit 1

# JVM tuning for containers:
# -XX:+UseContainerSupport  → respect Docker memory limits (not host RAM)
# -XX:MaxRAMPercentage=75   → use max 75% of container memory for heap
# -Djava.security.egd      → faster startup (avoids /dev/random blocking)
ENTRYPOINT ["java", \
  "-XX:+UseContainerSupport", \
  "-XX:MaxRAMPercentage=75.0", \
  "-Djava.security.egd=file:/dev/./urandom", \
  "-jar", "app.jar"]
