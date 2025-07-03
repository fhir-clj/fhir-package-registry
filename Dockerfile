FROM clojure:tools-deps AS builder

WORKDIR /app

RUN apt-get update && apt-get install -y curl \
    && curl -sLo /usr/local/bin/tailwindcss \
      https://github.com/tailwindlabs/tailwindcss/releases/latest/download/tailwindcss-linux-$(dpkg --print-architecture) \
    && chmod +x /usr/local/bin/tailwindcss

COPY . .

RUN --mount=target=/root/.m2,type=cache,sharing=locked \
    clojure -T:build uber


FROM bellsoft/liberica-openjre-alpine-musl:24

COPY --from=builder /app/target/fhir-registry.jar /app/fhir-registry.jar

CMD ["java", "-jar", "/app/fhir-registry.jar"]