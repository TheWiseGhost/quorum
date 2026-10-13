FROM eclipse-temurin:25-jdk AS build
WORKDIR /src
COPY . .
RUN ./gradlew --no-daemon :quorum-server:installDist :quorum-client:installDist

FROM eclipse-temurin:25-jre
WORKDIR /app
COPY --from=build /src/quorum-server/build/install/quorum-server /app/server
COPY --from=build /src/quorum-client/build/install/quorum /app/client
ENV PATH="/app/server/bin:/app/client/bin:${PATH}"
ENTRYPOINT ["quorum-server"]
