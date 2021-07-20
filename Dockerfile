FROM adoptopenjdk:11-hotspot AS build

RUN apt-get update && apt-get install git -y

COPY ./ /home/gradle/src
WORKDIR /home/gradle/src
RUN ./gradlew build --no-daemon && \
    mkdir -p /home/gradle/app && \
    cat build/distributions/couchbase-elasticsearch-connector*.tgz | tar -xzf - -C /home/gradle/app --strip 1



FROM adoptopenjdk:11-hotspot
COPY --from=build /home/gradle/app /opt/couchbase-elasticsearch-connector

ENV PATH="/opt/couchbase-elasticsearch-connector/bin:${PATH}"
VOLUME [ "/opt/couchbase-elasticsearch-connector/config", "/opt/couchbase-elasticsearch-connector/secrets" ]
EXPOSE 31415

ENTRYPOINT [ "/opt/couchbase-elasticsearch-connector/bin/cbes" ]
