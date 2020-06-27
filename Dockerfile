FROM gradle:4.9.0-jdk8-alpine AS build

# Switch back to root long enough to install Git
USER root
RUN apk add --no-cache git
USER gradle

COPY --chown=gradle:gradle ./ /home/gradle/src
WORKDIR /home/gradle/src
RUN gradle build --no-daemon && \
    mkdir -p /home/gradle/app && \
    cat build/distributions/couchbase-elasticsearch-connector*.tgz | tar -xzf - -C /home/gradle/app --strip 1



FROM adoptopenjdk:8-hotspot
COPY --from=build /home/gradle/app /opt/couchbase-elasticsearch-connector

ENV PATH="/opt/couchbase-elasticsearch-connector/bin:${PATH}"
VOLUME [ "/opt/couchbase-elasticsearch-connector/config", "/opt/couchbase-elasticsearch-connector/secrets" ]
EXPOSE 31415

ENTRYPOINT [ "/opt/couchbase-elasticsearch-connector/bin/cbes" ]
