FROM jeffail/benthos:4.10
WORKDIR /config
COPY benthos /config
ENV ELASTICSEARCH_BATCHING_COUNT 50
ENV ELASTICSEARCH_BATCHING_PERIOD 1s
ENV TOPIC_PREFIX=""
CMD ["-c", "config.yml", "-r", "/config/resources/*.yaml", "-t", "/config/templates/*.yaml", "streams", "/config/streams/*.yaml"]
