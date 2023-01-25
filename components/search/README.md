# search

## How to run?

```
docker compose up
```

## How to test messages?

```
docker compose exec -ti redpanda bash
redpanda@ad6639f1576b:/$ cat /src/benthos/messages/committed_transactions.json | rpk topic produce ledger
```

## Env vars

### Ingester

#### Input
- KAFKA_ADDRESS
- KAFKA_TOPIC
- KAFKA_VERSION
- KAFKA_CONSUMER_GROUP

#### Traces
- JAEGER_COLLECTOR
- SERVICE_NAME

#### Output
- OPENSEARCH_URL
- OPENSEARCH_INDEX
- OPENSEARCH_TLS_ENABLED
- OPENSEARCH_TLS_SKIP_CERT_VERIFY
- OPENSEARCH_BASIC_AUTH_ENABLED
- OPENSEARCH_AUTH_USERNAME
- OPENSEARCH_AUTH_PASSWORD
