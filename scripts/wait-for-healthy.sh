#!/bin/sh
NODE=$1
MAX_ATTEMPTS=30
ATTEMPT=0

echo "Waiting for $NODE to be healthy..."

while [ $ATTEMPT -lt $MAX_ATTEMPTS ]; do
    if docker-compose ps "$NODE" | grep -q "healthy"; then
        echo "$NODE is healthy"
        exit 0
    fi
    sleep 2
    ATTEMPT=$((ATTEMPT + 1))
done

echo "Warning: $NODE may not be healthy yet"
exit 1

