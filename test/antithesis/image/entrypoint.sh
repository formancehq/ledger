#!/bin/sh
# make sure pg is ready to accept connections
until pg_isready -d ledger -h 10.0.29.16 -U ledger
do
  echo "Waiting for postgres at: $POSTGRES_URI"
  sleep 2;
done

echo "Postgres is ready; serving ledger!"

ledger serve
