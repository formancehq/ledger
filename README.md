# Numary Ledger [![test](https://github.com/numary/ledger/actions/workflows/test.yml/badge.svg)](https://github.com/numary/ledger/actions/workflows/test.yml)

Numary is a general purpose ledger that wants to help you build sound financial applications.

# Getting started

```SHELL
numary server start

curl -X POST \
  -H 'Content-Type: application/json' \
  -d '{
    "postings": [
      {
        "source": "world",
        "destination": "central-bank",
        "asset": "GEM",
        "amount": 100
      },
      {
        "source": "central-bank",
        "destination": "users:001",
        "asset": "GEM",
        "amount": 100
      }
    ]
  }' http://localhost:3068/quickstart/transactions

numary ui
```

# Download