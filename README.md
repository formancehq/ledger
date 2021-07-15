# Numary Ledger [![test](https://github.com/numary/ledger/actions/workflows/main.yml/badge.svg)](https://github.com/numary/ledger/actions/workflows/main.yml)

Numary is a programmable financial ledger that wants to make building financial apps safe, fun and cheap.

Building financial software is both critical and notably hard. The [same bugs](https://medium.com/selency-tech-product/your-balance-is-0-30000000004-b6f7870bd32e) are repeated again and again, paving the highway to catastrophes.

Numary wants to tackle this issue with a ledger that provides atomic multi-postings transactions and is programmable in [Numscript](https://github.com/numary/machine), a built-in language dedicated to money movements. It will shine for apps that require a lot of custom, money-touching code such as:

* E-commerce with complex payments flows, payments splitting, such as marketplaces
* Company-issued currencies systems, e.g. Twitch Bits
* In-game currencies, inventories and trading systems, e.g. Fortnite V-Bucks
* Payment gateways using non-standard assets, e.g. learning credits
* Local currencies and complementary finance

# Getting started

Numary works as a standalone binary, the latest of which can be downloaded from the [releases page](https://github.com/numary/ledger/releases). You can move the binary to any executable path, such as to `/usr/local/bin`.

```SHELL

numary server start

# Issue GEMs from the world account, and fund users:001
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

# Get the balances of users:001
curl -X GET http://localhost:3068/quickstart/accounts/users:001

# List transactions
curl -X GET http://localhost:3068/quickstart/transactions
```

# Documentation

You can find the complete Numary documentation at [docs.numary.com](https://docs.numary.com)
