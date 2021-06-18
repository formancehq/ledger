# Numary Ledger [![test](https://github.com/numary/ledger/actions/workflows/test.yml/badge.svg)](https://github.com/numary/ledger/actions/workflows/test.yml)

Numary is an open-source general ledger that makes building financial apps safe, fun and cheap.

Building financial software is both critical and notably hard. The [same bugs](https://medium.com/selency-tech-product/your-balance-is-0-30000000004-b6f7870bd32e) are repeated again and again, paving the highway to catastrophes.

Numary wants to tackle this issue with a general ledger that provides atomic multi-postings transactions, and that will be programmable in [Numscript](https://github.com/numary/machine), a built-in language dedicated to money movements. It will shine for apps that require a lot of custom, money-touching code such as:

* E-commerce with complex payments flows, payments splitting, such as marketplaces
* Company-issued currencies systems, e.g. Twitch Bits
* In-game currencies, inventories and trading systems, e.g. Fortnite V-Bucks
* Payment gateways using non-standard assets, e.g. learning credits
* Local currencies and complementary finance

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

curl -X GET \
  http://localhost:3068/accounts/users:001
```

# Documentation

_Work in progress!_
