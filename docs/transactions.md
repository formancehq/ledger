# Transaction Model

When considering adopting a ledger, an important thing to understand is what transaction model the ledger uses underneath. There are multiple options there, the key distictions you'll find in the wild will be regarding the number of postings per transactions and the number of i/o per postings.

## Postings

In Numary and in general, postings model the movement of an amount of an asset from one account to another.

```
# Alice gives 100 coins to her friend Bob

100 COIN alice -> bob
```

## Transactions

In Numary, transactions model the wrapping of postings with the intent of comitting them atomically.

```
# Alice gives 100 coins to the teller in exchange for 5 gems

Transaction 001
  100 COIN  alice -> teller
  100 COIN teller ->   safe
    5  GEM teller ->  alice
```

Numary uses single i/o postings with multi-postings transactions. The rationale behind this originates from Numary's goal: help developers build sound financial applications, and supported by these observations:

* Multi-postings transactions allows the ledger to leverage atomicity to reduce the complexity on your side to handle complex transactions, e.g credit this user of X coin by funding the credit from multiple other accounts.

* While mathematically correct, multi i/o postings are inherently hard to grasp mentally and make auditability a challenge, which goes against Numary's first goal of helping developer build sound financial applications.

* In any-case, multi-postings transactions can be used to model i/o postings if you really need them, albeit at the cost the atomicity of transactions.