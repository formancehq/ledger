# Formance Ledger

Formance Ledger (fka numary) is a programmable financial ledger that provides a foundation for money-moving applications. The ledger provides atomic multi-postings transactions and is programmable in [Numscript](doc:machine-instructions), a built-in language dedicated to money movements. It can be used either as a standalone micro-service or as part of the greater Formance Stack, and will shine for apps that require a lot of custom, money-moving code, e.g:

- E-commerce with complex payments flows, payments splitting, such as marketplaces
- Company-issued currencies systems, e.g. Twitch Bits
- In-game currencies, inventories and trading systems, e.g. Fortnite V-Bucks
- Payment gateways using non-standard assets, e.g. learning credits
- Local currencies and complementary finance

Formance Ledger works as a standalone binary, the latest of which can be downloaded from the [releases page](https://github.com/formancehq/ledger/releases). You can move the binary to any executable path, such as to `/usr/local/bin`. Installations using brew, apt, yum or docker are also [available](https://docs.formance.com/docs/installation-1).

## Documentation

You can find the complete Numary documentation at [docs.formance.com](https://docs.formance.com)

## Community

If you need help, want to show us what you built or just hang out and chat about ledgers you are more than welcome on our [Slack](https://bit.ly/formance-slack) - looking forward to see you there!

## Getting started

You can test the ledger using the provided docker-compose:
```shell
$ docker compose up -d
```

Then create a first transaction (output truncated for brievity):
```shell
$ curl -X POST http://localhost:3068/quickstart/transactions -d '{"postings": [{"source": "world", "destination": "bank", "amount": 100, "asset": "USD"}]}' | jq
{
  "data": [
    {
      "postings": [
        {
          "source": "world",
          "destination": "bank",
          "amount": 100,
          "asset": "USD"
        }
      ],
      "metadata": {},
      "timestamp": "2024-10-03T08:10:37.109371Z",
      "insertedAt": "2024-10-03T08:10:37.109371Z",
      "id": 1,
      ...
    }
  ]
}
```

List transactions: 
```
$ curl -X GET http://localhost:3068/quickstart/transactions
```

With those commands, we have created a logical ledger named `quickstart` and a transaction on it.
As a program, the ledger can handle any number of logical ledger.

> [!NOTE]
> In the documentation, you will read the term `ledger` everywhere. When we speak of a `ledger`, we are speaking about a ***logical ledger***

The ledger has been automatically created because the commands use the v1 api (it's legacy).

> [!WARNING]
> Be careful while using the v1 api, if you mess in the ledger name in the url, a ledger will be automatically created

Actually, the ledger has a v2 api, used by prefixing urls with `/v2`.
And, on this version, ledgers has to be created manually.

You can create a new ledger this way :
```shell
$ curl -X POST http://localhost:3068/v2/testing
```

Check the ledger list :
```shell
$ curl http://localhost:3068/v2 | jq
{
  "cursor": {
    "pageSize": 15,
    "hasMore": false,
    "data": [
      {
        "bucket": "quickstart",
        "metadata": {},
        "features": {
          "ACCOUNT_METADATA_HISTORY": "SYNC",
          "HASH_LOGS": "SYNC",
          "INDEX_ADDRESS_SEGMENTS": "ON",
          "INDEX_TRANSACTION_ACCOUNTS": "ON",
          "MOVES_HISTORY": "ON",
          "MOVES_HISTORY_POST_COMMIT_EFFECTIVE_VOLUMES": "SYNC",
          "TRANSACTION_METADATA_HISTORY": "SYNC"
        },
        "id": 1,
        "name": "quickstart",
        "addedAt": "2024-10-02T12:27:56.750952Z"
      },
      {
        "bucket": "_default",
        "metadata": {},
        "features": {
          "ACCOUNT_METADATA_HISTORY": "SYNC",
          "HASH_LOGS": "SYNC",
          "INDEX_ADDRESS_SEGMENTS": "ON",
          "INDEX_TRANSACTION_ACCOUNTS": "ON",
          "MOVES_HISTORY": "ON",
          "MOVES_HISTORY_POST_COMMIT_EFFECTIVE_VOLUMES": "SYNC",
          "TRANSACTION_METADATA_HISTORY": "SYNC"
        },
        "id": 2,
        "name": "testing",
        "addedAt": "2024-10-03T08:03:27.360424Z"
      }
    ]
  }
}
```

Then we see our ledgers :
* `quickstart`: created automatically on the v1 api
* `testing`: created manually

We also see some additional properties:
* `bucket`: [bucket](#buckets) where the ledger is installed
* `features`: [features](#features) configuration of the ledger
* `metadata`: metadata of the ledger

Additionally, each ledger is created on a bucket, `quickstart` is installed on bucket `quickstart` while `testing` is installed on bucket `_default`.

> [!IMPORTANT]
> Any new ledger created on the /v2 api will use, by default, the bucket `_default`.
> 
> But, automatically created ledgers by v1 api will create a new bucket with the same name as the ledger. That's for compatibility reasons regarding ledger v1 behavior.

### Buckets

To create a ledger on a specific bucket, use the command:

```shell
$ curl -X POST http://localhost:3068/v2/testing -d '{"bucket": "bucket0"}'
$ curl http://localhost:3068/v2/testing | jq
{
  "data": {
    "bucket": "bucket0",
    "metadata": {},
    "features": {
      "ACCOUNT_METADATA_HISTORY": "SYNC",
      "HASH_LOGS": "SYNC",
      "INDEX_ADDRESS_SEGMENTS": "ON",
      "INDEX_TRANSACTION_ACCOUNTS": "ON",
      "MOVES_HISTORY": "ON",
      "MOVES_HISTORY_POST_COMMIT_EFFECTIVE_VOLUMES": "SYNC",
      "TRANSACTION_METADATA_HISTORY": "SYNC"
    },
    "id": 2,
    "name": "testing",
    "addedAt": "2024-10-03T08:27:11.540373Z"
  }
}
```

Under the hood, a bucket is a Postgres schema. You can use the bucket feature to implement some kind of horizontal scaling.

### Features

Each usage of the ledger service, is different.
Some usage involve a high write throughput, some other involve high read throughput, custom aggregation etc...

So, each ledger can be configured with a set of features. By default, when creating a ledger, all features are enabled.
See [variables and constants](./internal/README.md#constants) for possible configurations and meaning of each feature.

To create a ledger with specific features, use the command:
```shell
$ curl -X POST http://localhost:3068/v2/testing2 -d '{"features": {"HASH_LOGS": "DISABLED"}}'
$ curl http://localhost:3068/v2/testing2 | jq
{
  "data": {
    "bucket": "_default",
    "metadata": {},
    "features": {
      "ACCOUNT_METADATA_HISTORY": "SYNC",
      "HASH_LOGS": "DISABLED",
      "INDEX_ADDRESS_SEGMENTS": "ON",
      "INDEX_TRANSACTION_ACCOUNTS": "ON",
      "MOVES_HISTORY": "ON",
      "MOVES_HISTORY_POST_COMMIT_EFFECTIVE_VOLUMES": "SYNC",
      "TRANSACTION_METADATA_HISTORY": "SYNC"
    },
    "id": 3,
    "name": "testing2",
    "addedAt": "2024-10-03T08:40:40.545229Z"
  }
}
```

When overriding features, all not specified features will receive the default configuration.

> [!WARNING]
> Current set of feature is not stable, some can be added, or removed.

### Terminology

* Bounded source account: A bounded source account, is an account used in a Numscript script, as a source account, and with a bottom limit. Example:
    ```
    send [USD/2 100] {
      source = @bank
      destination = @user:1
    }
    ```
    In this example, ```bank``` is considered as an unbounded source account.

    An account used with an unbounded overdraft will not be considered as a bounded source account. 
    For example:
    ```
    send [USD/2 100] {
      source = @bank allowing unbounded overdraft
      destination = @user:1
    }
    ```
    With this script, ```bank``` will not be considered as an unbounded source account.
> [!NOTE]
> notes: It is also the case of the ```world``` account, which is always an unbounded overdraft account.
* post commit volumes (pcv): see [description](./internal/README.md#type-transaction)
* post commit effective volumes (pcev): see [description](./internal/README.md#type-transaction)

### Database

Database schemas are extracted from a live ledger.
You can find them in:
* [System schema](./docs/database/_system/diagrams)
* [Bucket schema](./docs/database/_default/diagrams)

## Data consistency

### Transaction commit

The following sequence diagram describe the process of creating a transaction.
It supposes the minimal set of features.

```mermaid
sequenceDiagram
    actor Client
    participant Ledger
    participant Database
    Client->>Ledger: Create transaction
    Ledger->>Database: Start SQL transaction (Read committed isolation level)
    Database-->>Ledger: SQL transaction started
    Ledger->>Database: Get and lock balances of bounded source accounts
    note left of Database: There, we will retrieve balances from database and lock associated balances<br/>Until the end of the SQL transaction, no concurrent transaction will be able to use locked balances.<br/>See notes above for details.
    Database-->>Ledger: Balances of bounded accounts
    Ledger->>Ledger: Compute transaction
    note right of Ledger: Calling Numscript interpreter
    Ledger-->>Database: Store transaction and update balances
    note left of Database: This diagram simplify the process of writing the transaction. More entities will be saved, <br/>and some computation can happen, depending on the enabled features. More on this later. 
    Database->>Ledger: Transaction written
    Ledger->>Database: Commit SQL transaction
    note left of Database: Now, updated account balances are written to the database and any <br/>concurrent transaction waiting for those accounts will be unblocked. 
    Database->>Ledger: Transaction committed
    Ledger->>Client: Created transaction
```

***Get and lock balances of bounded source accounts***

Locking balances of bounded source accounts can be achieved using the following query : 
```sql
SELECT input - output AS balance
FROM accounts_volumes
WHERE address IN (<bounded accounts list>)
FOR UPDATE
```

The ```FOR UPDATE``` add a [RowExclusiveLock](https://www.postgresql.org/docs/current/explicit-locking.html#LOCKING-ROWS) on the balance account until the end of the SQL transaction.

It will work... most of the time. There is still a edge case.
What if the ```accounts_volumes``` table has no rows for the account? Like a never used account.
In this case, the database will not take any lock and the core will work with data with no control.
For example, suppose the following case : 
* send 100 USD from ```user:1``` to ```order:1```
* ```user:1``` balance is allowed to go down to -200 USD

Now, two transaction starting at the same time spending 200 USD.
Both of them will try to get the balance of the ```user:1``` account, and will get an empty balance.
Since the account is allowed to go down to -200, both transaction will pass.
Finally, both transactions will be committed, resulting in a balance of -400 USD, which would violates business rules.

So, a complete solution is more :  
```sql
WITH (
    INSERT INTO accounts_volume (accounts_address, input, output)
    VALUES ... -- Insert 0 as input and output for each account address
    ON CONFLICT DO NOTHING -- If we have conflict, this indicates than the account has already a registered balance, so ignore the conflict
) AS ins
SELECT input - output AS balance -- Finally, we still need to select the balance and lock the row
FROM accounts_volumes
WHERE address IN (<bounded accounts list>)
FOR UPDATE
```

### Import

TODO

## Testing strategy

Tests are split in different scopes :
* Unit tests: as any go app, you will find unit test along the source code in _test.go files over the app.
* [e2e](./test/e2e) : End to end test. Those tests are mainly api tests, and app lifecycle tests. It checks than the ledger endpoint works as expected.
* [migrations](./test/migrations) : Migrations tests. Tests inside this package allow to import an existing database to apply current code migrations on it.
* [performance](./test/performance) : Performance tests. Tests inside this package test performance of the ledger.
* [stress](./test/stress) : Stress tests. Tests inside this package ensure than ledger state stay consistent under high concurrency.

## API changes

Openapi specification at [root](./openapi.yaml) must not be edited directly as it is generated.
Update [v2 spec](./openapi/v2.yaml) and regenerate the client using `earthly +pre-commit` or `earthly +generate-client`.

## Dev commands

### Before commit

```shell
$ earthly +pre-commit
```

This command will :
* lint the code
* generate the client sdk (in [pkg/client](pkg/client))
* fix dependencies
* generate [openapi](openapi.yaml) specification by combining [api versions](./openapi)

### Run tests

```shell
$ earthly -P +tests
```

Additionnally, the flag ```--coverage=true``` can be passed to generate coverage :
```shell
$ earthly -P +tests --coverage=true # Generated under cover.out
```