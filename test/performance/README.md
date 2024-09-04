# Performance test

## Run locally

```shell
go test -bench=. -run ^$ -tags it
```

## Run on a remote stack

```shell
go test -bench=. -run ^$ -tags it \
  --stack.url=XXX \
  --client.id=XXX \
  --client.secret=XXX
```

## Run on a remote ledger

```shell
go test -bench=. -run ^$ -tags it \
  --ledger.url=XXX \
  --auth.url=XXX \
  --client.id=XXX \
  --client.secret=XXX
```

## Results

The output is a standard go bench output.

Example: 
```
goos: darwin
goarch: arm64
pkg: github.com/formancehq/ledger/test/performance
BenchmarkWrite/remote/world->bank/HL=SYNC,PCEV=SYNC,PCV=SYNC-8                      84          15693529 ns/op               113.0 ms/transaction             62.81 t/s
BenchmarkWrite/remote/world->bank/HL=DISABLED,PCEV=SYNC,PCV=SYNC-8                  90          12960446 ns/op               100.0 ms/transaction             71.65 t/s
BenchmarkWrite/remote/world->bank/HL=SYNC,PCEV=DISABLED,PCV=SYNC-8                  87          13522271 ns/op               103.0 ms/transaction             70.18 t/s
BenchmarkWrite/remote/world->bank/HL=DISABLED,PCEV=DISABLED,PCV=SYNC-8              92          13460427 ns/op               102.0 ms/transaction             71.03 t/s
BenchmarkWrite/remote/world->bank/HL=SYNC,PCEV=SYNC,PCV=DISABLED-8                  66          15225778 ns/op               111.0 ms/transaction             59.71 t/s
BenchmarkWrite/remote/world->bank/HL=DISABLED,PCEV=SYNC,PCV=DISABLED-8              80          16243343 ns/op               119.0 ms/transaction             60.42 t/s
BenchmarkWrite/remote/world->bank/HL=SYNC,PCEV=DISABLED,PCV=DISABLED-8              55          19083905 ns/op               140.0 ms/transaction             48.15 t/s
BenchmarkWrite/remote/world->bank/HL=DISABLED,PCEV=DISABLED,PCV=DISABLED-8                  57          22134052 ns/op               155.0 ms/transaction             43.34 t/s
BenchmarkWrite/remote/world->any/HL=SYNC,PCEV=SYNC,PCV=SYNC-8                               75          15552648 ns/op               120.0 ms/transaction             58.70 t/s
BenchmarkWrite/remote/world->any/HL=DISABLED,PCEV=SYNC,PCV=SYNC-8                           68          17866844 ns/op               134.0 ms/transaction             51.82 t/s
BenchmarkWrite/remote/world->any/HL=SYNC,PCEV=DISABLED,PCV=SYNC-8                           81          13593212 ns/op               107.0 ms/transaction             66.39 t/s
BenchmarkWrite/remote/world->any/HL=DISABLED,PCEV=DISABLED,PCV=SYNC-8                       84          13560348 ns/op               104.0 ms/transaction             67.69 t/s
BenchmarkWrite/remote/world->any/HL=SYNC,PCEV=SYNC,PCV=DISABLED-8                           86          14518456 ns/op               108.0 ms/transaction             67.05 t/s
BenchmarkWrite/remote/world->any/HL=DISABLED,PCEV=SYNC,PCV=DISABLED-8                       90          13041432 ns/op               101.0 ms/transaction             71.27 t/s
BenchmarkWrite/remote/world->any/HL=SYNC,PCEV=DISABLED,PCV=DISABLED-8                       76          14276990 ns/op               108.0 ms/transaction             66.02 t/s
BenchmarkWrite/remote/world->any/HL=DISABLED,PCEV=DISABLED,PCV=DISABLED-8                   88          13430946 ns/op               103.0 ms/transaction             69.66 t/s
BenchmarkWrite/remote/any(unbounded)->any/HL=SYNC,PCEV=SYNC,PCV=SYNC-8                      72          15435962 ns/op               120.0 ms/transaction             58.49 t/s
BenchmarkWrite/remote/any(unbounded)->any/HL=DISABLED,PCEV=SYNC,PCV=SYNC-8                  84          15247506 ns/op               114.0 ms/transaction             62.61 t/s
BenchmarkWrite/remote/any(unbounded)->any/HL=SYNC,PCEV=DISABLED,PCV=SYNC-8                  78          16336328 ns/op               122.0 ms/transaction             59.34 t/s
BenchmarkWrite/remote/any(unbounded)->any/HL=DISABLED,PCEV=DISABLED,PCV=SYNC-8              62          17001069 ns/op               125.0 ms/transaction             54.60 t/s
BenchmarkWrite/remote/any(unbounded)->any/HL=SYNC,PCEV=SYNC,PCV=DISABLED-8                  62          18296797 ns/op               137.0 ms/transaction             51.63 t/s
BenchmarkWrite/remote/any(unbounded)->any/HL=DISABLED,PCEV=SYNC,PCV=DISABLED-8              78          15578895 ns/op               118.0 ms/transaction             60.62 t/s
BenchmarkWrite/remote/any(unbounded)->any/HL=SYNC,PCEV=DISABLED,PCV=DISABLED-8              57          18337382 ns/op               146.0 ms/transaction             45.01 t/s
BenchmarkWrite/remote/any(unbounded)->any/HL=DISABLED,PCEV=DISABLED,PCV=DISABLED-8          66          17246153 ns/op               133.0 ms/transaction             52.94 t/s

```

There is the format:
```
BenchmarkWrite/<env>/<transaction-type>/<features>/<bucket>/<ledger>-<cpu count>                      <iteration count>          <ns by op>               <average transaction duration>             <tps>
```

Where parameters are :
* env: 
  * core: the test use directly the core service of the ledger
  * testserver: the test use a full running server
  * remote : the test use a remote api
* transaction-type (see [scripts](./write_test.go))
  * world->bank : A transaction from `@world` to `@bank`
  * world->any : A transaction from `@world` to `@dst:<iteration>`
  * any(unbounded)->any : A transaction from `@src:<iteration>` to `@dst:<iteration>`
* features: features enabled on the server (affect only servers supporting the feature)
  * All combination of ledger features are tested. So the field `<features>` will contains the list of the feature. For better visibility, features names are abbreviated. There is the code : 
    * HL => HASH_LOG : Possible configuration are `DISABLED` or `SYNC`
    * PCEV => POST_COMMIT_EFFECTIVE_VOLUMES : Possible configurations are `DISABLED` or `SYNC`
    * PCV => POST_COMMIT_VOLUMES : Possible configurations are `DISABLED` or `SYNC`
    * IAS => INDEX_ADDRESS_SEGMENTS : Index individual segments address of accounts. Possible configurations are 'ON' or 'OFF'.
    * AMH => ACCOUNT_METADATA_HISTORY : Historize accounts metadata
    * TMH => TRANSACTION_METADATA_HISTORY : Historize transactions metadata

If the flag `--include-ledger-in-results` is specified, the line will contain two additional arguments :
* bucket: bucket name where the ledger is created
* ledger: ledger name

Example:
```
BenchmarkWrite/remote/world->bank/HL=SYNC,PCEV=SYNC,PCV=SYNC/5372990c/363e57d5-8                      90          13808911 ns/op               104.0 ms/transaction             68.89 t/s 
...
```

Other columns of the benchmark output are the metrics.
In addition to the standard metrics, we have two specific metrics : 
* average transaction duration
* tps : transactions per second