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

Additionally, you can pass the flag `-report.dir` to export results:
```shell
go test -bench=testserver -run ^$ -tags it -report.dir .
```

The exported files are PNG. 

Each feature is tested against a test script involving a transaction from a source to a destination.
The benchmarks also test the minimal set of features and the full set of features.

Refer to [features](../../CONTRIBUTING.md/#features) for more information about features.

Three types of script are actually tested:
* world->bank : A transaction from `@world` to `@bank`
* world->any : A transaction from `@world` to `@dst:<iteration>`
* any(unbounded)->any : A transaction from `@src:<iteration>` to `@dst:<iteration>`