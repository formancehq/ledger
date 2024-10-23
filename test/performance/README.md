# Performance test

Each feature is tested against a test script involving a transaction from a source to a destination.
The benchmarks also test the minimal set of features and the full set of features.

Refer to [features](../../CONTRIBUTING.md/#features) for more information about features.

Scripts can be found in directory [scripts](./scripts).

## Run locally

```shell
earthly +run
```

You can pass additional arguments (the underlying command is a standard `go test -bench=.`) using the flag `--args`.
For example:
```shell
earthly +run --args="-benchtime 10s"
```

## Run on a remote stack

```shell
earthly +run --args="--stack.url=XXX --client.id=XXX --client.secret=XXX"
```

## Run on a remote ledger

```shell
earthly +run --args="--ledger.url=XXX --auth.url=XXX --client.id=XXX --client.secret=XXX"
```

## Results

TPS is included as a benchmark metrics.

You can generate some graphs using the command: 
```
earthly +generate-graphs
```

See generated files in `report` directory.