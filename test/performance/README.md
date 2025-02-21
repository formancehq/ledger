# Performance test

Each feature is tested against a test script involving a transaction from a source to a destination.
The benchmarks also test the minimal set of features and the full set of features.

Refer to [features](../../CONTRIBUTING.md/#features) for more information about features.

Scripts can be found in directory [scripts](./scripts).

## Run locally

```shell
just run
```

You can pass additional arguments (the underlying command is a standard `go test -bench=.`) using the flag `--args`.
For example:
```shell
just run "-benchtime 10s"
```

## Run on a remote stack

```shell
just run "--stack.url=XXX --client.id=XXX --client.secret=XXX"
```

## Run on a remote ledger

```shell
just run "--ledger.url=XXX --auth.url=XXX --client.id=XXX --client.secret=XXX"
```

## Results

TPS is included as a benchmark metrics.

You can generate some graphs using the command: 
```
just graphs
```

See generated files in `report` directory.