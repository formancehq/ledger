# Transaction analysis equivalence: evidence and ownership

The [manifest](transaction-analysis-equivalence.json) covers the result of
`AnalyzeTransactions`, not transaction creation or a general query optimizer.
`DefaultController.AnalyzeTransactions` performs two log scans through a read
handle. `analysis.AnalyzeTransactionsFromIterators` builds an address trie in
the first pass and aggregates normalized flows in the second.

## Evidence contract

For each case, pin the ledger, read horizon, source log set, threshold and
included transactions. Verify whether the read handle gives both scans the
same snapshot before asserting that it does. Build a finite non-empty expected
result independently: raw postings, normalized addresses, flow membership,
transaction and reverted counts, per-asset volume statistics, and order.
Do not call production normalization, signature, grouping, or accumulation
helpers to construct the oracle.

Exercise repeated addresses, one and several postings, differing colors,
reordered postings, thresholds at boundaries, reverted transactions and skipped
logs. Distinguish the collision-safe internal grouping key from the public
display signature. For error cases, inject a failure in each pass after a
positive witness, then observe the controller and streaming caller's final
result or terminal error. Progress messages are not completion evidence.

## Ownership boundaries

`query-semantic-equivalence` owns predicate selection and general query plans.
`read-consistency-projections` owns the temporal and projection guarantees of
the source read. `accounting-invariants` owns the truth of posted amounts and
balances. `api-boundary-contracts` owns request and stream translation. This
domain owns the analysis algorithm and its result after the input set and
values are established. Assign one finding to the correction owner.

Ambiguous threshold, normalization or concurrent-write expectations become
questions until an authoritative contract establishes them. A test name or
green suite does not prove a case ran; record the actual assertions.
