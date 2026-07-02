# Scripting

Numscript is the DSL used to express financial transactions as deterministic postings. The admission path executes a numscript program against preloaded state to produce the postings that get proposed into Raft. A versioned global library lets clients reuse named programs across requests.

## Documents

| Document | Description |
|----------|-------------|
| [numscript-library.md](numscript-library.md) | Global repository for reusable numscript programs with semantic versioning. |

## Related

- [Admission](../admission/) — the consumer that executes numscript during request processing.
- [API](../api/) — the surface through which programs are published / referenced.
