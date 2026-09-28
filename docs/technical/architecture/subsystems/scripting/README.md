# Scripting

Numscript is the DSL used to express financial transactions as deterministic postings. Admission resolves a numscript program against preloaded state, declares what it reads and writes, and compiles it to VM bytecode bound to the order; the FSM then executes that bytecode (or interprets the text when no bytecode was bound) to produce the postings. A versioned global library lets clients reuse named programs across requests.

## Documents

| Document | Description |
|----------|-------------|
| [numscript-library.md](numscript-library.md) | Global repository for reusable numscript programs with semantic versioning. |

## Related

- [Admission](../admission/) — the consumer that executes numscript during request processing.
- [API](../api/) — the surface through which programs are published / referenced.
