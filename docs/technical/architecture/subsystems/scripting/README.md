# Scripting

Numscript is the DSL used to express financial transactions as deterministic postings. Admission resolves a Numscript program against preloaded state, declares what it reads and writes, and compiles it to validate and predict effects. The committed order carries the script text or library reference with business variables. Each FSM replica compiles and executes the resolved script with its local VM to produce postings. A versioned global library lets clients reuse named programs across requests.

## Documents

| Document | Description |
|----------|-------------|
| [numscript-library.md](numscript-library.md) | Global repository for reusable numscript programs with semantic versioning. |

## Related

- [Admission](../admission/) — the consumer that executes numscript during request processing.
- [API](../api/) — the surface through which programs are published / referenced.
