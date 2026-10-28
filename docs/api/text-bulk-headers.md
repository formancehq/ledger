# Text-bulk script headers and retry keys

A text-bulk script starts with `//script` and ends at `//end` or the end of the
input. The idempotency key is optional:

```text
//script
send [USD 100] (
  source = @world
  destination = @bank
)
//end
```

To provide a retry key, use `//script ik=<value>`. The parser rejects:

- a named key without `=` and a value, such as `//script ik`;
- an empty value, such as `//script ik=`;
- duplicate `ik` fields, including a duplicate after an empty value;
- unknown header fields.

Omitting `ik` entirely is still valid; naming it without a value is not.

The header is split at the first `=` only. Additional equals signs, including
base64 padding, are part of the value rather than being truncated. Leading and
internal whitespace in the value is preserved: `//script ik= foo` uses ` foo`,
not `foo`, as its key. The existing normalization of whitespace around the whole
header line is unchanged.

Idempotency keys are matched exactly. Clients retrying an earlier request must
send the same key, including significant whitespace and equals signs; the parser
does not canonicalize those values into a different database lookup key.

An empty script body, whether followed by `//end` or by end of input, is parsed
as an empty string without a panic. It still goes through normal transaction and
script validation; successful text parsing does not make an empty Numscript
transaction valid.
