# Chart of accounts properties

The `chart` of a schema describes which account addresses a ledger accepts.
Keys without a leading `.` are address segments, and a segment whose name
starts with `$` is a variable segment that matches any value at that position.
Keys that start with `.` are properties of the segment they appear in.

## Supported properties

Only four properties are accepted, and names are case-sensitive:

| Property    | Allowed on             | Meaning                                                     |
|-------------|------------------------|-------------------------------------------------------------|
| `.self`     | Any segment            | Makes a segment with sub-segments an account. Must be `{}`. |
| `.metadata` | Account segments       | Default metadata for matching accounts, by metadata key.    |
| `.rules`    | Account segments       | Account rules.                                              |
| `.pattern`  | Variable segments only | Regular expression the segment value must match.            |

A segment is an account when it has no sub-segments or when it sets `.self`.
The top level of the chart holds segments only, never properties.

## Validation behavior

`POST /v2/{ledger}/schemas/{version}` returns HTTP 400 with the `VALIDATION`
error code when the chart:

- Uses any other dot-prefixed key on any segment. Typos such as `.patern` or
  `.metadatas` and wrong-case variants such as `.Pattern` or `.SELF` fail with
  `unknown property <key>`. Earlier releases accepted these keys and dropped
  them, so a misspelled `.pattern` left a variable segment unconstrained.
- Sets `.pattern` on a fixed segment, including a top-level segment, which
  fails with `cannot have a pattern on a fixed segment`.
- Sets `.pattern` to a value that is not a string or is not a valid Go (RE2)
  regular expression.

A variable segment without `.pattern` accepts any value. The pattern is not
anchored, so `[0-9]+` also accepts `abc123`; use `^` and `$` to match the
whole segment:

~~~json
{
  "chart": {
    "users": {
      "$userID": {
        ".pattern": "^[0-9]{16}$",
        ".metadata": { "type": { "default": "user" } }
      }
    }
  }
}
~~~

Schemas inserted before this check still load. A stored chart is written with
only the supported properties, so an unknown key never reached storage.

## Implementation notes

- Parsing lives in `internal/chart.go` (`ChartOfAccounts.UnmarshalJSON` and
  `ChartSegment.UnmarshalJSON`). The parent segment reads and validates
  `.pattern` on its children, so `ChartSegment.UnmarshalJSON` skips that key
  instead of rejecting it.
- The v2 schema insert controller
  (`internal/api/v2/controllers_schema_insert.go`) maps the decode error to
  HTTP 400 `VALIDATION`.
