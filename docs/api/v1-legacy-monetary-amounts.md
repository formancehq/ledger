# Legacy v1 monetary script amounts

This note applies to monetary variables in script.vars for legacy v1 transaction scripts and to legacy script payloads
in JSON bulk CREATE_TRANSACTION elements.

The amount must be an integer. Fractional amounts are rejected with HTTP 400 and error code VALIDATION. In a JSON
bulk request, the affected element is reported as VALIDATION and its transaction is not sent to the controller.

JSON numeric tokens are parsed exactly rather than first being rounded through a floating-point value. For example,
9007199254740993 remains 9007199254740993, and an integral exponent form such as 1e3 represents 1000. If a client's
JSON encoder cannot preserve large integer tokens, send the amount as a decimal string instead:

~~~json
{
  "script": {
    "plain": "vars {\n monetary $amount\n}\n...",
    "vars": {
      "amount": {
        "asset": "USD",
        "amount": "9007199254740993"
      }
    }
  }
}
~~~

Code paths that supply an already-decoded float64 (rather than JSON) accept only whole values within the IEEE-754
safe-integer range, -(2^53 - 1) through 2^53 - 1. Fractional and out-of-range floats return an ErrInvalidMonetaryAmount
error.
