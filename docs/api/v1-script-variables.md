# Legacy v1 Numscript variables

For POST /v1/{ledger}/transactions, script.vars binds values to the variables declared in script.plain.

Ordinary variables must be JSON strings. Monetary variables use an object with asset and amount fields:

~~~json
{
  "script": {
    "plain": "vars {\n account $user\n monetary $amount\n}\n...",
    "vars": {
      "user": "users:42",
      "amount": {
        "asset": "USD",
        "amount": 100
      }
    }
  }
}
~~~

Bare numbers, booleans, arrays, null, and objects without the monetary fields are invalid variable values. The API
returns HTTP 400 with error code VALIDATION; it does not treat these values as strings or panic while converting the
request.
