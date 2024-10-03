---
title: Ledger API vLEDGER_VERSION
language_tabs:
  - http: HTTP
language_clients:
  - http: ""
toc_footers: []
includes: []
search: false
highlight_theme: darkula
headingLevel: 2

---

<!-- Generator: Widdershins v4.0.1 -->

<h1 id="ledger-api">Ledger API vLEDGER_VERSION</h1>

> Scroll down for code samples, example requests and responses. Select a language for code samples from the tabs above or the mobile navigation menu.

Base URLs:

* <a href="http://localhost:8080/">http://localhost:8080/</a>

# Authentication

- oAuth2 authentication. 

    - Flow: clientCredentials

    - Token URL = [/api/auth/oauth/token](/api/auth/oauth/token)

|Scope|Scope Description|
|---|---|

<h1 id="ledger-api-ledger-v2">ledger.v2</h1>

## v2GetInfo

<a id="opIdv2GetInfo"></a>

> Code samples

```http
GET http://localhost:8080/v2/_info HTTP/1.1
Host: localhost:8080
Accept: application/json

```

`GET /v2/_info`

*Show server information*

> Example responses

> 200 Response

```json
{
  "server": "string",
  "version": "string"
}
```

<h3 id="v2getinfo-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|OK|[V2ConfigInfo](#schemav2configinfo)|
|default|Default|Error|[V2ErrorResponse](#schemav2errorresponse)|
|5XX|Unknown|Error|[V2ErrorResponse](#schemav2errorresponse)|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
Authorization ( Scopes: ledger:read )
</aside>

## v2ListLedgers

<a id="opIdv2ListLedgers"></a>

> Code samples

```http
GET http://localhost:8080/v2 HTTP/1.1
Host: localhost:8080
Accept: application/json

```

`GET /v2`

*List ledgers*

<h3 id="v2listledgers-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|pageSize|query|integer(int64)|false|The maximum number of results to return per page.|
|cursor|query|string|false|Parameter used in pagination requests. Maximum page size is set to 15.|

#### Detailed descriptions

**pageSize**: The maximum number of results to return per page.

**cursor**: Parameter used in pagination requests. Maximum page size is set to 15.
Set to the value of next for the next page of results.
Set to the value of previous for the previous page of results.
No other parameters can be set when this parameter is set.

> Example responses

> 200 Response

```json
{
  "cursor": {
    "pageSize": 15,
    "hasMore": false,
    "previous": "YXVsdCBhbmQgYSBtYXhpbXVtIG1heF9yZXN1bHRzLol=",
    "next": "",
    "data": [
      {
        "name": "string",
        "addedAt": "2019-08-24T14:15:22Z",
        "bucket": "string",
        "metadata": {
          "admin": "true"
        }
      }
    ]
  }
}
```

<h3 id="v2listledgers-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|OK|[V2LedgerListResponse](#schemav2ledgerlistresponse)|
|default|Default|Error|[V2ErrorResponse](#schemav2errorresponse)|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
Authorization ( Scopes: ledger:read )
</aside>

## v2GetLedger

<a id="opIdv2GetLedger"></a>

> Code samples

```http
GET http://localhost:8080/v2/{ledger} HTTP/1.1
Host: localhost:8080
Accept: application/json

```

`GET /v2/{ledger}`

*Get a ledger*

<h3 id="v2getledger-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|ledger|path|string|true|Name of the ledger.|

> Example responses

> 200 Response

```json
{
  "data": {
    "name": "string",
    "addedAt": "2019-08-24T14:15:22Z",
    "bucket": "string",
    "metadata": {
      "admin": "true"
    }
  }
}
```

<h3 id="v2getledger-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|OK|[V2GetLedgerResponse](#schemav2getledgerresponse)|
|default|Default|Error|[V2ErrorResponse](#schemav2errorresponse)|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
Authorization ( Scopes: ledger:read )
</aside>

## v2CreateLedger

<a id="opIdv2CreateLedger"></a>

> Code samples

```http
POST http://localhost:8080/v2/{ledger} HTTP/1.1
Host: localhost:8080
Content-Type: application/json
Accept: application/json

```

`POST /v2/{ledger}`

*Create a ledger*

> Body parameter

```json
{
  "bucket": "string",
  "metadata": {
    "admin": "true"
  },
  "features": {
    "property1": "string",
    "property2": "string"
  }
}
```

<h3 id="v2createledger-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|body|body|[V2CreateLedgerRequest](#schemav2createledgerrequest)|false|none|
|ledger|path|string|true|Name of the ledger.|

> Example responses

> default Response

```json
{
  "errorCode": "VALIDATION",
  "errorMessage": "[VALIDATION] invalid 'cursor' query param",
  "details": "https://play.numscript.org/?payload=eyJlcnJvciI6ImFjY291bnQgaGFkIGluc3VmZmljaWVudCBmdW5kcyJ9"
}
```

<h3 id="v2createledger-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|204|[No Content](https://tools.ietf.org/html/rfc7231#section-6.3.5)|OK|None|
|default|Default|Error|[V2ErrorResponse](#schemav2errorresponse)|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
Authorization ( Scopes: ledger:write )
</aside>

## v2UpdateLedgerMetadata

<a id="opIdv2UpdateLedgerMetadata"></a>

> Code samples

```http
PUT http://localhost:8080/v2/{ledger}/metadata HTTP/1.1
Host: localhost:8080
Content-Type: application/json
Accept: application/json

```

`PUT /v2/{ledger}/metadata`

*Update ledger metadata*

> Body parameter

```json
{
  "admin": "true"
}
```

<h3 id="v2updateledgermetadata-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|body|body|[V2Metadata](#schemav2metadata)|false|none|
|ledger|path|string|true|Name of the ledger.|

> Example responses

> default Response

```json
{
  "errorCode": "VALIDATION",
  "errorMessage": "[VALIDATION] invalid 'cursor' query param",
  "details": "https://play.numscript.org/?payload=eyJlcnJvciI6ImFjY291bnQgaGFkIGluc3VmZmljaWVudCBmdW5kcyJ9"
}
```

<h3 id="v2updateledgermetadata-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|204|[No Content](https://tools.ietf.org/html/rfc7231#section-6.3.5)|OK|None|
|default|Default|Error|[V2ErrorResponse](#schemav2errorresponse)|
|5XX|Unknown|Error|[V2ErrorResponse](#schemav2errorresponse)|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
Authorization ( Scopes: ledger:write )
</aside>

## v2DeleteLedgerMetadata

<a id="opIdv2DeleteLedgerMetadata"></a>

> Code samples

```http
DELETE http://localhost:8080/v2/{ledger}/metadata/{key} HTTP/1.1
Host: localhost:8080
Accept: application/json

```

`DELETE /v2/{ledger}/metadata/{key}`

*Delete ledger metadata by key*

<h3 id="v2deleteledgermetadata-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|ledger|path|string|true|Name of the ledger.|
|key|path|string|true|Key to remove.|

> Example responses

> default Response

```json
{
  "errorCode": "VALIDATION",
  "errorMessage": "[VALIDATION] invalid 'cursor' query param",
  "details": "https://play.numscript.org/?payload=eyJlcnJvciI6ImFjY291bnQgaGFkIGluc3VmZmljaWVudCBmdW5kcyJ9"
}
```

<h3 id="v2deleteledgermetadata-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|204|[No Content](https://tools.ietf.org/html/rfc7231#section-6.3.5)|OK|None|
|default|Default|Error|[V2ErrorResponse](#schemav2errorresponse)|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
Authorization ( Scopes: ledger:write )
</aside>

## v2GetLedgerInfo

<a id="opIdv2GetLedgerInfo"></a>

> Code samples

```http
GET http://localhost:8080/v2/{ledger}/_info HTTP/1.1
Host: localhost:8080
Accept: application/json

```

`GET /v2/{ledger}/_info`

*Get information about a ledger*

<h3 id="v2getledgerinfo-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|ledger|path|string|true|Name of the ledger.|

> Example responses

> 200 Response

```json
{
  "data": {
    "name": "ledger001",
    "storage": {
      "migrations": [
        {
          "version": 11,
          "name": "migrations:001",
          "date": "2019-08-24T14:15:22Z",
          "state": "TO DO"
        }
      ]
    }
  }
}
```

<h3 id="v2getledgerinfo-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|OK|[V2LedgerInfoResponse](#schemav2ledgerinforesponse)|
|default|Default|Error|[V2ErrorResponse](#schemav2errorresponse)|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
Authorization ( Scopes: ledger:read )
</aside>

## v2CreateBulk

<a id="opIdv2CreateBulk"></a>

> Code samples

```http
POST http://localhost:8080/v2/{ledger}/_bulk HTTP/1.1
Host: localhost:8080
Content-Type: application/json
Accept: application/json

```

`POST /v2/{ledger}/_bulk`

*Bulk request*

> Body parameter

```json
[
  {
    "action": "string",
    "ik": "string",
    "data": {
      "timestamp": "2019-08-24T14:15:22Z",
      "postings": [
        {
          "amount": 100,
          "asset": "COIN",
          "destination": "users:002",
          "source": "users:001"
        }
      ],
      "script": {
        "plain": "vars {\naccount $user\n}\nsend [COIN 10] (\n\tsource = @world\n\tdestination = $user\n)\n",
        "vars": {
          "user": "users:042"
        }
      },
      "reference": "ref:001",
      "metadata": {
        "admin": "true"
      }
    }
  }
]
```

<h3 id="v2createbulk-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|ledger|path|string|true|Name of the ledger.|
|body|body|[V2Bulk](#schemav2bulk)|false|none|

> Example responses

> 200 Response

```json
{
  "data": [
    {
      "responseType": "string",
      "data": {
        "insertedAt": "2019-08-24T14:15:22Z",
        "timestamp": "2019-08-24T14:15:22Z",
        "postings": [
          {
            "amount": 100,
            "asset": "COIN",
            "destination": "users:002",
            "source": "users:001"
          }
        ],
        "reference": "ref:001",
        "metadata": {
          "admin": "true"
        },
        "id": 0,
        "reverted": true,
        "revertedAt": "2019-08-24T14:15:22Z",
        "preCommitVolumes": {
          "orders:1": {
            "USD": {
              "input": 100,
              "output": 10,
              "balance": 90
            }
          },
          "orders:2": {
            "USD": {
              "input": 100,
              "output": 10,
              "balance": 90
            }
          }
        },
        "postCommitVolumes": {
          "orders:1": {
            "USD": {
              "input": 100,
              "output": 10,
              "balance": 90
            }
          },
          "orders:2": {
            "USD": {
              "input": 100,
              "output": 10,
              "balance": 90
            }
          }
        },
        "preCommitEffectiveVolumes": {
          "orders:1": {
            "USD": {
              "input": 100,
              "output": 10,
              "balance": 90
            }
          },
          "orders:2": {
            "USD": {
              "input": 100,
              "output": 10,
              "balance": 90
            }
          }
        },
        "postCommitEffectiveVolumes": {
          "orders:1": {
            "USD": {
              "input": 100,
              "output": 10,
              "balance": 90
            }
          },
          "orders:2": {
            "USD": {
              "input": 100,
              "output": 10,
              "balance": 90
            }
          }
        }
      }
    }
  ]
}
```

<h3 id="v2createbulk-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|OK|[V2BulkResponse](#schemav2bulkresponse)|
|400|[Bad Request](https://tools.ietf.org/html/rfc7231#section-6.5.1)|OK|[V2BulkResponse](#schemav2bulkresponse)|
|default|Default|Error|[V2ErrorResponse](#schemav2errorresponse)|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
Authorization ( Scopes: ledger:write )
</aside>

## v2CountAccounts

<a id="opIdv2CountAccounts"></a>

> Code samples

```http
HEAD http://localhost:8080/v2/{ledger}/accounts HTTP/1.1
Host: localhost:8080
Content-Type: application/json
Accept: application/json

```

`HEAD /v2/{ledger}/accounts`

*Count the accounts from a ledger*

> Body parameter

```json
{}
```

<h3 id="v2countaccounts-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|ledger|path|string|true|Name of the ledger.|
|pit|query|string(date-time)|false|none|
|body|body|object|false|none|

> Example responses

> default Response

```json
{
  "errorCode": "VALIDATION",
  "errorMessage": "[VALIDATION] invalid 'cursor' query param",
  "details": "https://play.numscript.org/?payload=eyJlcnJvciI6ImFjY291bnQgaGFkIGluc3VmZmljaWVudCBmdW5kcyJ9"
}
```

<h3 id="v2countaccounts-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|204|[No Content](https://tools.ietf.org/html/rfc7231#section-6.3.5)|OK|None|
|default|Default|Error|[V2ErrorResponse](#schemav2errorresponse)|

### Response Headers

|Status|Header|Type|Format|Description|
|---|---|---|---|---|
|204|Count|integer|bigint|none|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
Authorization ( Scopes: ledger:read )
</aside>

## v2ListAccounts

<a id="opIdv2ListAccounts"></a>

> Code samples

```http
GET http://localhost:8080/v2/{ledger}/accounts HTTP/1.1
Host: localhost:8080
Content-Type: application/json
Accept: application/json

```

`GET /v2/{ledger}/accounts`

*List accounts from a ledger*

List accounts from a ledger, sorted by address in descending order.

> Body parameter

```json
{}
```

<h3 id="v2listaccounts-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|ledger|path|string|true|Name of the ledger.|
|pageSize|query|integer(int64)|false|The maximum number of results to return per page.|
|cursor|query|string|false|Parameter used in pagination requests. Maximum page size is set to 15.|
|expand|query|string|false|none|
|pit|query|string(date-time)|false|none|
|body|body|object|false|none|

#### Detailed descriptions

**pageSize**: The maximum number of results to return per page.

**cursor**: Parameter used in pagination requests. Maximum page size is set to 15.
Set to the value of next for the next page of results.
Set to the value of previous for the previous page of results.
No other parameters can be set when this parameter is set.

> Example responses

> 200 Response

```json
{
  "cursor": {
    "pageSize": 15,
    "hasMore": false,
    "previous": "YXVsdCBhbmQgYSBtYXhpbXVtIG1heF9yZXN1bHRzLol=",
    "next": "",
    "data": [
      {
        "address": "users:001",
        "metadata": {
          "admin": "true"
        },
        "volumes": {
          "USD": {
            "input": 100,
            "output": 10,
            "balance": 90
          },
          "EUR": {
            "input": 100,
            "output": 10,
            "balance": 90
          }
        },
        "effectiveVolumes": {
          "USD": {
            "input": 100,
            "output": 10,
            "balance": 90
          },
          "EUR": {
            "input": 100,
            "output": 10,
            "balance": 90
          }
        }
      }
    ]
  }
}
```

<h3 id="v2listaccounts-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|OK|[V2AccountsCursorResponse](#schemav2accountscursorresponse)|
|default|Default|Error|[V2ErrorResponse](#schemav2errorresponse)|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
Authorization ( Scopes: ledger:read )
</aside>

## v2GetAccount

<a id="opIdv2GetAccount"></a>

> Code samples

```http
GET http://localhost:8080/v2/{ledger}/accounts/{address} HTTP/1.1
Host: localhost:8080
Accept: application/json

```

`GET /v2/{ledger}/accounts/{address}`

*Get account by its address*

<h3 id="v2getaccount-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|ledger|path|string|true|Name of the ledger.|
|address|path|string|true|Exact address of the account. It must match the following regular expressions pattern:|
|expand|query|string|false|none|
|pit|query|string(date-time)|false|none|

#### Detailed descriptions

**address**: Exact address of the account. It must match the following regular expressions pattern:
```
^\w+(:\w+)*$
```

> Example responses

> 200 Response

```json
{
  "data": {
    "address": "users:001",
    "metadata": {
      "admin": "true"
    },
    "volumes": {
      "USD": {
        "input": 100,
        "output": 10,
        "balance": 90
      },
      "EUR": {
        "input": 100,
        "output": 10,
        "balance": 90
      }
    },
    "effectiveVolumes": {
      "USD": {
        "input": 100,
        "output": 10,
        "balance": 90
      },
      "EUR": {
        "input": 100,
        "output": 10,
        "balance": 90
      }
    }
  }
}
```

<h3 id="v2getaccount-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|OK|[V2AccountResponse](#schemav2accountresponse)|
|default|Default|Error|[V2ErrorResponse](#schemav2errorresponse)|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
Authorization ( Scopes: ledger:read )
</aside>

## v2AddMetadataToAccount

<a id="opIdv2AddMetadataToAccount"></a>

> Code samples

```http
POST http://localhost:8080/v2/{ledger}/accounts/{address}/metadata HTTP/1.1
Host: localhost:8080
Content-Type: application/json
Accept: application/json
Idempotency-Key: string

```

`POST /v2/{ledger}/accounts/{address}/metadata`

*Add metadata to an account*

> Body parameter

```json
{
  "admin": "true"
}
```

<h3 id="v2addmetadatatoaccount-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|ledger|path|string|true|Name of the ledger.|
|address|path|string|true|Exact address of the account. It must match the following regular expressions pattern:|
|dryRun|query|boolean|false|Set the dry run mode. Dry run mode doesn't add the logs to the database or publish a message to the message broker.|
|Idempotency-Key|header|string|false|Use an idempotency key|
|body|body|[V2Metadata](#schemav2metadata)|true|metadata|

#### Detailed descriptions

**address**: Exact address of the account. It must match the following regular expressions pattern:
```
^\w+(:\w+)*$
```

> Example responses

> default Response

```json
{
  "errorCode": "VALIDATION",
  "errorMessage": "[VALIDATION] invalid 'cursor' query param",
  "details": "https://play.numscript.org/?payload=eyJlcnJvciI6ImFjY291bnQgaGFkIGluc3VmZmljaWVudCBmdW5kcyJ9"
}
```

<h3 id="v2addmetadatatoaccount-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|204|[No Content](https://tools.ietf.org/html/rfc7231#section-6.3.5)|No Content|None|
|default|Default|Error|[V2ErrorResponse](#schemav2errorresponse)|

<h3 id="v2addmetadatatoaccount-responseschema">Response Schema</h3>

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
Authorization ( Scopes: ledger:write )
</aside>

## v2DeleteAccountMetadata

<a id="opIdv2DeleteAccountMetadata"></a>

> Code samples

```http
DELETE http://localhost:8080/v2/{ledger}/accounts/{address}/metadata/{key} HTTP/1.1
Host: localhost:8080
Accept: application/json

```

`DELETE /v2/{ledger}/accounts/{address}/metadata/{key}`

*Delete metadata by key*

Delete metadata by key

<h3 id="v2deleteaccountmetadata-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|ledger|path|string|true|Name of the ledger.|
|address|path|string|true|Account address|
|key|path|string|true|The key to remove.|

> Example responses

> default Response

```json
{
  "errorCode": "VALIDATION",
  "errorMessage": "[VALIDATION] invalid 'cursor' query param",
  "details": "https://play.numscript.org/?payload=eyJlcnJvciI6ImFjY291bnQgaGFkIGluc3VmZmljaWVudCBmdW5kcyJ9"
}
```

<h3 id="v2deleteaccountmetadata-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|2XX|Unknown|Key deleted|None|
|default|Default|Error|[V2ErrorResponse](#schemav2errorresponse)|

<h3 id="v2deleteaccountmetadata-responseschema">Response Schema</h3>

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
Authorization ( Scopes: ledger:write )
</aside>

## v2ReadStats

<a id="opIdv2ReadStats"></a>

> Code samples

```http
GET http://localhost:8080/v2/{ledger}/stats HTTP/1.1
Host: localhost:8080
Accept: application/json

```

`GET /v2/{ledger}/stats`

*Get statistics from a ledger*

Get statistics from a ledger. (aggregate metrics on accounts and transactions)

<h3 id="v2readstats-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|ledger|path|string|true|name of the ledger|

> Example responses

> 200 Response

```json
{
  "data": {
    "accounts": 0,
    "transactions": 0
  }
}
```

<h3 id="v2readstats-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|OK|[V2StatsResponse](#schemav2statsresponse)|
|default|Default|Error|[V2ErrorResponse](#schemav2errorresponse)|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
Authorization ( Scopes: ledger:read )
</aside>

## v2CountTransactions

<a id="opIdv2CountTransactions"></a>

> Code samples

```http
HEAD http://localhost:8080/v2/{ledger}/transactions HTTP/1.1
Host: localhost:8080
Content-Type: application/json
Accept: application/json

```

`HEAD /v2/{ledger}/transactions`

*Count the transactions from a ledger*

> Body parameter

```json
{}
```

<h3 id="v2counttransactions-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|ledger|path|string|true|Name of the ledger.|
|pit|query|string(date-time)|false|none|
|body|body|object|false|none|

> Example responses

> default Response

```json
{
  "errorCode": "VALIDATION",
  "errorMessage": "[VALIDATION] invalid 'cursor' query param",
  "details": "https://play.numscript.org/?payload=eyJlcnJvciI6ImFjY291bnQgaGFkIGluc3VmZmljaWVudCBmdW5kcyJ9"
}
```

<h3 id="v2counttransactions-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|204|[No Content](https://tools.ietf.org/html/rfc7231#section-6.3.5)|OK|None|
|default|Default|Error|[V2ErrorResponse](#schemav2errorresponse)|

### Response Headers

|Status|Header|Type|Format|Description|
|---|---|---|---|---|
|204|Count|integer|int64|none|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
Authorization ( Scopes: ledger:read )
</aside>

## v2ListTransactions

<a id="opIdv2ListTransactions"></a>

> Code samples

```http
GET http://localhost:8080/v2/{ledger}/transactions HTTP/1.1
Host: localhost:8080
Content-Type: application/json
Accept: application/json

```

`GET /v2/{ledger}/transactions`

*List transactions from a ledger*

List transactions from a ledger, sorted by id in descending order.

> Body parameter

```json
{}
```

<h3 id="v2listtransactions-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|ledger|path|string|true|Name of the ledger.|
|pageSize|query|integer(int64)|false|The maximum number of results to return per page.|
|cursor|query|string|false|Parameter used in pagination requests. Maximum page size is set to 15.|
|expand|query|string|false|none|
|pit|query|string(date-time)|false|none|
|order|query|string|false|none|
|reverse|query|boolean|false|none|
|body|body|object|false|none|

#### Detailed descriptions

**pageSize**: The maximum number of results to return per page.

**cursor**: Parameter used in pagination requests. Maximum page size is set to 15.
Set to the value of next for the next page of results.
Set to the value of previous for the previous page of results.
No other parameters can be set when this parameter is set.

#### Enumerated Values

|Parameter|Value|
|---|---|
|order|effective|

> Example responses

> 200 Response

```json
{
  "cursor": {
    "pageSize": 15,
    "hasMore": false,
    "previous": "YXVsdCBhbmQgYSBtYXhpbXVtIG1heF9yZXN1bHRzLol=",
    "next": "",
    "data": [
      {
        "insertedAt": "2019-08-24T14:15:22Z",
        "timestamp": "2019-08-24T14:15:22Z",
        "postings": [
          {
            "amount": 100,
            "asset": "COIN",
            "destination": "users:002",
            "source": "users:001"
          }
        ],
        "reference": "ref:001",
        "metadata": {
          "admin": "true"
        },
        "id": 0,
        "reverted": true,
        "revertedAt": "2019-08-24T14:15:22Z",
        "preCommitVolumes": {
          "orders:1": {
            "USD": {
              "input": 100,
              "output": 10,
              "balance": 90
            }
          },
          "orders:2": {
            "USD": {
              "input": 100,
              "output": 10,
              "balance": 90
            }
          }
        },
        "postCommitVolumes": {
          "orders:1": {
            "USD": {
              "input": 100,
              "output": 10,
              "balance": 90
            }
          },
          "orders:2": {
            "USD": {
              "input": 100,
              "output": 10,
              "balance": 90
            }
          }
        },
        "preCommitEffectiveVolumes": {
          "orders:1": {
            "USD": {
              "input": 100,
              "output": 10,
              "balance": 90
            }
          },
          "orders:2": {
            "USD": {
              "input": 100,
              "output": 10,
              "balance": 90
            }
          }
        },
        "postCommitEffectiveVolumes": {
          "orders:1": {
            "USD": {
              "input": 100,
              "output": 10,
              "balance": 90
            }
          },
          "orders:2": {
            "USD": {
              "input": 100,
              "output": 10,
              "balance": 90
            }
          }
        }
      }
    ]
  }
}
```

<h3 id="v2listtransactions-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|OK|[V2TransactionsCursorResponse](#schemav2transactionscursorresponse)|
|default|Default|Error|[V2ErrorResponse](#schemav2errorresponse)|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
Authorization ( Scopes: ledger:read )
</aside>

## v2CreateTransaction

<a id="opIdv2CreateTransaction"></a>

> Code samples

```http
POST http://localhost:8080/v2/{ledger}/transactions HTTP/1.1
Host: localhost:8080
Content-Type: application/json
Accept: application/json
Idempotency-Key: string

```

`POST /v2/{ledger}/transactions`

*Create a new transaction to a ledger*

> Body parameter

```json
{
  "timestamp": "2019-08-24T14:15:22Z",
  "postings": [
    {
      "amount": 100,
      "asset": "COIN",
      "destination": "users:002",
      "source": "users:001"
    }
  ],
  "script": {
    "plain": "vars {\naccount $user\n}\nsend [COIN 10] (\n\tsource = @world\n\tdestination = $user\n)\n",
    "vars": {
      "user": "users:042"
    }
  },
  "reference": "ref:001",
  "metadata": {
    "admin": "true"
  }
}
```

<h3 id="v2createtransaction-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|ledger|path|string|true|Name of the ledger.|
|dryRun|query|boolean|false|Set the dryRun mode. dry run mode doesn't add the logs to the database or publish a message to the message broker.|
|Idempotency-Key|header|string|false|Use an idempotency key|
|force|query|boolean|false|Disable balance checks when passing postings|
|body|body|[V2PostTransaction](#schemav2posttransaction)|true|The request body must contain at least one of the following objects:|

#### Detailed descriptions

**body**: The request body must contain at least one of the following objects:
  - `postings`: suitable for simple transactions
  - `script`: enabling more complex transactions with Numscript

> Example responses

> 200 Response

```json
{
  "data": {
    "insertedAt": "2019-08-24T14:15:22Z",
    "timestamp": "2019-08-24T14:15:22Z",
    "postings": [
      {
        "amount": 100,
        "asset": "COIN",
        "destination": "users:002",
        "source": "users:001"
      }
    ],
    "reference": "ref:001",
    "metadata": {
      "admin": "true"
    },
    "id": 0,
    "reverted": true,
    "revertedAt": "2019-08-24T14:15:22Z",
    "preCommitVolumes": {
      "orders:1": {
        "USD": {
          "input": 100,
          "output": 10,
          "balance": 90
        }
      },
      "orders:2": {
        "USD": {
          "input": 100,
          "output": 10,
          "balance": 90
        }
      }
    },
    "postCommitVolumes": {
      "orders:1": {
        "USD": {
          "input": 100,
          "output": 10,
          "balance": 90
        }
      },
      "orders:2": {
        "USD": {
          "input": 100,
          "output": 10,
          "balance": 90
        }
      }
    },
    "preCommitEffectiveVolumes": {
      "orders:1": {
        "USD": {
          "input": 100,
          "output": 10,
          "balance": 90
        }
      },
      "orders:2": {
        "USD": {
          "input": 100,
          "output": 10,
          "balance": 90
        }
      }
    },
    "postCommitEffectiveVolumes": {
      "orders:1": {
        "USD": {
          "input": 100,
          "output": 10,
          "balance": 90
        }
      },
      "orders:2": {
        "USD": {
          "input": 100,
          "output": 10,
          "balance": 90
        }
      }
    }
  }
}
```

<h3 id="v2createtransaction-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|OK|[V2CreateTransactionResponse](#schemav2createtransactionresponse)|
|default|Default|Error|[V2ErrorResponse](#schemav2errorresponse)|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
Authorization ( Scopes: ledger:write )
</aside>

## v2GetTransaction

<a id="opIdv2GetTransaction"></a>

> Code samples

```http
GET http://localhost:8080/v2/{ledger}/transactions/{id} HTTP/1.1
Host: localhost:8080
Accept: application/json

```

`GET /v2/{ledger}/transactions/{id}`

*Get transaction from a ledger by its ID*

<h3 id="v2gettransaction-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|ledger|path|string|true|Name of the ledger.|
|id|path|integer(bigint)|true|Transaction ID.|
|expand|query|string|false|none|
|pit|query|string(date-time)|false|none|

> Example responses

> 200 Response

```json
{
  "data": {
    "insertedAt": "2019-08-24T14:15:22Z",
    "timestamp": "2019-08-24T14:15:22Z",
    "postings": [
      {
        "amount": 100,
        "asset": "COIN",
        "destination": "users:002",
        "source": "users:001"
      }
    ],
    "reference": "ref:001",
    "metadata": {
      "admin": "true"
    },
    "id": 0,
    "reverted": true,
    "revertedAt": "2019-08-24T14:15:22Z",
    "preCommitVolumes": {
      "orders:1": {
        "USD": {
          "input": 100,
          "output": 10,
          "balance": 90
        }
      },
      "orders:2": {
        "USD": {
          "input": 100,
          "output": 10,
          "balance": 90
        }
      }
    },
    "postCommitVolumes": {
      "orders:1": {
        "USD": {
          "input": 100,
          "output": 10,
          "balance": 90
        }
      },
      "orders:2": {
        "USD": {
          "input": 100,
          "output": 10,
          "balance": 90
        }
      }
    },
    "preCommitEffectiveVolumes": {
      "orders:1": {
        "USD": {
          "input": 100,
          "output": 10,
          "balance": 90
        }
      },
      "orders:2": {
        "USD": {
          "input": 100,
          "output": 10,
          "balance": 90
        }
      }
    },
    "postCommitEffectiveVolumes": {
      "orders:1": {
        "USD": {
          "input": 100,
          "output": 10,
          "balance": 90
        }
      },
      "orders:2": {
        "USD": {
          "input": 100,
          "output": 10,
          "balance": 90
        }
      }
    }
  }
}
```

<h3 id="v2gettransaction-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|OK|[V2GetTransactionResponse](#schemav2gettransactionresponse)|
|default|Default|Error|[V2ErrorResponse](#schemav2errorresponse)|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
Authorization ( Scopes: ledger:read )
</aside>

## v2AddMetadataOnTransaction

<a id="opIdv2AddMetadataOnTransaction"></a>

> Code samples

```http
POST http://localhost:8080/v2/{ledger}/transactions/{id}/metadata HTTP/1.1
Host: localhost:8080
Content-Type: application/json
Accept: application/json
Idempotency-Key: string

```

`POST /v2/{ledger}/transactions/{id}/metadata`

*Set the metadata of a transaction by its ID*

> Body parameter

```json
{
  "admin": "true"
}
```

<h3 id="v2addmetadataontransaction-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|ledger|path|string|true|Name of the ledger.|
|id|path|integer(bigint)|true|Transaction ID.|
|dryRun|query|boolean|false|Set the dryRun mode. Dry run mode doesn't add the logs to the database or publish a message to the message broker.|
|Idempotency-Key|header|string|false|Use an idempotency key|
|body|body|[V2Metadata](#schemav2metadata)|false|metadata|

> Example responses

> default Response

```json
{
  "errorCode": "VALIDATION",
  "errorMessage": "[VALIDATION] invalid 'cursor' query param",
  "details": "https://play.numscript.org/?payload=eyJlcnJvciI6ImFjY291bnQgaGFkIGluc3VmZmljaWVudCBmdW5kcyJ9"
}
```

<h3 id="v2addmetadataontransaction-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|204|[No Content](https://tools.ietf.org/html/rfc7231#section-6.3.5)|No Content|None|
|default|Default|Error|[V2ErrorResponse](#schemav2errorresponse)|

<h3 id="v2addmetadataontransaction-responseschema">Response Schema</h3>

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
Authorization ( Scopes: ledger:write )
</aside>

## v2DeleteTransactionMetadata

<a id="opIdv2DeleteTransactionMetadata"></a>

> Code samples

```http
DELETE http://localhost:8080/v2/{ledger}/transactions/{id}/metadata/{key} HTTP/1.1
Host: localhost:8080
Accept: application/json

```

`DELETE /v2/{ledger}/transactions/{id}/metadata/{key}`

*Delete metadata by key*

Delete metadata by key

<h3 id="v2deletetransactionmetadata-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|ledger|path|string|true|Name of the ledger.|
|id|path|integer(bigint)|true|Transaction ID.|
|key|path|string|true|The key to remove.|

> Example responses

> default Response

```json
{
  "errorCode": "VALIDATION",
  "errorMessage": "[VALIDATION] invalid 'cursor' query param",
  "details": "https://play.numscript.org/?payload=eyJlcnJvciI6ImFjY291bnQgaGFkIGluc3VmZmljaWVudCBmdW5kcyJ9"
}
```

<h3 id="v2deletetransactionmetadata-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|2XX|Unknown|Key deleted|None|
|default|Default|Error|[V2ErrorResponse](#schemav2errorresponse)|

<h3 id="v2deletetransactionmetadata-responseschema">Response Schema</h3>

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
Authorization ( Scopes: ledger:write )
</aside>

## v2RevertTransaction

<a id="opIdv2RevertTransaction"></a>

> Code samples

```http
POST http://localhost:8080/v2/{ledger}/transactions/{id}/revert HTTP/1.1
Host: localhost:8080
Accept: application/json

```

`POST /v2/{ledger}/transactions/{id}/revert`

*Revert a ledger transaction by its ID*

<h3 id="v2reverttransaction-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|ledger|path|string|true|Name of the ledger.|
|id|path|integer(bigint)|true|Transaction ID.|
|force|query|boolean|false|Force revert|
|atEffectiveDate|query|boolean|false|Revert transaction at effective date of the original tx|
|dryRun|query|boolean|false|Set the dryRun mode. dry run mode doesn't add the logs to the database or publish a message to the message broker.|

> Example responses

> 201 Response

```json
{
  "data": {
    "insertedAt": "2019-08-24T14:15:22Z",
    "timestamp": "2019-08-24T14:15:22Z",
    "postings": [
      {
        "amount": 100,
        "asset": "COIN",
        "destination": "users:002",
        "source": "users:001"
      }
    ],
    "reference": "ref:001",
    "metadata": {
      "admin": "true"
    },
    "id": 0,
    "reverted": true,
    "revertedAt": "2019-08-24T14:15:22Z",
    "preCommitVolumes": {
      "orders:1": {
        "USD": {
          "input": 100,
          "output": 10,
          "balance": 90
        }
      },
      "orders:2": {
        "USD": {
          "input": 100,
          "output": 10,
          "balance": 90
        }
      }
    },
    "postCommitVolumes": {
      "orders:1": {
        "USD": {
          "input": 100,
          "output": 10,
          "balance": 90
        }
      },
      "orders:2": {
        "USD": {
          "input": 100,
          "output": 10,
          "balance": 90
        }
      }
    },
    "preCommitEffectiveVolumes": {
      "orders:1": {
        "USD": {
          "input": 100,
          "output": 10,
          "balance": 90
        }
      },
      "orders:2": {
        "USD": {
          "input": 100,
          "output": 10,
          "balance": 90
        }
      }
    },
    "postCommitEffectiveVolumes": {
      "orders:1": {
        "USD": {
          "input": 100,
          "output": 10,
          "balance": 90
        }
      },
      "orders:2": {
        "USD": {
          "input": 100,
          "output": 10,
          "balance": 90
        }
      }
    }
  }
}
```

<h3 id="v2reverttransaction-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|201|[Created](https://tools.ietf.org/html/rfc7231#section-6.3.2)|OK|[V2CreateTransactionResponse](#schemav2createtransactionresponse)|
|default|Default|Error|[V2ErrorResponse](#schemav2errorresponse)|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
Authorization ( Scopes: ledger:write )
</aside>

## v2GetBalancesAggregated

<a id="opIdv2GetBalancesAggregated"></a>

> Code samples

```http
GET http://localhost:8080/v2/{ledger}/aggregate/balances HTTP/1.1
Host: localhost:8080
Content-Type: application/json
Accept: application/json

```

`GET /v2/{ledger}/aggregate/balances`

*Get the aggregated balances from selected accounts*

> Body parameter

```json
{}
```

<h3 id="v2getbalancesaggregated-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|ledger|path|string|true|Name of the ledger.|
|pit|query|string(date-time)|false|none|
|useInsertionDate|query|boolean|false|Use insertion date instead of effective date|
|body|body|object|false|none|

> Example responses

> 200 Response

```json
{
  "data": {
    "USD": 100,
    "EUR": 12
  }
}
```

<h3 id="v2getbalancesaggregated-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|OK|[V2AggregateBalancesResponse](#schemav2aggregatebalancesresponse)|
|default|Default|Error|[V2ErrorResponse](#schemav2errorresponse)|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
Authorization ( Scopes: ledger:read )
</aside>

## v2GetVolumesWithBalances

<a id="opIdv2GetVolumesWithBalances"></a>

> Code samples

```http
GET http://localhost:8080/v2/{ledger}/volumes HTTP/1.1
Host: localhost:8080
Content-Type: application/json
Accept: application/json

```

`GET /v2/{ledger}/volumes`

*Get list of volumes with balances for (account/asset)*

> Body parameter

```json
{}
```

<h3 id="v2getvolumeswithbalances-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|pageSize|query|integer(int64)|false|The maximum number of results to return per page.|
|cursor|query|string|false|Parameter used in pagination requests. Maximum page size is set to 15.|
|ledger|path|string|true|Name of the ledger.|
|endTime|query|string(date-time)|false|none|
|startTime|query|string(date-time)|false|none|
|insertionDate|query|boolean|false|Use insertion date instead of effective date|
|groupBy|query|integer(int64)|false|Group volumes and balance by the level of the segment of the address|
|body|body|object|false|none|

#### Detailed descriptions

**pageSize**: The maximum number of results to return per page.

**cursor**: Parameter used in pagination requests. Maximum page size is set to 15.
Set to the value of next for the next page of results.
Set to the value of previous for the previous page of results.
No other parameters can be set when this parameter is set.

> Example responses

> 200 Response

```json
{
  "cursor": {
    "pageSize": 15,
    "hasMore": false,
    "previous": "YXVsdCBhbmQgYSBtYXhpbXVtIG1heF9yZXN1bHRzLol=",
    "next": "",
    "data": [
      {
        "account": "string",
        "asset": "string",
        "input": 0,
        "output": 0,
        "balance": 0
      }
    ]
  }
}
```

<h3 id="v2getvolumeswithbalances-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|OK|[V2VolumesWithBalanceCursorResponse](#schemav2volumeswithbalancecursorresponse)|
|default|Default|Error|[V2ErrorResponse](#schemav2errorresponse)|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
Authorization ( Scopes: ledger:read )
</aside>

## v2ListLogs

<a id="opIdv2ListLogs"></a>

> Code samples

```http
GET http://localhost:8080/v2/{ledger}/logs HTTP/1.1
Host: localhost:8080
Content-Type: application/json
Accept: application/json

```

`GET /v2/{ledger}/logs`

*List the logs from a ledger*

List the logs from a ledger, sorted by ID in descending order.

> Body parameter

```json
{}
```

<h3 id="v2listlogs-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|ledger|path|string|true|Name of the ledger.|
|pageSize|query|integer(int64)|false|The maximum number of results to return per page.|
|cursor|query|string|false|Parameter used in pagination requests. Maximum page size is set to 15.|
|pit|query|string(date-time)|false|none|
|body|body|object|false|none|

#### Detailed descriptions

**pageSize**: The maximum number of results to return per page.

**cursor**: Parameter used in pagination requests. Maximum page size is set to 15.
Set to the value of next for the next page of results.
Set to the value of previous for the previous page of results.
No other parameters can be set when this parameter is set.

> Example responses

> 200 Response

```json
{
  "cursor": {
    "pageSize": 15,
    "hasMore": false,
    "previous": "YXVsdCBhbmQgYSBtYXhpbXVtIG1heF9yZXN1bHRzLol=",
    "next": "",
    "data": [
      {
        "id": 1234,
        "type": "NEW_TRANSACTION",
        "data": {},
        "hash": "9ee060170400f556b7e1575cb13f9db004f150a08355c7431c62bc639166431e",
        "date": "2019-08-24T14:15:22Z"
      }
    ]
  }
}
```

<h3 id="v2listlogs-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|OK|[V2LogsCursorResponse](#schemav2logscursorresponse)|
|default|Default|Error|[V2ErrorResponse](#schemav2errorresponse)|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
Authorization ( Scopes: ledger:read )
</aside>

## v2ImportLogs

<a id="opIdv2ImportLogs"></a>

> Code samples

```http
POST http://localhost:8080/v2/{ledger}/logs/import HTTP/1.1
Host: localhost:8080
Content-Type: application/octet-stream
Accept: application/json

```

`POST /v2/{ledger}/logs/import`

> Body parameter

```yaml
string

```

<h3 id="v2importlogs-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|ledger|path|string|true|Name of the ledger.|
|body|body|string|false|none|

> Example responses

> default Response

```json
{
  "errorCode": "VALIDATION",
  "errorMessage": "[VALIDATION] invalid 'cursor' query param",
  "details": "https://play.numscript.org/?payload=eyJlcnJvciI6ImFjY291bnQgaGFkIGluc3VmZmljaWVudCBmdW5kcyJ9"
}
```

<h3 id="v2importlogs-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|204|[No Content](https://tools.ietf.org/html/rfc7231#section-6.3.5)|Import OK|None|
|default|Default|Error|[V2ErrorResponse](#schemav2errorresponse)|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
Authorization ( Scopes: ledger:write )
</aside>

## v2ExportLogs

<a id="opIdv2ExportLogs"></a>

> Code samples

```http
POST http://localhost:8080/v2/{ledger}/logs/export HTTP/1.1
Host: localhost:8080
Accept: application/octet-stream

```

`POST /v2/{ledger}/logs/export`

*Export logs*

<h3 id="v2exportlogs-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|ledger|path|string|true|Name of the ledger.|

> Example responses

> default Response

<h3 id="v2exportlogs-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|Import OK|None|
|default|Default|Error|string|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
Authorization ( Scopes: ledger:write )
</aside>

# Schemas

<h2 id="tocS_V2AccountsCursorResponse">V2AccountsCursorResponse</h2>
<!-- backwards compatibility -->
<a id="schemav2accountscursorresponse"></a>
<a id="schema_V2AccountsCursorResponse"></a>
<a id="tocSv2accountscursorresponse"></a>
<a id="tocsv2accountscursorresponse"></a>

```json
{
  "cursor": {
    "pageSize": 15,
    "hasMore": false,
    "previous": "YXVsdCBhbmQgYSBtYXhpbXVtIG1heF9yZXN1bHRzLol=",
    "next": "",
    "data": [
      {
        "address": "users:001",
        "metadata": {
          "admin": "true"
        },
        "volumes": {
          "USD": {
            "input": 100,
            "output": 10,
            "balance": 90
          },
          "EUR": {
            "input": 100,
            "output": 10,
            "balance": 90
          }
        },
        "effectiveVolumes": {
          "USD": {
            "input": 100,
            "output": 10,
            "balance": 90
          },
          "EUR": {
            "input": 100,
            "output": 10,
            "balance": 90
          }
        }
      }
    ]
  }
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|cursor|object|true|none|none|
|» pageSize|integer(int64)|true|none|none|
|» hasMore|boolean|true|none|none|
|» previous|string|false|none|none|
|» next|string|false|none|none|
|» data|[[V2Account](#schemav2account)]|true|none|none|

<h2 id="tocS_V2TransactionsCursorResponse">V2TransactionsCursorResponse</h2>
<!-- backwards compatibility -->
<a id="schemav2transactionscursorresponse"></a>
<a id="schema_V2TransactionsCursorResponse"></a>
<a id="tocSv2transactionscursorresponse"></a>
<a id="tocsv2transactionscursorresponse"></a>

```json
{
  "cursor": {
    "pageSize": 15,
    "hasMore": false,
    "previous": "YXVsdCBhbmQgYSBtYXhpbXVtIG1heF9yZXN1bHRzLol=",
    "next": "",
    "data": [
      {
        "insertedAt": "2019-08-24T14:15:22Z",
        "timestamp": "2019-08-24T14:15:22Z",
        "postings": [
          {
            "amount": 100,
            "asset": "COIN",
            "destination": "users:002",
            "source": "users:001"
          }
        ],
        "reference": "ref:001",
        "metadata": {
          "admin": "true"
        },
        "id": 0,
        "reverted": true,
        "revertedAt": "2019-08-24T14:15:22Z",
        "preCommitVolumes": {
          "orders:1": {
            "USD": {
              "input": 100,
              "output": 10,
              "balance": 90
            }
          },
          "orders:2": {
            "USD": {
              "input": 100,
              "output": 10,
              "balance": 90
            }
          }
        },
        "postCommitVolumes": {
          "orders:1": {
            "USD": {
              "input": 100,
              "output": 10,
              "balance": 90
            }
          },
          "orders:2": {
            "USD": {
              "input": 100,
              "output": 10,
              "balance": 90
            }
          }
        },
        "preCommitEffectiveVolumes": {
          "orders:1": {
            "USD": {
              "input": 100,
              "output": 10,
              "balance": 90
            }
          },
          "orders:2": {
            "USD": {
              "input": 100,
              "output": 10,
              "balance": 90
            }
          }
        },
        "postCommitEffectiveVolumes": {
          "orders:1": {
            "USD": {
              "input": 100,
              "output": 10,
              "balance": 90
            }
          },
          "orders:2": {
            "USD": {
              "input": 100,
              "output": 10,
              "balance": 90
            }
          }
        }
      }
    ]
  }
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|cursor|object|true|none|none|
|» pageSize|integer(int64)|true|none|none|
|» hasMore|boolean|true|none|none|
|» previous|string|false|none|none|
|» next|string|false|none|none|
|» data|[[V2Transaction](#schemav2transaction)]|true|none|none|

<h2 id="tocS_V2LogsCursorResponse">V2LogsCursorResponse</h2>
<!-- backwards compatibility -->
<a id="schemav2logscursorresponse"></a>
<a id="schema_V2LogsCursorResponse"></a>
<a id="tocSv2logscursorresponse"></a>
<a id="tocsv2logscursorresponse"></a>

```json
{
  "cursor": {
    "pageSize": 15,
    "hasMore": false,
    "previous": "YXVsdCBhbmQgYSBtYXhpbXVtIG1heF9yZXN1bHRzLol=",
    "next": "",
    "data": [
      {
        "id": 1234,
        "type": "NEW_TRANSACTION",
        "data": {},
        "hash": "9ee060170400f556b7e1575cb13f9db004f150a08355c7431c62bc639166431e",
        "date": "2019-08-24T14:15:22Z"
      }
    ]
  }
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|cursor|object|true|none|none|
|» pageSize|integer(int64)|true|none|none|
|» hasMore|boolean|true|none|none|
|» previous|string|false|none|none|
|» next|string|false|none|none|
|» data|[[V2Log](#schemav2log)]|true|none|none|

<h2 id="tocS_V2AccountResponse">V2AccountResponse</h2>
<!-- backwards compatibility -->
<a id="schemav2accountresponse"></a>
<a id="schema_V2AccountResponse"></a>
<a id="tocSv2accountresponse"></a>
<a id="tocsv2accountresponse"></a>

```json
{
  "data": {
    "address": "users:001",
    "metadata": {
      "admin": "true"
    },
    "volumes": {
      "USD": {
        "input": 100,
        "output": 10,
        "balance": 90
      },
      "EUR": {
        "input": 100,
        "output": 10,
        "balance": 90
      }
    },
    "effectiveVolumes": {
      "USD": {
        "input": 100,
        "output": 10,
        "balance": 90
      },
      "EUR": {
        "input": 100,
        "output": 10,
        "balance": 90
      }
    }
  }
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|data|[V2Account](#schemav2account)|true|none|none|

<h2 id="tocS_V2AggregateBalancesResponse">V2AggregateBalancesResponse</h2>
<!-- backwards compatibility -->
<a id="schemav2aggregatebalancesresponse"></a>
<a id="schema_V2AggregateBalancesResponse"></a>
<a id="tocSv2aggregatebalancesresponse"></a>
<a id="tocsv2aggregatebalancesresponse"></a>

```json
{
  "data": {
    "USD": 100,
    "EUR": 12
  }
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|data|[V2AssetsBalances](#schemav2assetsbalances)|true|none|none|

<h2 id="tocS_V2VolumesWithBalanceCursorResponse">V2VolumesWithBalanceCursorResponse</h2>
<!-- backwards compatibility -->
<a id="schemav2volumeswithbalancecursorresponse"></a>
<a id="schema_V2VolumesWithBalanceCursorResponse"></a>
<a id="tocSv2volumeswithbalancecursorresponse"></a>
<a id="tocsv2volumeswithbalancecursorresponse"></a>

```json
{
  "cursor": {
    "pageSize": 15,
    "hasMore": false,
    "previous": "YXVsdCBhbmQgYSBtYXhpbXVtIG1heF9yZXN1bHRzLol=",
    "next": "",
    "data": [
      {
        "account": "string",
        "asset": "string",
        "input": 0,
        "output": 0,
        "balance": 0
      }
    ]
  }
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|cursor|object|true|none|none|
|» pageSize|integer(int64)|true|none|none|
|» hasMore|boolean|true|none|none|
|» previous|string|false|none|none|
|» next|string|false|none|none|
|» data|[[V2VolumesWithBalance](#schemav2volumeswithbalance)]|true|none|none|

<h2 id="tocS_V2VolumesWithBalance">V2VolumesWithBalance</h2>
<!-- backwards compatibility -->
<a id="schemav2volumeswithbalance"></a>
<a id="schema_V2VolumesWithBalance"></a>
<a id="tocSv2volumeswithbalance"></a>
<a id="tocsv2volumeswithbalance"></a>

```json
{
  "account": "string",
  "asset": "string",
  "input": 0,
  "output": 0,
  "balance": 0
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|account|string|true|none|none|
|asset|string|true|none|none|
|input|integer(bigint)|true|none|none|
|output|integer(bigint)|true|none|none|
|balance|integer(bigint)|true|none|none|

<h2 id="tocS_V2Metadata">V2Metadata</h2>
<!-- backwards compatibility -->
<a id="schemav2metadata"></a>
<a id="schema_V2Metadata"></a>
<a id="tocSv2metadata"></a>
<a id="tocsv2metadata"></a>

```json
{
  "admin": "true"
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|**additionalProperties**|string|false|none|none|

<h2 id="tocS_V2ConfigInfo">V2ConfigInfo</h2>
<!-- backwards compatibility -->
<a id="schemav2configinfo"></a>
<a id="schema_V2ConfigInfo"></a>
<a id="tocSv2configinfo"></a>
<a id="tocsv2configinfo"></a>

```json
{
  "server": "string",
  "version": "string"
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|server|string|true|none|none|
|version|string|true|none|none|

<h2 id="tocS_V2Account">V2Account</h2>
<!-- backwards compatibility -->
<a id="schemav2account"></a>
<a id="schema_V2Account"></a>
<a id="tocSv2account"></a>
<a id="tocsv2account"></a>

```json
{
  "address": "users:001",
  "metadata": {
    "admin": "true"
  },
  "volumes": {
    "USD": {
      "input": 100,
      "output": 10,
      "balance": 90
    },
    "EUR": {
      "input": 100,
      "output": 10,
      "balance": 90
    }
  },
  "effectiveVolumes": {
    "USD": {
      "input": 100,
      "output": 10,
      "balance": 90
    },
    "EUR": {
      "input": 100,
      "output": 10,
      "balance": 90
    }
  }
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|address|string|true|none|none|
|metadata|object|true|none|none|
|» **additionalProperties**|string|false|none|none|
|volumes|[V2Volumes](#schemav2volumes)|false|none|none|
|effectiveVolumes|[V2Volumes](#schemav2volumes)|false|none|none|

<h2 id="tocS_V2AssetsBalances">V2AssetsBalances</h2>
<!-- backwards compatibility -->
<a id="schemav2assetsbalances"></a>
<a id="schema_V2AssetsBalances"></a>
<a id="tocSv2assetsbalances"></a>
<a id="tocsv2assetsbalances"></a>

```json
{
  "USD": 100,
  "EUR": 12
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|**additionalProperties**|integer(bigint)|false|none|none|

<h2 id="tocS_V2Posting">V2Posting</h2>
<!-- backwards compatibility -->
<a id="schemav2posting"></a>
<a id="schema_V2Posting"></a>
<a id="tocSv2posting"></a>
<a id="tocsv2posting"></a>

```json
{
  "amount": 100,
  "asset": "COIN",
  "destination": "users:002",
  "source": "users:001"
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|amount|integer(bigint)|true|none|none|
|asset|string|true|none|none|
|destination|string|true|none|none|
|source|string|true|none|none|

<h2 id="tocS_V2Transaction">V2Transaction</h2>
<!-- backwards compatibility -->
<a id="schemav2transaction"></a>
<a id="schema_V2Transaction"></a>
<a id="tocSv2transaction"></a>
<a id="tocsv2transaction"></a>

```json
{
  "insertedAt": "2019-08-24T14:15:22Z",
  "timestamp": "2019-08-24T14:15:22Z",
  "postings": [
    {
      "amount": 100,
      "asset": "COIN",
      "destination": "users:002",
      "source": "users:001"
    }
  ],
  "reference": "ref:001",
  "metadata": {
    "admin": "true"
  },
  "id": 0,
  "reverted": true,
  "revertedAt": "2019-08-24T14:15:22Z",
  "preCommitVolumes": {
    "orders:1": {
      "USD": {
        "input": 100,
        "output": 10,
        "balance": 90
      }
    },
    "orders:2": {
      "USD": {
        "input": 100,
        "output": 10,
        "balance": 90
      }
    }
  },
  "postCommitVolumes": {
    "orders:1": {
      "USD": {
        "input": 100,
        "output": 10,
        "balance": 90
      }
    },
    "orders:2": {
      "USD": {
        "input": 100,
        "output": 10,
        "balance": 90
      }
    }
  },
  "preCommitEffectiveVolumes": {
    "orders:1": {
      "USD": {
        "input": 100,
        "output": 10,
        "balance": 90
      }
    },
    "orders:2": {
      "USD": {
        "input": 100,
        "output": 10,
        "balance": 90
      }
    }
  },
  "postCommitEffectiveVolumes": {
    "orders:1": {
      "USD": {
        "input": 100,
        "output": 10,
        "balance": 90
      }
    },
    "orders:2": {
      "USD": {
        "input": 100,
        "output": 10,
        "balance": 90
      }
    }
  }
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|insertedAt|string(date-time)|true|none|none|
|timestamp|string(date-time)|true|none|none|
|postings|[[V2Posting](#schemav2posting)]|true|none|none|
|reference|string|false|none|none|
|metadata|[V2Metadata](#schemav2metadata)|true|none|none|
|id|integer(bigint)|true|none|none|
|reverted|boolean|true|none|none|
|revertedAt|string(date-time)|false|none|none|
|preCommitVolumes|[V2AggregatedVolumes](#schemav2aggregatedvolumes)|false|none|none|
|postCommitVolumes|[V2AggregatedVolumes](#schemav2aggregatedvolumes)|false|none|none|
|preCommitEffectiveVolumes|[V2AggregatedVolumes](#schemav2aggregatedvolumes)|false|none|none|
|postCommitEffectiveVolumes|[V2AggregatedVolumes](#schemav2aggregatedvolumes)|false|none|none|

<h2 id="tocS_V2PostTransaction">V2PostTransaction</h2>
<!-- backwards compatibility -->
<a id="schemav2posttransaction"></a>
<a id="schema_V2PostTransaction"></a>
<a id="tocSv2posttransaction"></a>
<a id="tocsv2posttransaction"></a>

```json
{
  "timestamp": "2019-08-24T14:15:22Z",
  "postings": [
    {
      "amount": 100,
      "asset": "COIN",
      "destination": "users:002",
      "source": "users:001"
    }
  ],
  "script": {
    "plain": "vars {\naccount $user\n}\nsend [COIN 10] (\n\tsource = @world\n\tdestination = $user\n)\n",
    "vars": {
      "user": "users:042"
    }
  },
  "reference": "ref:001",
  "metadata": {
    "admin": "true"
  }
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|timestamp|string(date-time)|false|none|none|
|postings|[[V2Posting](#schemav2posting)]|false|none|none|
|script|object|false|none|none|
|» plain|string|true|none|none|
|» vars|object|false|none|none|
|reference|string|false|none|none|
|metadata|[V2Metadata](#schemav2metadata)|true|none|none|

<h2 id="tocS_V2Stats">V2Stats</h2>
<!-- backwards compatibility -->
<a id="schemav2stats"></a>
<a id="schema_V2Stats"></a>
<a id="tocSv2stats"></a>
<a id="tocsv2stats"></a>

```json
{
  "accounts": 0,
  "transactions": 0
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|accounts|integer(int64)|true|none|none|
|transactions|integer(bigint)|true|none|none|

<h2 id="tocS_V2Log">V2Log</h2>
<!-- backwards compatibility -->
<a id="schemav2log"></a>
<a id="schema_V2Log"></a>
<a id="tocSv2log"></a>
<a id="tocsv2log"></a>

```json
{
  "id": 1234,
  "type": "NEW_TRANSACTION",
  "data": {},
  "hash": "9ee060170400f556b7e1575cb13f9db004f150a08355c7431c62bc639166431e",
  "date": "2019-08-24T14:15:22Z"
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|id|integer(bigint)|true|none|none|
|type|string|true|none|none|
|data|object|true|none|none|
|hash|string|true|none|none|
|date|string(date-time)|true|none|none|

#### Enumerated Values

|Property|Value|
|---|---|
|type|NEW_TRANSACTION|
|type|SET_METADATA|
|type|REVERTED_TRANSACTION|

<h2 id="tocS_V2CreateTransactionResponse">V2CreateTransactionResponse</h2>
<!-- backwards compatibility -->
<a id="schemav2createtransactionresponse"></a>
<a id="schema_V2CreateTransactionResponse"></a>
<a id="tocSv2createtransactionresponse"></a>
<a id="tocsv2createtransactionresponse"></a>

```json
{
  "data": {
    "insertedAt": "2019-08-24T14:15:22Z",
    "timestamp": "2019-08-24T14:15:22Z",
    "postings": [
      {
        "amount": 100,
        "asset": "COIN",
        "destination": "users:002",
        "source": "users:001"
      }
    ],
    "reference": "ref:001",
    "metadata": {
      "admin": "true"
    },
    "id": 0,
    "reverted": true,
    "revertedAt": "2019-08-24T14:15:22Z",
    "preCommitVolumes": {
      "orders:1": {
        "USD": {
          "input": 100,
          "output": 10,
          "balance": 90
        }
      },
      "orders:2": {
        "USD": {
          "input": 100,
          "output": 10,
          "balance": 90
        }
      }
    },
    "postCommitVolumes": {
      "orders:1": {
        "USD": {
          "input": 100,
          "output": 10,
          "balance": 90
        }
      },
      "orders:2": {
        "USD": {
          "input": 100,
          "output": 10,
          "balance": 90
        }
      }
    },
    "preCommitEffectiveVolumes": {
      "orders:1": {
        "USD": {
          "input": 100,
          "output": 10,
          "balance": 90
        }
      },
      "orders:2": {
        "USD": {
          "input": 100,
          "output": 10,
          "balance": 90
        }
      }
    },
    "postCommitEffectiveVolumes": {
      "orders:1": {
        "USD": {
          "input": 100,
          "output": 10,
          "balance": 90
        }
      },
      "orders:2": {
        "USD": {
          "input": 100,
          "output": 10,
          "balance": 90
        }
      }
    }
  }
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|data|[V2Transaction](#schemav2transaction)|true|none|none|

<h2 id="tocS_V2RevertTransactionResponse">V2RevertTransactionResponse</h2>
<!-- backwards compatibility -->
<a id="schemav2reverttransactionresponse"></a>
<a id="schema_V2RevertTransactionResponse"></a>
<a id="tocSv2reverttransactionresponse"></a>
<a id="tocsv2reverttransactionresponse"></a>

```json
{
  "data": {
    "insertedAt": "2019-08-24T14:15:22Z",
    "timestamp": "2019-08-24T14:15:22Z",
    "postings": [
      {
        "amount": 100,
        "asset": "COIN",
        "destination": "users:002",
        "source": "users:001"
      }
    ],
    "reference": "ref:001",
    "metadata": {
      "admin": "true"
    },
    "id": 0,
    "reverted": true,
    "revertedAt": "2019-08-24T14:15:22Z",
    "preCommitVolumes": {
      "orders:1": {
        "USD": {
          "input": 100,
          "output": 10,
          "balance": 90
        }
      },
      "orders:2": {
        "USD": {
          "input": 100,
          "output": 10,
          "balance": 90
        }
      }
    },
    "postCommitVolumes": {
      "orders:1": {
        "USD": {
          "input": 100,
          "output": 10,
          "balance": 90
        }
      },
      "orders:2": {
        "USD": {
          "input": 100,
          "output": 10,
          "balance": 90
        }
      }
    },
    "preCommitEffectiveVolumes": {
      "orders:1": {
        "USD": {
          "input": 100,
          "output": 10,
          "balance": 90
        }
      },
      "orders:2": {
        "USD": {
          "input": 100,
          "output": 10,
          "balance": 90
        }
      }
    },
    "postCommitEffectiveVolumes": {
      "orders:1": {
        "USD": {
          "input": 100,
          "output": 10,
          "balance": 90
        }
      },
      "orders:2": {
        "USD": {
          "input": 100,
          "output": 10,
          "balance": 90
        }
      }
    }
  }
}

```

### Properties

*None*

<h2 id="tocS_V2GetTransactionResponse">V2GetTransactionResponse</h2>
<!-- backwards compatibility -->
<a id="schemav2gettransactionresponse"></a>
<a id="schema_V2GetTransactionResponse"></a>
<a id="tocSv2gettransactionresponse"></a>
<a id="tocsv2gettransactionresponse"></a>

```json
{
  "data": {
    "insertedAt": "2019-08-24T14:15:22Z",
    "timestamp": "2019-08-24T14:15:22Z",
    "postings": [
      {
        "amount": 100,
        "asset": "COIN",
        "destination": "users:002",
        "source": "users:001"
      }
    ],
    "reference": "ref:001",
    "metadata": {
      "admin": "true"
    },
    "id": 0,
    "reverted": true,
    "revertedAt": "2019-08-24T14:15:22Z",
    "preCommitVolumes": {
      "orders:1": {
        "USD": {
          "input": 100,
          "output": 10,
          "balance": 90
        }
      },
      "orders:2": {
        "USD": {
          "input": 100,
          "output": 10,
          "balance": 90
        }
      }
    },
    "postCommitVolumes": {
      "orders:1": {
        "USD": {
          "input": 100,
          "output": 10,
          "balance": 90
        }
      },
      "orders:2": {
        "USD": {
          "input": 100,
          "output": 10,
          "balance": 90
        }
      }
    },
    "preCommitEffectiveVolumes": {
      "orders:1": {
        "USD": {
          "input": 100,
          "output": 10,
          "balance": 90
        }
      },
      "orders:2": {
        "USD": {
          "input": 100,
          "output": 10,
          "balance": 90
        }
      }
    },
    "postCommitEffectiveVolumes": {
      "orders:1": {
        "USD": {
          "input": 100,
          "output": 10,
          "balance": 90
        }
      },
      "orders:2": {
        "USD": {
          "input": 100,
          "output": 10,
          "balance": 90
        }
      }
    }
  }
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|data|[V2Transaction](#schemav2transaction)|true|none|none|

<h2 id="tocS_V2StatsResponse">V2StatsResponse</h2>
<!-- backwards compatibility -->
<a id="schemav2statsresponse"></a>
<a id="schema_V2StatsResponse"></a>
<a id="tocSv2statsresponse"></a>
<a id="tocsv2statsresponse"></a>

```json
{
  "data": {
    "accounts": 0,
    "transactions": 0
  }
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|data|[V2Stats](#schemav2stats)|true|none|none|

<h2 id="tocS_V2ConfigInfoResponse">V2ConfigInfoResponse</h2>
<!-- backwards compatibility -->
<a id="schemav2configinforesponse"></a>
<a id="schema_V2ConfigInfoResponse"></a>
<a id="tocSv2configinforesponse"></a>
<a id="tocsv2configinforesponse"></a>

```json
{
  "server": "string",
  "version": "string"
}

```

### Properties

*None*

<h2 id="tocS_V2Volume">V2Volume</h2>
<!-- backwards compatibility -->
<a id="schemav2volume"></a>
<a id="schema_V2Volume"></a>
<a id="tocSv2volume"></a>
<a id="tocsv2volume"></a>

```json
{
  "input": 100,
  "output": 20,
  "balance": 80
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|input|integer(bigint)|true|none|none|
|output|integer(bigint)|true|none|none|
|balance|integer(bigint)|false|none|none|

<h2 id="tocS_V2Volumes">V2Volumes</h2>
<!-- backwards compatibility -->
<a id="schemav2volumes"></a>
<a id="schema_V2Volumes"></a>
<a id="tocSv2volumes"></a>
<a id="tocsv2volumes"></a>

```json
{
  "USD": {
    "input": 100,
    "output": 10,
    "balance": 90
  },
  "EUR": {
    "input": 100,
    "output": 10,
    "balance": 90
  }
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|**additionalProperties**|[V2Volume](#schemav2volume)|false|none|none|

<h2 id="tocS_V2AggregatedVolumes">V2AggregatedVolumes</h2>
<!-- backwards compatibility -->
<a id="schemav2aggregatedvolumes"></a>
<a id="schema_V2AggregatedVolumes"></a>
<a id="tocSv2aggregatedvolumes"></a>
<a id="tocsv2aggregatedvolumes"></a>

```json
{
  "orders:1": {
    "USD": {
      "input": 100,
      "output": 10,
      "balance": 90
    }
  },
  "orders:2": {
    "USD": {
      "input": 100,
      "output": 10,
      "balance": 90
    }
  }
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|**additionalProperties**|[V2Volumes](#schemav2volumes)|false|none|none|

<h2 id="tocS_V2ErrorResponse">V2ErrorResponse</h2>
<!-- backwards compatibility -->
<a id="schemav2errorresponse"></a>
<a id="schema_V2ErrorResponse"></a>
<a id="tocSv2errorresponse"></a>
<a id="tocsv2errorresponse"></a>

```json
{
  "errorCode": "VALIDATION",
  "errorMessage": "[VALIDATION] invalid 'cursor' query param",
  "details": "https://play.numscript.org/?payload=eyJlcnJvciI6ImFjY291bnQgaGFkIGluc3VmZmljaWVudCBmdW5kcyJ9"
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|errorCode|[V2ErrorsEnum](#schemav2errorsenum)|true|none|none|
|errorMessage|string|true|none|none|
|details|string|false|none|none|

<h2 id="tocS_V2ErrorsEnum">V2ErrorsEnum</h2>
<!-- backwards compatibility -->
<a id="schemav2errorsenum"></a>
<a id="schema_V2ErrorsEnum"></a>
<a id="tocSv2errorsenum"></a>
<a id="tocsv2errorsenum"></a>

```json
"VALIDATION"

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|*anonymous*|string|false|none|none|

#### Enumerated Values

|Property|Value|
|---|---|
|*anonymous*|INTERNAL|
|*anonymous*|INSUFFICIENT_FUND|
|*anonymous*|VALIDATION|
|*anonymous*|CONFLICT|
|*anonymous*|COMPILATION_FAILED|
|*anonymous*|METADATA_OVERRIDE|
|*anonymous*|NOT_FOUND|
|*anonymous*|REVERT_OCCURRING|
|*anonymous*|ALREADY_REVERT|
|*anonymous*|NO_POSTINGS|
|*anonymous*|LEDGER_NOT_FOUND|
|*anonymous*|IMPORT|

<h2 id="tocS_V2LedgerInfoResponse">V2LedgerInfoResponse</h2>
<!-- backwards compatibility -->
<a id="schemav2ledgerinforesponse"></a>
<a id="schema_V2LedgerInfoResponse"></a>
<a id="tocSv2ledgerinforesponse"></a>
<a id="tocsv2ledgerinforesponse"></a>

```json
{
  "data": {
    "name": "ledger001",
    "storage": {
      "migrations": [
        {
          "version": 11,
          "name": "migrations:001",
          "date": "2019-08-24T14:15:22Z",
          "state": "TO DO"
        }
      ]
    }
  }
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|data|[V2LedgerInfo](#schemav2ledgerinfo)|false|none|none|

<h2 id="tocS_V2LedgerInfo">V2LedgerInfo</h2>
<!-- backwards compatibility -->
<a id="schemav2ledgerinfo"></a>
<a id="schema_V2LedgerInfo"></a>
<a id="tocSv2ledgerinfo"></a>
<a id="tocsv2ledgerinfo"></a>

```json
{
  "name": "ledger001",
  "storage": {
    "migrations": [
      {
        "version": 11,
        "name": "migrations:001",
        "date": "2019-08-24T14:15:22Z",
        "state": "TO DO"
      }
    ]
  }
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|name|string|false|none|none|
|storage|object|false|none|none|
|» migrations|[[V2MigrationInfo](#schemav2migrationinfo)]|false|none|none|

<h2 id="tocS_V2MigrationInfo">V2MigrationInfo</h2>
<!-- backwards compatibility -->
<a id="schemav2migrationinfo"></a>
<a id="schema_V2MigrationInfo"></a>
<a id="tocSv2migrationinfo"></a>
<a id="tocsv2migrationinfo"></a>

```json
{
  "version": 11,
  "name": "migrations:001",
  "date": "2019-08-24T14:15:22Z",
  "state": "TO DO"
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|version|integer(int64)|false|none|none|
|name|string|false|none|none|
|date|string(date-time)|false|none|none|
|state|string|false|none|none|

#### Enumerated Values

|Property|Value|
|---|---|
|state|TO DO|
|state|DONE|

<h2 id="tocS_V2Bulk">V2Bulk</h2>
<!-- backwards compatibility -->
<a id="schemav2bulk"></a>
<a id="schema_V2Bulk"></a>
<a id="tocSv2bulk"></a>
<a id="tocsv2bulk"></a>

```json
[
  {
    "action": "string",
    "ik": "string",
    "data": {
      "timestamp": "2019-08-24T14:15:22Z",
      "postings": [
        {
          "amount": 100,
          "asset": "COIN",
          "destination": "users:002",
          "source": "users:001"
        }
      ],
      "script": {
        "plain": "vars {\naccount $user\n}\nsend [COIN 10] (\n\tsource = @world\n\tdestination = $user\n)\n",
        "vars": {
          "user": "users:042"
        }
      },
      "reference": "ref:001",
      "metadata": {
        "admin": "true"
      }
    }
  }
]

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|*anonymous*|[[V2BulkElement](#schemav2bulkelement)]|false|none|none|

<h2 id="tocS_V2BaseBulkElement">V2BaseBulkElement</h2>
<!-- backwards compatibility -->
<a id="schemav2basebulkelement"></a>
<a id="schema_V2BaseBulkElement"></a>
<a id="tocSv2basebulkelement"></a>
<a id="tocsv2basebulkelement"></a>

```json
{
  "action": "string",
  "ik": "string"
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|action|string|true|none|none|
|ik|string|false|none|none|

<h2 id="tocS_V2BulkElement">V2BulkElement</h2>
<!-- backwards compatibility -->
<a id="schemav2bulkelement"></a>
<a id="schema_V2BulkElement"></a>
<a id="tocSv2bulkelement"></a>
<a id="tocsv2bulkelement"></a>

```json
{
  "action": "string",
  "ik": "string",
  "data": {
    "timestamp": "2019-08-24T14:15:22Z",
    "postings": [
      {
        "amount": 100,
        "asset": "COIN",
        "destination": "users:002",
        "source": "users:001"
      }
    ],
    "script": {
      "plain": "vars {\naccount $user\n}\nsend [COIN 10] (\n\tsource = @world\n\tdestination = $user\n)\n",
      "vars": {
        "user": "users:042"
      }
    },
    "reference": "ref:001",
    "metadata": {
      "admin": "true"
    }
  }
}

```

### Properties

oneOf

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|*anonymous*|[V2BulkElementCreateTransaction](#schemav2bulkelementcreatetransaction)|false|none|none|

xor

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|*anonymous*|[V2BulkElementAddMetadata](#schemav2bulkelementaddmetadata)|false|none|none|

xor

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|*anonymous*|[V2BulkElementRevertTransaction](#schemav2bulkelementreverttransaction)|false|none|none|

xor

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|*anonymous*|[V2BulkElementDeleteMetadata](#schemav2bulkelementdeletemetadata)|false|none|none|

<h2 id="tocS_V2BulkElementCreateTransaction">V2BulkElementCreateTransaction</h2>
<!-- backwards compatibility -->
<a id="schemav2bulkelementcreatetransaction"></a>
<a id="schema_V2BulkElementCreateTransaction"></a>
<a id="tocSv2bulkelementcreatetransaction"></a>
<a id="tocsv2bulkelementcreatetransaction"></a>

```json
{
  "action": "string",
  "ik": "string",
  "data": {
    "timestamp": "2019-08-24T14:15:22Z",
    "postings": [
      {
        "amount": 100,
        "asset": "COIN",
        "destination": "users:002",
        "source": "users:001"
      }
    ],
    "script": {
      "plain": "vars {\naccount $user\n}\nsend [COIN 10] (\n\tsource = @world\n\tdestination = $user\n)\n",
      "vars": {
        "user": "users:042"
      }
    },
    "reference": "ref:001",
    "metadata": {
      "admin": "true"
    }
  }
}

```

### Properties

allOf

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|*anonymous*|[V2BaseBulkElement](#schemav2basebulkelement)|false|none|none|

and

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|*anonymous*|object|false|none|none|
|» data|[V2PostTransaction](#schemav2posttransaction)|false|none|none|

<h2 id="tocS_V2TargetId">V2TargetId</h2>
<!-- backwards compatibility -->
<a id="schemav2targetid"></a>
<a id="schema_V2TargetId"></a>
<a id="tocSv2targetid"></a>
<a id="tocsv2targetid"></a>

```json
"string"

```

### Properties

oneOf

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|*anonymous*|string|false|none|none|

xor

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|*anonymous*|integer(bigint)|false|none|none|

<h2 id="tocS_V2TargetType">V2TargetType</h2>
<!-- backwards compatibility -->
<a id="schemav2targettype"></a>
<a id="schema_V2TargetType"></a>
<a id="tocSv2targettype"></a>
<a id="tocsv2targettype"></a>

```json
"TRANSACTION"

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|*anonymous*|string|false|none|none|

#### Enumerated Values

|Property|Value|
|---|---|
|*anonymous*|TRANSACTION|
|*anonymous*|ACCOUNT|

<h2 id="tocS_V2BulkElementAddMetadata">V2BulkElementAddMetadata</h2>
<!-- backwards compatibility -->
<a id="schemav2bulkelementaddmetadata"></a>
<a id="schema_V2BulkElementAddMetadata"></a>
<a id="tocSv2bulkelementaddmetadata"></a>
<a id="tocsv2bulkelementaddmetadata"></a>

```json
{
  "action": "string",
  "ik": "string",
  "data": {
    "targetId": "string",
    "targetType": "TRANSACTION",
    "metadata": {
      "property1": "string",
      "property2": "string"
    }
  }
}

```

### Properties

allOf

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|*anonymous*|[V2BaseBulkElement](#schemav2basebulkelement)|false|none|none|

and

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|*anonymous*|object|false|none|none|
|» data|object|false|none|none|
|»» targetId|[V2TargetId](#schemav2targetid)|true|none|none|
|»» targetType|[V2TargetType](#schemav2targettype)|true|none|none|
|»» metadata|object|true|none|none|
|»»» **additionalProperties**|string|false|none|none|

<h2 id="tocS_V2BulkElementRevertTransaction">V2BulkElementRevertTransaction</h2>
<!-- backwards compatibility -->
<a id="schemav2bulkelementreverttransaction"></a>
<a id="schema_V2BulkElementRevertTransaction"></a>
<a id="tocSv2bulkelementreverttransaction"></a>
<a id="tocsv2bulkelementreverttransaction"></a>

```json
{
  "action": "string",
  "ik": "string",
  "data": {
    "id": 0,
    "force": true,
    "atEffectiveDate": true
  }
}

```

### Properties

allOf

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|*anonymous*|[V2BaseBulkElement](#schemav2basebulkelement)|false|none|none|

and

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|*anonymous*|object|false|none|none|
|» data|object|false|none|none|
|»» id|integer(bigint)|true|none|none|
|»» force|boolean|false|none|none|
|»» atEffectiveDate|boolean|false|none|none|

<h2 id="tocS_V2BulkElementDeleteMetadata">V2BulkElementDeleteMetadata</h2>
<!-- backwards compatibility -->
<a id="schemav2bulkelementdeletemetadata"></a>
<a id="schema_V2BulkElementDeleteMetadata"></a>
<a id="tocSv2bulkelementdeletemetadata"></a>
<a id="tocsv2bulkelementdeletemetadata"></a>

```json
{
  "action": "string",
  "ik": "string",
  "data": {
    "targetId": "string",
    "targetType": "TRANSACTION",
    "key": "string"
  }
}

```

### Properties

allOf

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|*anonymous*|[V2BaseBulkElement](#schemav2basebulkelement)|false|none|none|

and

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|*anonymous*|object|false|none|none|
|» data|object|false|none|none|
|»» targetId|[V2TargetId](#schemav2targetid)|true|none|none|
|»» targetType|[V2TargetType](#schemav2targettype)|true|none|none|
|»» key|string|true|none|none|

<h2 id="tocS_V2BulkResponse">V2BulkResponse</h2>
<!-- backwards compatibility -->
<a id="schemav2bulkresponse"></a>
<a id="schema_V2BulkResponse"></a>
<a id="tocSv2bulkresponse"></a>
<a id="tocsv2bulkresponse"></a>

```json
{
  "data": [
    {
      "responseType": "string",
      "data": {
        "insertedAt": "2019-08-24T14:15:22Z",
        "timestamp": "2019-08-24T14:15:22Z",
        "postings": [
          {
            "amount": 100,
            "asset": "COIN",
            "destination": "users:002",
            "source": "users:001"
          }
        ],
        "reference": "ref:001",
        "metadata": {
          "admin": "true"
        },
        "id": 0,
        "reverted": true,
        "revertedAt": "2019-08-24T14:15:22Z",
        "preCommitVolumes": {
          "orders:1": {
            "USD": {
              "input": 100,
              "output": 10,
              "balance": 90
            }
          },
          "orders:2": {
            "USD": {
              "input": 100,
              "output": 10,
              "balance": 90
            }
          }
        },
        "postCommitVolumes": {
          "orders:1": {
            "USD": {
              "input": 100,
              "output": 10,
              "balance": 90
            }
          },
          "orders:2": {
            "USD": {
              "input": 100,
              "output": 10,
              "balance": 90
            }
          }
        },
        "preCommitEffectiveVolumes": {
          "orders:1": {
            "USD": {
              "input": 100,
              "output": 10,
              "balance": 90
            }
          },
          "orders:2": {
            "USD": {
              "input": 100,
              "output": 10,
              "balance": 90
            }
          }
        },
        "postCommitEffectiveVolumes": {
          "orders:1": {
            "USD": {
              "input": 100,
              "output": 10,
              "balance": 90
            }
          },
          "orders:2": {
            "USD": {
              "input": 100,
              "output": 10,
              "balance": 90
            }
          }
        }
      }
    }
  ]
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|data|[[V2BulkElementResult](#schemav2bulkelementresult)]|true|none|none|

<h2 id="tocS_V2BulkElementResult">V2BulkElementResult</h2>
<!-- backwards compatibility -->
<a id="schemav2bulkelementresult"></a>
<a id="schema_V2BulkElementResult"></a>
<a id="tocSv2bulkelementresult"></a>
<a id="tocsv2bulkelementresult"></a>

```json
{
  "responseType": "string",
  "data": {
    "insertedAt": "2019-08-24T14:15:22Z",
    "timestamp": "2019-08-24T14:15:22Z",
    "postings": [
      {
        "amount": 100,
        "asset": "COIN",
        "destination": "users:002",
        "source": "users:001"
      }
    ],
    "reference": "ref:001",
    "metadata": {
      "admin": "true"
    },
    "id": 0,
    "reverted": true,
    "revertedAt": "2019-08-24T14:15:22Z",
    "preCommitVolumes": {
      "orders:1": {
        "USD": {
          "input": 100,
          "output": 10,
          "balance": 90
        }
      },
      "orders:2": {
        "USD": {
          "input": 100,
          "output": 10,
          "balance": 90
        }
      }
    },
    "postCommitVolumes": {
      "orders:1": {
        "USD": {
          "input": 100,
          "output": 10,
          "balance": 90
        }
      },
      "orders:2": {
        "USD": {
          "input": 100,
          "output": 10,
          "balance": 90
        }
      }
    },
    "preCommitEffectiveVolumes": {
      "orders:1": {
        "USD": {
          "input": 100,
          "output": 10,
          "balance": 90
        }
      },
      "orders:2": {
        "USD": {
          "input": 100,
          "output": 10,
          "balance": 90
        }
      }
    },
    "postCommitEffectiveVolumes": {
      "orders:1": {
        "USD": {
          "input": 100,
          "output": 10,
          "balance": 90
        }
      },
      "orders:2": {
        "USD": {
          "input": 100,
          "output": 10,
          "balance": 90
        }
      }
    }
  }
}

```

### Properties

oneOf

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|*anonymous*|[V2BulkElementResultCreateTransaction](#schemav2bulkelementresultcreatetransaction)|false|none|none|

xor

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|*anonymous*|[V2BulkElementResultAddMetadata](#schemav2bulkelementresultaddmetadata)|false|none|none|

xor

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|*anonymous*|[V2BulkElementResultRevertTransaction](#schemav2bulkelementresultreverttransaction)|false|none|none|

xor

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|*anonymous*|[V2BulkElementResultDeleteMetadata](#schemav2bulkelementresultdeletemetadata)|false|none|none|

xor

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|*anonymous*|[V2BulkElementResultError](#schemav2bulkelementresulterror)|false|none|none|

<h2 id="tocS_V2BaseBulkElementResult">V2BaseBulkElementResult</h2>
<!-- backwards compatibility -->
<a id="schemav2basebulkelementresult"></a>
<a id="schema_V2BaseBulkElementResult"></a>
<a id="tocSv2basebulkelementresult"></a>
<a id="tocsv2basebulkelementresult"></a>

```json
{
  "responseType": "string"
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|responseType|string|true|none|none|

<h2 id="tocS_V2BulkElementResultCreateTransaction">V2BulkElementResultCreateTransaction</h2>
<!-- backwards compatibility -->
<a id="schemav2bulkelementresultcreatetransaction"></a>
<a id="schema_V2BulkElementResultCreateTransaction"></a>
<a id="tocSv2bulkelementresultcreatetransaction"></a>
<a id="tocsv2bulkelementresultcreatetransaction"></a>

```json
{
  "responseType": "string",
  "data": {
    "insertedAt": "2019-08-24T14:15:22Z",
    "timestamp": "2019-08-24T14:15:22Z",
    "postings": [
      {
        "amount": 100,
        "asset": "COIN",
        "destination": "users:002",
        "source": "users:001"
      }
    ],
    "reference": "ref:001",
    "metadata": {
      "admin": "true"
    },
    "id": 0,
    "reverted": true,
    "revertedAt": "2019-08-24T14:15:22Z",
    "preCommitVolumes": {
      "orders:1": {
        "USD": {
          "input": 100,
          "output": 10,
          "balance": 90
        }
      },
      "orders:2": {
        "USD": {
          "input": 100,
          "output": 10,
          "balance": 90
        }
      }
    },
    "postCommitVolumes": {
      "orders:1": {
        "USD": {
          "input": 100,
          "output": 10,
          "balance": 90
        }
      },
      "orders:2": {
        "USD": {
          "input": 100,
          "output": 10,
          "balance": 90
        }
      }
    },
    "preCommitEffectiveVolumes": {
      "orders:1": {
        "USD": {
          "input": 100,
          "output": 10,
          "balance": 90
        }
      },
      "orders:2": {
        "USD": {
          "input": 100,
          "output": 10,
          "balance": 90
        }
      }
    },
    "postCommitEffectiveVolumes": {
      "orders:1": {
        "USD": {
          "input": 100,
          "output": 10,
          "balance": 90
        }
      },
      "orders:2": {
        "USD": {
          "input": 100,
          "output": 10,
          "balance": 90
        }
      }
    }
  }
}

```

### Properties

allOf

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|*anonymous*|[V2BaseBulkElementResult](#schemav2basebulkelementresult)|false|none|none|

and

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|*anonymous*|object|false|none|none|
|» data|[V2Transaction](#schemav2transaction)|true|none|none|

<h2 id="tocS_V2BulkElementResultAddMetadata">V2BulkElementResultAddMetadata</h2>
<!-- backwards compatibility -->
<a id="schemav2bulkelementresultaddmetadata"></a>
<a id="schema_V2BulkElementResultAddMetadata"></a>
<a id="tocSv2bulkelementresultaddmetadata"></a>
<a id="tocsv2bulkelementresultaddmetadata"></a>

```json
{
  "responseType": "string"
}

```

### Properties

*None*

<h2 id="tocS_V2BulkElementResultRevertTransaction">V2BulkElementResultRevertTransaction</h2>
<!-- backwards compatibility -->
<a id="schemav2bulkelementresultreverttransaction"></a>
<a id="schema_V2BulkElementResultRevertTransaction"></a>
<a id="tocSv2bulkelementresultreverttransaction"></a>
<a id="tocsv2bulkelementresultreverttransaction"></a>

```json
{
  "responseType": "string",
  "data": {
    "insertedAt": "2019-08-24T14:15:22Z",
    "timestamp": "2019-08-24T14:15:22Z",
    "postings": [
      {
        "amount": 100,
        "asset": "COIN",
        "destination": "users:002",
        "source": "users:001"
      }
    ],
    "reference": "ref:001",
    "metadata": {
      "admin": "true"
    },
    "id": 0,
    "reverted": true,
    "revertedAt": "2019-08-24T14:15:22Z",
    "preCommitVolumes": {
      "orders:1": {
        "USD": {
          "input": 100,
          "output": 10,
          "balance": 90
        }
      },
      "orders:2": {
        "USD": {
          "input": 100,
          "output": 10,
          "balance": 90
        }
      }
    },
    "postCommitVolumes": {
      "orders:1": {
        "USD": {
          "input": 100,
          "output": 10,
          "balance": 90
        }
      },
      "orders:2": {
        "USD": {
          "input": 100,
          "output": 10,
          "balance": 90
        }
      }
    },
    "preCommitEffectiveVolumes": {
      "orders:1": {
        "USD": {
          "input": 100,
          "output": 10,
          "balance": 90
        }
      },
      "orders:2": {
        "USD": {
          "input": 100,
          "output": 10,
          "balance": 90
        }
      }
    },
    "postCommitEffectiveVolumes": {
      "orders:1": {
        "USD": {
          "input": 100,
          "output": 10,
          "balance": 90
        }
      },
      "orders:2": {
        "USD": {
          "input": 100,
          "output": 10,
          "balance": 90
        }
      }
    }
  }
}

```

### Properties

allOf

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|*anonymous*|[V2BaseBulkElementResult](#schemav2basebulkelementresult)|false|none|none|

and

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|*anonymous*|object|false|none|none|
|» data|[V2Transaction](#schemav2transaction)|true|none|none|

<h2 id="tocS_V2BulkElementResultDeleteMetadata">V2BulkElementResultDeleteMetadata</h2>
<!-- backwards compatibility -->
<a id="schemav2bulkelementresultdeletemetadata"></a>
<a id="schema_V2BulkElementResultDeleteMetadata"></a>
<a id="tocSv2bulkelementresultdeletemetadata"></a>
<a id="tocsv2bulkelementresultdeletemetadata"></a>

```json
{
  "responseType": "string"
}

```

### Properties

*None*

<h2 id="tocS_V2BulkElementResultError">V2BulkElementResultError</h2>
<!-- backwards compatibility -->
<a id="schemav2bulkelementresulterror"></a>
<a id="schema_V2BulkElementResultError"></a>
<a id="tocSv2bulkelementresulterror"></a>
<a id="tocsv2bulkelementresulterror"></a>

```json
{
  "responseType": "string",
  "errorCode": "string",
  "errorDescription": "string",
  "errorDetails": "string"
}

```

### Properties

allOf

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|*anonymous*|[V2BaseBulkElementResult](#schemav2basebulkelementresult)|false|none|none|

and

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|*anonymous*|object|false|none|none|
|» errorCode|string|true|none|none|
|» errorDescription|string|true|none|none|
|» errorDetails|string|false|none|none|

<h2 id="tocS_V2CreateLedgerRequest">V2CreateLedgerRequest</h2>
<!-- backwards compatibility -->
<a id="schemav2createledgerrequest"></a>
<a id="schema_V2CreateLedgerRequest"></a>
<a id="tocSv2createledgerrequest"></a>
<a id="tocsv2createledgerrequest"></a>

```json
{
  "bucket": "string",
  "metadata": {
    "admin": "true"
  },
  "features": {
    "property1": "string",
    "property2": "string"
  }
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|bucket|string|false|none|none|
|metadata|[V2Metadata](#schemav2metadata)|false|none|none|
|features|object|false|none|none|
|» **additionalProperties**|string|false|none|none|

<h2 id="tocS_V2Ledger">V2Ledger</h2>
<!-- backwards compatibility -->
<a id="schemav2ledger"></a>
<a id="schema_V2Ledger"></a>
<a id="tocSv2ledger"></a>
<a id="tocsv2ledger"></a>

```json
{
  "name": "string",
  "addedAt": "2019-08-24T14:15:22Z",
  "bucket": "string",
  "metadata": {
    "admin": "true"
  }
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|name|string|true|none|none|
|addedAt|string(date-time)|true|none|none|
|bucket|string|true|none|none|
|metadata|[V2Metadata](#schemav2metadata)|false|none|none|

<h2 id="tocS_V2LedgerListResponse">V2LedgerListResponse</h2>
<!-- backwards compatibility -->
<a id="schemav2ledgerlistresponse"></a>
<a id="schema_V2LedgerListResponse"></a>
<a id="tocSv2ledgerlistresponse"></a>
<a id="tocsv2ledgerlistresponse"></a>

```json
{
  "cursor": {
    "pageSize": 15,
    "hasMore": false,
    "previous": "YXVsdCBhbmQgYSBtYXhpbXVtIG1heF9yZXN1bHRzLol=",
    "next": "",
    "data": [
      {
        "name": "string",
        "addedAt": "2019-08-24T14:15:22Z",
        "bucket": "string",
        "metadata": {
          "admin": "true"
        }
      }
    ]
  }
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|cursor|object|true|none|none|
|» pageSize|integer(int64)|true|none|none|
|» hasMore|boolean|true|none|none|
|» previous|string|false|none|none|
|» next|string|false|none|none|
|» data|[[V2Ledger](#schemav2ledger)]|true|none|none|

<h2 id="tocS_V2UpdateLedgerMetadataRequest">V2UpdateLedgerMetadataRequest</h2>
<!-- backwards compatibility -->
<a id="schemav2updateledgermetadatarequest"></a>
<a id="schema_V2UpdateLedgerMetadataRequest"></a>
<a id="tocSv2updateledgermetadatarequest"></a>
<a id="tocsv2updateledgermetadatarequest"></a>

```json
{
  "admin": "true"
}

```

### Properties

*None*

<h2 id="tocS_V2GetLedgerResponse">V2GetLedgerResponse</h2>
<!-- backwards compatibility -->
<a id="schemav2getledgerresponse"></a>
<a id="schema_V2GetLedgerResponse"></a>
<a id="tocSv2getledgerresponse"></a>
<a id="tocsv2getledgerresponse"></a>

```json
{
  "data": {
    "name": "string",
    "addedAt": "2019-08-24T14:15:22Z",
    "bucket": "string",
    "metadata": {
      "admin": "true"
    }
  }
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|data|[V2Ledger](#schemav2ledger)|true|none|none|

