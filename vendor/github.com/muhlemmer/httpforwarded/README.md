# httpforwarded
[![License](https://img.shields.io/badge/license-BSD--style_3--clause-brightgreen.svg?style=flat)](https://github.com/muhlemmer/httpforwarded/blob/master/LICENSE)
[![Go Reference](https://pkg.go.dev/badge/github.com/muhlemmer/httpforwarded.svg)](https://pkg.go.dev/github.com/muhlemmer/httpforwarded)
[![Go test](https://github.com/muhlemmer/httpforwarded/actions/workflows/go.yml/badge.svg)](https://github.com/muhlemmer/httpforwarded/actions/workflows/go.yml)
[![codecov](https://codecov.io/gh/muhlemmer/httpforwarded/graph/badge.svg?token=E3G7W40GLU)](https://codecov.io/gh/muhlemmer/httpforwarded)

The `httpforwarded` go package provides utility functions for working with the
`Forwarded` HTTP header as defined in [RFC-7239](https://tools.ietf.org/html/rfc7239).
This header is proposed to replace the `X-Forwarded-For` and `X-Forwarded-Proto`
headers, amongst others.

This package was heavily inspired by the `mime` package in the standard library,
more specifically the [ParseMediaType()](https://golang.org/pkg/mime/#ParseMediaType)
function.

This is a **fork** from https://github.com/theckman/httpforwarded,
which seems to have become idle for several years.

## License
This package copies some functions, without modification, from the Go standard
library. As such, the entirety of this package is released under the same
permissive BSD-style license as the Go language itself. Please see the contents
of the [LICENSE](https://github.com/muhlemmer/httpforwarded/blob/master/LICENSE)
file for the full details of the license.

## Installing
To install this package for consumption, you can run the following:

```
go get -u github.com/muhlemmer/httpforwarded
```

## Usage

Given a `*http.Request`:

```Go
// var req *http.Request

params, _ := httpforwarded.ParseFromRequest(req)

// you can then do something like this to get the first "for" param:
fmt.Printf("origin %s", params["for"][0])
```

Given a list of `Forwarded` header values:

```Go
// var req *http.Request

headerValues := req.Header[http.CanonicalHeaderKey("forwarded")]

params, _ := httpforwarded.Parse(headerValues)

// you can then do something like this to get the first "for" param:
fmt.Printf("origin %s", params["for"][0])
```
