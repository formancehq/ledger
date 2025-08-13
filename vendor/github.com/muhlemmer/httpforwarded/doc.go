// Copyright 2016 Tim Heckman. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package httpforwarded is a helper package for parsing the Forwarded HTTP
// header as defined in RFC-7239 [1]. There is a function for parsing the value
// of multiple Forwarded headers, and a function for formatting a Forwarded
// header.
//
// Because an HTTP request can have multiple Forwarded headers with unique
// fields, we need to support parsing and joining of all of them in the order
// they are provided.
//
// Here is an example of parsing the Forwarded header:
//
// 		// var req *http.Request
//		vals := req.Header[http.CanonicalHeaderKey("forwarded")] // get all Forwarded fields
//		params, _ := httpforwarded.Parse(vals)
//
// [1] https://tools.ietf.org/html/rfc7239
package httpforwarded
