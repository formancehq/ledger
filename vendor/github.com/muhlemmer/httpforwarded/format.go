// Copyright 2016 Tim Heckman. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package httpforwarded

import (
	"bytes"
	"sort"
	"strings"
	"sync"
)

var bytesBufferPool = &sync.Pool{
	New: func() interface{} { return &bytes.Buffer{} },
}

// Format is a function that takes a map[string][]string and generates the
// appropriate content for an HTTP Forwarded header.
func Format(params map[string][]string) string {
	if len(params) == 0 {
		return ""
	}

	buf := bytesBufferPool.Get().(*bytes.Buffer)
	buf.Reset()

	for paramCount, paramName := range sortedParamKeys(params) {
		values := params[paramName]

		for i, val := range values {
			// write key=
			buf.WriteString(paramName)
			buf.WriteByte('=')

			// if this is a token, write the value without wrapping in quotes
			if strings.IndexFunc(val, isNotTokenChar) == -1 {
				buf.WriteString(val)
			} else {
				// wrap the value in quotes
				buf.WriteByte('"')
				buf.WriteString(val)
				buf.WriteByte('"')
			}

			// if this is not the last value of this parameter
			// write a comma and a space
			if len(values) > i+1 {
				buf.WriteString(", ")
			} else {
				// if this is not the last parameter
				// write a semi-colon and a space
				if len(params) > paramCount+1 {
					buf.WriteString("; ")
				}
			}
		}
	}

	out := buf.String()

	bytesBufferPool.Put(buf)

	return out
}

func sortedParamKeys(params map[string][]string) []string {
	keys := make(sort.StringSlice, len(params))

	var keyCount int

	for paramName := range params {
		keys[keyCount] = paramName
		keyCount++
	}

	sort.Sort(keys)

	return keys
}
