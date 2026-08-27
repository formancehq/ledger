package main

import (
	"google.golang.org/grpc/metadata"
)

// trailerDiag extracts the server's x-diag response trailer — the serving
// node, route, barrier state, and store fold points recorded by the
// ledger's diag interceptor — so finding details can name the exact
// server-side path that produced a violating response.
func trailerDiag(md metadata.MD) string {
	if v := md.Get("x-diag"); len(v) > 0 {
		return v[0]
	}

	return ""
}

// streamDiag is trailerDiag over a finished (or nil) stream.
func streamDiag(s interface{ Trailer() metadata.MD }) string {
	if s == nil {
		return ""
	}

	return trailerDiag(s.Trailer())
}

// firstDiag unwraps the optional variadic diag argument the validation
// helpers take, so pre-existing call sites and tests stay unchanged.
func firstDiag(diag []string) string {
	if len(diag) > 0 {
		return diag[0]
	}

	return ""
}
