//go:build !wasip1

package main

// The executable is a WASI-only module. This host stub keeps the repository's
// normal Go package tests/builds from trying to compile wasm imports.
func main() {}
