package main

import "testing"

func TestSmoke(t *testing.T) {
	t.Parallel()
	if err := run(); err != nil {
		t.Fatal(err)
	}
}
