package pgserver

import (
	. "github.com/onsi/gomega"
)

var pgServer *PGServer

func StartServer() {
	var err error
	pgServer, err = PostgresServer()
	Expect(err).WithOffset(1).To(BeNil())
}

func StopServer() {
	pgServer.Close()
}

func ConnString(name string) string {
	return pgServer.ConnString(name)
}
