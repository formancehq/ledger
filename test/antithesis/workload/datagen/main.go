package main

import (
	"fmt"
	"math/rand"
)

type Sequence struct {
}

func Account() string {
	return fmt.Sprintf("%d", rand.Intn(10e9))
}

func main() {
	fmt.Println(Account())
}
