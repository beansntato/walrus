package main

import (
	"fmt"
	"log"
	"os"

	"beansntato.dev/wal"
)

func main() {
	os.MkdirAll("snapshot", 0755)

	sm := NewKV()
	w := wal.New()

	if err := w.Recover(sm); err != nil {
		log.Fatal(err)
	}

	go w.Start(sm)

	w.Append(wal.Record{Method: "PUT", Key: "foo", Value: "bar"})
	fmt.Println(sm.Get("foo"))
}
