package wal

import (
	"fmt"
	"log"
	"os"
	"time"
)

type StateMachine interface {
	apply()
	snapshot()
	restore()
}

type WAL struct {
	writeCh chan Request
}

type Request struct {
	Record Record
	Done   chan error
}

type Record struct {
	Key    string
	Value  string
	Method string
}

func New() *WAL {
	return &WAL{writeCh: make(chan Request)}
}

func (w *WAL) Start() {

	f, err := os.OpenFile("example.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	var pending []Request
	var buffer []byte
	ticker := time.NewTicker(5 * time.Millisecond)

	for {
		select {
		case data := <-w.writeCh:
			pending = append(pending, data)
			buffer = append(buffer, []byte(fmt.Sprintf("%s %s:%s\n", data.Record.Method, data.Record.Key, data.Record.Value))...)
		case <-ticker.C:
			if len(buffer) == 0 {
				continue
			}

			_, err := f.Write(buffer)

			f.Sync()

			// notify the waiters
			for _, req := range pending {
				req.Done <- err
			}

			pending = pending[:0]
			buffer = buffer[:0]
		}
	}

}

// func Apply(key, val string, method string) {
func (w *WAL) Append(record Record) error {
	done := make(chan error)
	request := Request{Record: record, Done: done}

	fmt.Println("adding request")
	w.writeCh <- request

	fmt.Println("done")

	return <-done
}

func Restore() {

}
