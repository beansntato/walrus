package wal

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"strings"
	"time"
)

type StateMachine interface {
	Apply(record Record) error
	Snapshot()
	Restore()
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

func (w *WAL) Recover(sm StateMachine) error {
	f, err := os.Open("example.txt")
	if err != nil {
		return err
	}
	defer f.Close()

	var records []Record

	scanner := bufio.NewScanner(f)

	for scanner.Scan() {
		line := scanner.Text()

		parts := strings.SplitN(line, " ", 2)

		method := parts[0]

		keyValue := strings.SplitN(parts[1], ":", 2)

		record := Record{
			Key:    keyValue[0],
			Value:  keyValue[1],
			Method: method,
		}

		records = append(records, record)
	}

	for _, record := range records {
		err := sm.Apply(record)
		if err != nil {
			return err
		}
	}

	return nil
}
