package wal

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"slices"
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

func getEpoch(root string, ext string) ([]string, error) {
	var files []string

	f, err := os.Open(".")
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	// Read all names (-1 means all)
	names, err := f.Readdirnames(-1)
	if err != nil {
		log.Fatal(err)
	}

	// Print the names
	for _, name := range names {
		fmt.Println(name)
	}

	for _, path := range names {
		if strings.HasPrefix(path, "wal-") && strings.HasSuffix(path, "."+ext) {
			files = append(files, path)
		}
	}

	slices.SortFunc(files, naturalCmp)

	fmt.Println("files", files)
	return files, nil
}

func (w *WAL) Start() {
	// get length of files and create wal-{n + 1}.log, edge-case for 10, 11
	epoch, err := getEpoch(".", "log")

	if err != nil {
		return
	}

	var currentFile *os.File
	epochLen := len(epoch)

	// create starting wal file or open file
	if len(epoch) == 0 {
		currentFile, err = os.Create(fmt.Sprintf("wal-%d.log", len(epoch)+1))
		if err != nil {
			fmt.Printf("unable to create starting file: %v", err)
		}
		// add 1 to epochLen - getEpoch only runs on start up
		epochLen++
	} else {
		currentFile, err = os.OpenFile(epoch[len(epoch)-1], os.O_APPEND|os.O_WRONLY, 0644)
		if err != nil {
			log.Fatal(err)
		}
	}
	defer currentFile.Close()

	var pending []Request
	var buffer []byte
	ticker := time.NewTicker(5 * time.Millisecond)

	for {
		select {
		case data := <-w.writeCh:
			pending = append(pending, data)
			buffer = append(buffer, []byte(fmt.Sprintf("%s %s:%s\n", data.Record.Method, data.Record.Key, data.Record.Value))...)
		case <-ticker.C:
			// checkpoint every 5ms - make sure to not recover records on database already
			if len(buffer) == 0 {
				continue
			}

			_, err := currentFile.Write(buffer)
			currentFile.Sync()

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
	epoch, err := getEpoch(".", "log")

	if err != nil {
		return err
	}

	var currentFile *os.File

	// create starting wal file or open file
	if len(epoch) == 0 {
		// stop recover since no wal files yet
		return nil
	}

	currentFile, err = os.Open(epoch[len(epoch)-1])
	fmt.Println(epoch[len(epoch)-1])
	if err != nil {
		log.Fatal(err)
	}
	defer currentFile.Close()

	fmt.Println(currentFile)

	var records []Record

	scanner := bufio.NewScanner(currentFile)

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
