package wal

import (
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/zeebo/xxh3"
)

type StateMachine interface {
	Apply(record Record) error
	Snapshot()
	Restore()
}

type WAL struct {
	writeCh chan Request
	seq     uint32
	epoch   uint32
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

const (
	TypeWrite byte = iota + 1
	TypeCheckpoint
)

func New() *WAL {
	return &WAL{writeCh: make(chan Request), epoch: 0, seq: 0}
}

func getEpoch(root string, ext string) ([]string, error) {
	var files []string

	f, err := os.Open(root)
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

	return files, nil
}

func (w *WAL) checkpoint() {
	// check if WAL has 1000 lines -> Recover()

	// // initialize the new wal file wal-{n}.log - n = no. of files with wal- prefix and .log extension
	// currentFile, err = os.Create(fmt.Sprintf("wal-%d.log", epochLen+1))
	// if err != nil {
	// 	fmt.Printf("unable to create starting file: %w", err)
	// }
	// epochLen++
}

const HEADER_SIZE = 4 + // epoch
	4 + // seq
	1 + // type
	2 + // length
	8 // xxh3 checksum

func buildHeader(epoch uint32, seq uint32, headerType byte, payload []byte) []byte {
	h := make([]byte, HEADER_SIZE)
	binary.LittleEndian.PutUint32(h[0:4], epoch)
	binary.LittleEndian.PutUint32(h[4:8], seq)
	h[8] = headerType
	binary.LittleEndian.PutUint16(h[9:11], uint16(len(payload)))

	// checksum
	checksum := xxh3.Hash(append(h[:11], payload...))
	binary.LittleEndian.PutUint64(h[11:HEADER_SIZE], checksum)
	return h
}

func (w *WAL) Start() {
	// get length of files and create wal-{n + 1}.log, edge-case for 10, 11
	epoch, err := getEpoch(".", "log")
	w.epoch = uint32(len(epoch))

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
			// [epoch][sequence][type][length][XXH3 checksum][payload]
			payload := []byte(fmt.Sprintf("%s %s:%s", data.Record.Method, data.Record.Key, data.Record.Value))
			w.seq++

			header := buildHeader(w.epoch, w.seq, TypeWrite, payload)
			buffer = append(buffer, header...)
			buffer = append(buffer, payload...)
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
	w.epoch = uint32(len(epoch))

	if err != nil {
		return err
	}

	// create starting wal file or open file
	if len(epoch) == 0 {
		// stop recover since no wal files yet
		return nil
	}

	currentFile, err := os.Open(epoch[len(epoch)-1])
	fmt.Println(epoch[len(epoch)-1])

	if err != nil {
		log.Fatal(err)
	}
	defer currentFile.Close()

	fmt.Println(currentFile)

	var records []Record

	var expectedSeq uint32 = 1

	for {
		headerBuf := make([]byte, HEADER_SIZE)
		_, err := io.ReadFull(currentFile, headerBuf)
		if err == io.EOF {
			break
		}
		if err != nil {
			// corrupted header
			break
		}
		seq := binary.LittleEndian.Uint32(headerBuf[4:8])
		recType := headerBuf[8]
		length := binary.LittleEndian.Uint16(headerBuf[9:11])
		storedChecksum := binary.LittleEndian.Uint64(headerBuf[11:HEADER_SIZE])

		payload := make([]byte, length)
		_, err = io.ReadFull(currentFile, payload)

		if err != nil {
			// corrupted payload
			break
		}

		// compare and verify the checksum
		computedChecksum := xxh3.Hash(append(headerBuf[:11], payload...))
		if computedChecksum != storedChecksum {
			break
		}

		if seq != expectedSeq {
			break
		}
		expectedSeq++

		// only apply write records
		if recType != TypeWrite {
			continue
		}

		parts := strings.SplitN(string(payload), " ", 2)

		method := parts[0]

		keyValue := strings.SplitN(parts[1], ":", 2)

		record := Record{
			Key:    keyValue[0],
			Method: method,
		}
		if len(keyValue) > 1 {
			record.Value = keyValue[1]
		}

		records = append(records, record)

		err = sm.Apply(record)
		if err != nil {
			return err
		}
	}

	w.seq = expectedSeq - 1
	return nil
}
