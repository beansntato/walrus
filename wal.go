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

	"github.com/cespare/xxhash"
)

type StateMachine interface {
	Apply(record Record) error
	Snapshot() []byte
	Restore([]byte) error
}

type WAL struct {
	writeCh     chan Request
	seq         uint32
	epoch       uint32
	currentFile *os.File
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

const MAX_SEGMENT_SIZE = 64 * 1024 * 1024 // 64MB

const HEADER_SIZE = 4 + // epoch
	4 + // local seq
	1 + // type
	4 + // length
	8 // xxh3 checksum

func New() *WAL {
	return &WAL{writeCh: make(chan Request), epoch: 0, seq: 0}
}

func getSegments(root string, ext string) ([]string, error) {
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

// once the current wal file size exceeds the max size,
// create the next file. this is used to manage the file size of the logs
func (w *WAL) rotate() error {
	info, err := w.currentFile.Stat()
	if err != nil {
		return err
	}

	if info.Size() < MAX_SEGMENT_SIZE {
		return nil
	}

	if err := w.currentFile.Sync(); err != nil {
		return err
	}
	w.currentFile.Close()

	w.epoch++
	w.seq = 0
	next := getSegmentPath(w.epoch)
	f, err := os.OpenFile(next, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	w.currentFile = f
	return nil
}

// this creates a snapshot to avoid having to replay all lines of a WAL file
// the snapshot contains the state of the StateMachine at that point in time
// which means it doesn't matter what records comes before
// that state is the consequence of all the records before it
func (w *WAL) Checkpoint(sm StateMachine) error {
	// create snapshot + checkpoint marker in WAL
	snapPath := fmt.Sprintf("snapshot/%d-%d.snap", w.epoch, w.seq)
	tmpPath := snapPath + ".tmp"

	f, err := os.Create(tmpPath)

	if err != nil {
		return fmt.Errorf("snapshot create: %w", err)
	}

	data := sm.Snapshot()

	if _, err := f.Write(data); err != nil {
		f.Close()
		os.Remove(tmpPath)
		return fmt.Errorf("snapshot write: %w", err)
	}

	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmpPath)
		return fmt.Errorf("snapshot sync: %w", err)
	}
	f.Close()

	if err := os.Rename(tmpPath, snapPath); err != nil {
		return fmt.Errorf("snapshot rename: %w", err)
	}

	dir, _ := os.Open("snapshot")
	dir.Sync()
	dir.Close()

	snapshotChecksum := xxhash.Sum64(data)

	payload := make([]byte, 16)
	binary.LittleEndian.PutUint32(payload[0:4], w.epoch)
	binary.LittleEndian.PutUint32(payload[4:8], w.seq)
	binary.LittleEndian.PutUint64(payload[8:16], snapshotChecksum)

	header := buildHeader(w.epoch, w.seq, TypeCheckpoint, payload)
	if _, err := w.currentFile.Write(append(header, payload...)); err != nil {
		return fmt.Errorf("checkpoint WAL write: %w", err)
	}
	return w.compact(w.epoch)
}

func (w *WAL) compact(currentEpoch uint32) error {
	segments, err := getSegments(".", ".log")
	if err != nil {
		return fmt.Errorf("deleteSegmentsBefore: %w", err)
	}

	for _, seg := range segments {
		var epoch uint32
		if _, err := fmt.Sscanf(seg, "wal-%d.log", &epoch); err != nil {
			continue
		}
		if epoch < currentEpoch {
			if err := os.Remove(seg); err != nil {
				return fmt.Errorf("delete segment %s: %w", seg, err)
			}
		}
	}
	return nil
}

func buildHeader(epoch, seq uint32, recType byte, payload []byte) []byte {
	h := make([]byte, HEADER_SIZE)
	binary.LittleEndian.PutUint32(h[0:4], epoch)
	binary.LittleEndian.PutUint32(h[4:8], seq)
	h[8] = recType
	binary.LittleEndian.PutUint32(h[9:13], uint32(len(payload)))

	checksum := xxhash.Sum64(append(h[:13], payload...))
	binary.LittleEndian.PutUint64(h[13:21], checksum)
	return h
}

func (w *WAL) Start(sm StateMachine) {
	// get length of files and create wal-{n + 1}.log, edge-case for 10, 11
	epoch, err := getSegments(".", "log")
	w.epoch = uint32(len(epoch))

	if err != nil {
		return
	}

	var currentFile *os.File
	epochLen := len(epoch)

	// create starting wal file or open file
	if len(epoch) == 0 {
		currentFile, err = os.Create(getSegmentPath(uint32(len(epoch) + 1)))
		if err != nil {
			fmt.Printf("unable to create starting file: %v", err)
		}
		// add 1 to epochLen - getSegments only runs on start up
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

	w.currentFile = currentFile

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

			if w.seq%100000 == 0 {
				w.Checkpoint(sm)
			}

			// if file size exceeds limit, rotate the wal
			if err := w.rotate(); err != nil {
				for _, req := range pending {
					req.Done <- err
				}
				continue
			}

		}
	}

}

// func Apply(key, val string, method string) {
func (w *WAL) Append(record Record) error {
	done := make(chan error)
	request := Request{Record: record, Done: done}

	w.writeCh <- request

	return <-done
}

func (w *WAL) Recover(sm StateMachine) error {
	segments, err := getSegments(".", ".log")
	if err != nil {
		return fmt.Errorf("recover: %w", err)
	}

	if len(segments) == 0 {
		return nil
	}

	w.epoch = uint32(len(segments))

	currentFile, err := os.Open(segments[len(segments)-1])
	if err != nil {
		return fmt.Errorf("recover open segment: %w", err)
	}
	defer currentFile.Close()

	var expectedSeq uint32 = 1

	for {
		headerBuf := make([]byte, HEADER_SIZE)
		if _, err := io.ReadFull(currentFile, headerBuf); err == io.EOF {
			break
		} else if err != nil {
			break
		}

		seq := binary.LittleEndian.Uint32(headerBuf[4:8])
		recType := headerBuf[8]
		length := binary.LittleEndian.Uint32(headerBuf[9:13])
		storedChecksum := binary.LittleEndian.Uint64(headerBuf[13:HEADER_SIZE])

		payload := make([]byte, length)
		if _, err := io.ReadFull(currentFile, payload); err != nil {
			break
		}

		computedChecksum := xxhash.Sum64(append(headerBuf[:13], payload...))
		if computedChecksum != storedChecksum {
			break
		}

		if seq != expectedSeq {
			break
		}
		expectedSeq++

		if recType == TypeCheckpoint {
			cpEpoch := binary.LittleEndian.Uint32(payload[0:4])
			cpSeq := binary.LittleEndian.Uint32(payload[4:8])
			storedSnapChecksum := binary.LittleEndian.Uint64(payload[8:16])

			snapData, err := os.ReadFile(getSnapshotPath(cpEpoch, cpSeq))
			if err != nil {
				// snapshot missing, stop — can't trust state past this point
				break
			}
			if xxhash.Sum64(snapData) != storedSnapChecksum {
				// snapshot corrupt, same fallback
				break
			}
			if err := sm.Restore(snapData); err != nil {
				return fmt.Errorf("restore snapshot (%d, %d): %w", cpEpoch, cpSeq, err)
			}

			// everything before this is now covered by the snapshot
			expectedSeq = seq + 1
			continue
		}

		if recType != TypeWrite {
			continue
		}

		parts := strings.SplitN(string(payload), " ", 2)
		if len(parts) < 2 {
			break
		}
		method := parts[0]
		keyValue := strings.SplitN(parts[1], ":", 2)

		record := Record{
			Key:    keyValue[0],
			Method: method,
		}
		if len(keyValue) > 1 {
			record.Value = keyValue[1]
		}

		if err := sm.Apply(record); err != nil {
			return err
		}
	}

	w.seq = expectedSeq - 1
	return nil
}
