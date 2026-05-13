package wal

import (
	"encoding/binary"
	"fmt"
	"io"
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
	writeCh       chan Request
	seq           uint32
	epoch         uint32
	currentFile   *os.File
	snapshotCount uint32
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

const (
	MAX_SEGMENT_SIZE = 64 * 1024 * 1024 // 64MB
	SNAPSHOT_COUNT   = 100_000
	SNAPSHOTS_DIR    = "snapshot"
)
const HEADER_SIZE = 4 + // epoch
	4 + // local seq
	1 + // type
	4 + // length
	8 // xxh3 checksum

func New() *WAL {
	return &WAL{writeCh: make(chan Request, 1), snapshotCount: SNAPSHOT_COUNT}
}

func NewWithOptions(snapshotCount uint32) *WAL {
	return &WAL{
		writeCh:       make(chan Request, 1),
		snapshotCount: snapshotCount,
	}
}

func getSegments(root string) ([]string, error) {
	var files []string

	f, err := os.Open(root)
	if err != nil {
		return nil, fmt.Errorf("open dir %q: %w", root, err)
	}
	defer f.Close()

	names, err := f.Readdirnames(-1)
	if err != nil {
		return nil, fmt.Errorf("readdirnames %q: %w", root, err)
	}

	for _, name := range names {
		if strings.HasPrefix(name, "wal-") && strings.HasSuffix(name, ".log") {
			files = append(files, name)
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
		return fmt.Errorf("rotate stat: %w", err)
	}

	if info.Size() < MAX_SEGMENT_SIZE {
		return nil
	}

	if err := w.currentFile.Sync(); err != nil {
		return fmt.Errorf("rotate sync: %w", err)
	}
	if err := w.currentFile.Close(); err != nil {
		return fmt.Errorf("rotate close: %w", err)
	}

	w.epoch++
	w.seq = 0
	f, err := os.OpenFile(getSegmentPath(w.epoch), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("rotate open next segment: %w", err)
	}
	w.currentFile = f
	return nil
}

// this creates a snapshot to avoid having to replay all lines of a WAL file
// the snapshot contains the state of the StateMachine at that point in time
// which means it doesn't matter what records comes before
// that state is the consequence of all the records before it
func (w *WAL) Checkpoint(sm StateMachine) error {
	cpEpoch := w.epoch
	cpSeq := w.seq

	snapPath := getSnapshotPath(cpEpoch, cpSeq)
	tmpPath := snapPath + ".tmp"

	// Phase 1: write snapshot file.
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
	if dir, err := os.Open(SNAPSHOTS_DIR); err == nil {
		dir.Sync()
		dir.Close()
	}

	// Phase 2: write checkpoint marker.
	snapshotChecksum := xxhash.Sum64(data)

	payload := make([]byte, 16)
	binary.LittleEndian.PutUint32(payload[0:4], cpEpoch)
	binary.LittleEndian.PutUint32(payload[4:8], cpSeq)
	binary.LittleEndian.PutUint64(payload[8:16], snapshotChecksum)

	w.seq++
	fmt.Printf("writing checkpoint marker at header seq=%d cpSeq=%d\n", w.seq, cpSeq)
	header := buildHeader(cpEpoch, w.seq, TypeCheckpoint, payload)
	if _, err := w.currentFile.Write(append(header, payload...)); err != nil {
		return fmt.Errorf("checkpoint WAL write: %w", err)
	}
	if err := w.currentFile.Sync(); err != nil {
		return fmt.Errorf("checkpoint WAL sync: %w", err)
	}

	return w.compact(cpEpoch)
}

func (w *WAL) compact(currentEpoch uint32) error {
	segments, err := getSegments(".")
	if err != nil {
		return fmt.Errorf("compact: %w", err)
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

func (w *WAL) Start(sm StateMachine) error {
	segments, err := getSegments(".")
	if err != nil {
		return fmt.Errorf("start: %w", err)
	}

	var currentFile *os.File

	if len(segments) == 0 {
		w.epoch = 1
		w.seq = 0
		currentFile, err = os.OpenFile(getSegmentPath(w.epoch), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		if err != nil {
			return fmt.Errorf("create first segment: %w", err)
		}
	} else {
		w.epoch = uint32(len(segments))
		currentFile, err = os.OpenFile(segments[len(segments)-1], os.O_APPEND|os.O_WRONLY, 0644)
		if err != nil {
			return fmt.Errorf("open segment: %w", err)
		}
	}

	w.currentFile = currentFile
	defer w.currentFile.Close()

	var pending []Request
	var buffer []byte
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case req := <-w.writeCh:
			pending = append(pending, req)
			payload := []byte(fmt.Sprintf("%s %s:%s", req.Record.Method, req.Record.Key, req.Record.Value))
			fmt.Println(w.seq)
			w.seq++
			header := buildHeader(w.epoch, w.seq, TypeWrite, payload)
			buffer = append(buffer, header...)
			buffer = append(buffer, payload...)
			sm.Apply(req.Record)
		case <-ticker.C:
			if len(buffer) == 0 {
				continue
			}

			// flush batch first - snapshot must reflect committed state
			_, err := w.currentFile.Write(buffer)
			w.currentFile.Sync()

			// checkpoint before rotate so marker lands in the current segment
			if w.seq%w.snapshotCount == 0 {
				fmt.Println("CHECKPOINT", w.seq)
				w.Checkpoint(sm)
			}

			for _, req := range pending {
				req.Done <- err
			}
			pending = pending[:0]
			buffer = buffer[:0]

			w.rotate()
		}
	}
}

func (w *WAL) Append(record Record) error {
	done := make(chan error, 1)
	w.writeCh <- Request{Record: record, Done: done}
	return <-done
}

func (w *WAL) Recover(sm StateMachine) error {
	segments, err := getSegments(".")
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
			fmt.Printf("checksum mismatch at seq=%d type=%d\n", seq, recType)
			break
		}

		fmt.Printf("reading record2: seq=%d expectedSeq=%d type=%d\n", seq, expectedSeq, recType)
		if seq != expectedSeq {
			break
		}
		expectedSeq++

		fmt.Printf("reading record: seq=%d expectedSeq=%d type=%d\n", seq, expectedSeq, recType)
		if recType == TypeCheckpoint {
			fmt.Println("payload")
			cpEpoch := binary.LittleEndian.Uint32(payload[0:4])
			cpSeq := binary.LittleEndian.Uint32(payload[4:8])
			storedSnapChecksum := binary.LittleEndian.Uint64(payload[8:16])

			fmt.Println("reading file")
			snapData, err := os.ReadFile(getSnapshotPath(cpEpoch, cpSeq))
			fmt.Printf("snapData: %q\n", string(snapData))
			if err != nil {
				fmt.Printf("snapshot read error: %v\n", err)
				break
			}
			fmt.Println("read")
			computed := xxhash.Sum64(snapData)
			if computed != storedSnapChecksum {
				fmt.Printf("snapshot checksum mismatch: stored=%d computed=%d\n", storedSnapChecksum, computed)
				break
			}
			fmt.Println("correct checksum")
			if err := sm.Restore(snapData); err != nil {
				return fmt.Errorf("restore snapshot (%d, %d): %w", cpEpoch, cpSeq, err)
			}
			fmt.Println("restore")
			// fmt.Printf("state after restore: %v\n", sm.)

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
