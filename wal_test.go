package wal

import (
	"fmt"
	"os"
	"strings"
	"testing"
	"time"
)

// testSM is a minimal StateMachine for WAL tests.
// Avoids any dependency on the kv example package.
type testSM struct {
	data map[string]string
}

func newTestSM() *testSM {
	return &testSM{data: make(map[string]string)}
}

func (s *testSM) Apply(r Record) error {
	switch r.Method {
	case "PUT":
		s.data[r.Key] = r.Value
	case "DELETE":
		delete(s.data, r.Key)
	default:
		return fmt.Errorf("unknown method: %s", r.Method)
	}
	return nil
}

func (s *testSM) Snapshot() []byte {
	var b []byte
	for k, v := range s.data {
		b = append(b, []byte(k+"="+v+"\n")...)
	}
	return b
}

func (s *testSM) Restore(data []byte) error {
	s.data = make(map[string]string)
	for _, line := range strings.Split(string(data), "\n") {
		if line == "" {
			continue
		}
		parts := strings.SplitN(line, "=", 2)
		if len(parts) == 2 {
			s.data[parts[0]] = parts[1]
		}
	}

	return nil
}

func (s *testSM) get(key string) string {
	return s.data[key]
}

// helpers
func setupDir(t *testing.T) {
	t.Helper()
	dir := t.TempDir()
	if err := os.Chdir(dir); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(SNAPSHOTS_DIR, 0755); err != nil {
		t.Fatal(err)
	}
}

func startWAL(t *testing.T, sm StateMachine) *WAL {
	t.Helper()
	w := New()
	go func() {
		w.Start(sm)
	}()
	return w
}

// WAL tests

func TestWALRecoverEmpty(t *testing.T) {
	setupDir(t)
	sm := newTestSM()
	w := New()
	if err := w.Recover(sm); err != nil {
		t.Fatalf("recover on empty dir: %v", err)
	}
}

func TestWALAppendRecover(t *testing.T) {
	setupDir(t)
	sm := newTestSM()
	w := startWAL(t, sm)

	w.Append(Record{Method: "PUT", Key: "foo", Value: "bar"})
	w.Append(Record{Method: "PUT", Key: "baz", Value: "qux"})

	time.Sleep(20 * time.Millisecond) // let the ticker flush

	sm2 := newTestSM()
	w2 := New()
	if err := w2.Recover(sm2); err != nil {
		t.Fatalf("recover: %v", err)
	}

	if got := sm2.get("foo"); got != "bar" {
		t.Fatalf("expected bar got %s", got)
	}
	if got := sm2.get("baz"); got != "qux" {
		t.Fatalf("expected qux got %s", got)
	}
}

func TestWALAppendDelete(t *testing.T) {
	setupDir(t)
	sm := newTestSM()
	w := startWAL(t, sm)

	w.Append(Record{Method: "PUT", Key: "foo", Value: "bar"})
	w.Append(Record{Method: "DELETE", Key: "foo"})

	sm2 := newTestSM()
	w2 := New()
	if err := w2.Recover(sm2); err != nil {
		t.Fatalf("recover: %v", err)
	}

	if got := sm2.get("foo"); got != "" {
		t.Fatalf("expected empty got %s", got)
	}
}

func TestWALCheckpoint(t *testing.T) {
	setupDir(t)
	sm := newTestSM()

	w := NewWithOptions(2) // not startWAL — we need snapshotCount=2
	go w.Start(sm)
	time.Sleep(5 * time.Millisecond) // wait for Start to open the file

	w.Append(Record{Method: "PUT", Key: "foo", Value: "bar"})
	w.Append(Record{Method: "PUT", Key: "baz", Value: "qux"})
	w.Append(Record{Method: "PUT", Key: "after", Value: "checkpoint"})

	sm2 := newTestSM()
	w2 := New()
	if err := w2.Recover(sm2); err != nil {
		t.Fatalf("recover: %v", err)
	}

	if got := sm2.get("foo"); got != "bar" {
		t.Fatalf("expected bar got %s", got)
	}
	if got := sm2.get("after"); got != "checkpoint" {
		t.Fatalf("expected checkpoint got %s", got)
	}
}

func TestWALCheckpointDeletesCoveredSegments(t *testing.T) {
	setupDir(t)
	sm := newTestSM()
	w := startWAL(t, sm)

	w.Append(Record{Method: "PUT", Key: "foo", Value: "bar"})

	if err := w.Checkpoint(sm); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}

	segments, err := getSegments(".")
	if err != nil {
		t.Fatal(err)
	}
	for _, seg := range segments {
		var epoch uint32
		fmt.Sscanf(seg, "wal-%d.log", &epoch)
		if epoch < w.epoch {
			t.Fatalf("segment %s should have been deleted", seg)
		}
	}
}

func TestWALCorruptedRecord(t *testing.T) {
	setupDir(t)
	sm := newTestSM()
	w := startWAL(t, sm)

	w.Append(Record{Method: "PUT", Key: "foo", Value: "bar"})
	w.Append(Record{Method: "PUT", Key: "baz", Value: "qux"})

	time.Sleep(20 * time.Millisecond) // wait for flush

	segments, _ := getSegments(".")
	if len(segments) == 0 {
		t.Fatal("no segments found")
	}

	f, err := os.OpenFile(segments[len(segments)-1], os.O_WRONLY, 0644)
	if err != nil {
		t.Fatal(err)
	}
	f.Seek(-4, 2)
	f.Write([]byte{0xFF, 0xFF, 0xFF, 0xFF})
	f.Close()

	sm2 := newTestSM()
	w2 := New()
	if err := w2.Recover(sm2); err != nil {
		t.Fatalf("recover should handle corruption gracefully: %v", err)
	}
}

func TestWALCrashBetweenPhases(t *testing.T) {
	setupDir(t)
	sm := newTestSM()
	w := startWAL(t, sm)

	w.Append(Record{Method: "PUT", Key: "foo", Value: "bar"})

	// phase 1 only - snapshot file written, no WAL marker
	data := sm.Snapshot()
	os.WriteFile(getSnapshotPath(w.epoch, w.seq), data, 0644)

	sm2 := newTestSM()
	w2 := New()
	if err := w2.Recover(sm2); err != nil {
		t.Fatalf("recover after phase 1 crash: %v", err)
	}

	// record should still be recovered via WAL replay
	if got := sm2.get("foo"); got != "bar" {
		t.Fatalf("expected bar got %s", got)
	}
}

func TestWALManyRecords(t *testing.T) {
	setupDir(t)
	sm := newTestSM()
	w := startWAL(t, sm)

	for i := range 1000 {
		key := fmt.Sprintf("key-%d", i)
		val := fmt.Sprintf("val-%d", i)
		if err := w.Append(Record{Method: "PUT", Key: key, Value: val}); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}

	sm2 := newTestSM()
	w2 := New()
	if err := w2.Recover(sm2); err != nil {
		t.Fatalf("recover: %v", err)
	}

	for i := range 1000 {
		key := fmt.Sprintf("key-%d", i)
		val := fmt.Sprintf("val-%d", i)
		if got := sm2.get(key); got != val {
			t.Fatalf("key %s: expected %s got %s", key, val, got)
		}
	}
}
