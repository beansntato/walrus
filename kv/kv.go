package main

import (
	"encoding/json"
	"fmt"

	"beansnbeans.dev/wal"
)

type KV struct {
	data map[string]string
}

func NewKV() *KV {
	return &KV{data: make(map[string]string)}
}

func (k *KV) Apply(r wal.Record) error {
	switch r.Method {
	case "PUT":
		k.data[r.Key] = r.Value
	case "DELETE":
		delete(k.data, r.Key)
	default:
		return fmt.Errorf("unknown method: %s", r.Method)
	}
	return nil
}

func (k *KV) Snapshot() []byte {
	data, _ := json.Marshal(k.data)
	return data
}

func (k *KV) Restore(data []byte) error {
	return json.Unmarshal(data, &k.data)
}

func (k *KV) Get(key string) string {
	return k.data[key]
}
