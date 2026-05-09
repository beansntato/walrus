package main

import (
	"fmt"

	"beansnbeans.dev/wal"
)

type methodType int

const (
	PUT methodType = iota
)

var methodName = map[methodType]string{
	PUT: "PUT",
}

type KV struct {
	data map[string]string
	wal  *wal.WAL
}

func main() {
	wal := wal.New()
	kv := &KV{data: make(map[string]string), wal: wal}
	wal.Recover(kv)

	go wal.Start()

	// kv.Put("b", "2")
	kv.print()
}

func (kv *KV) print() {
	fmt.Println("kv data")
	for i, v := range kv.data {
		fmt.Println(i, " - ", v)
	}
}

func (kv *KV) Put(key, value string) error {
	record := wal.Record{
		Key:    key,
		Value:  value,
		Method: methodName[PUT],
	}

	fmt.Println("running")
	err := kv.wal.Append(record)
	fmt.Println("err", err)
	if err != nil {
		return err
	}

	kv.Apply(record)
	return nil
}

func (kv *KV) Apply(record wal.Record) error {
	switch record.Method {
	case "PUT":
		kv.data[record.Key] = record.Value
	default:
		fmt.Println("Working in progress on this method bruh")
	}

	return nil
}

func (k *KV) Snapshot() {}

func (k *KV) Restore() {}
