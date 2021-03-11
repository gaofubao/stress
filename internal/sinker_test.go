package internal

import (
	"stress/buffer"
	"testing"
)

func TestSinkToNet_Sink(t *testing.T) {
	s := &SinkToNet{
		Protocol: "tcp",
		Address:  "127.0.0.1:8888",
	}

	pool, err := buffer.NewPool(100, 10)
	if err != nil {
		t.Fatal("实例化缓冲池失败", err.Error())
	}

	data := []byte("hello")
	err = pool.Put(data)
	if err != nil {
		t.Fatal("向缓冲池中写入数据失败:", err.Error())
	}

	s.Sink(pool)
}

func TestSinkToES_Sink(t *testing.T) {
	pool, err := buffer.NewPool(100, 10)
	if err != nil {
		t.Fatal("实例化缓冲池失败", err.Error())
	}

	s := &SinkToES{
		Address: "http://192.168.38.60:9200",
		IndexName: "ut-index",
		Shards: 3,
		Replicas: 1,
		Workers: 1,
		FlushBytes: 1024,
	}

	data := []byte(`{"name":"tom"}`)
	err = pool.Put(data)
	if err != nil {
		t.Fatal("向缓冲池中写入数据失败:", err.Error())
	}

	s.Sink(pool)
}
