package internal

import (
	"testing"

	"github.com/gaofubao/stress/v1.0.0/buffer"
)

func TestSinkToNet_Sink(t *testing.T) {
	s := &sinkToNet{
		protocol: "tcp",
		address:  "127.0.0.1:8888",
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

	s := &sinkToES{
		address:    "http://192.168.38.60:9200",
		indexName:  "ut-index",
		shards:     3,
		replicas:   1,
		workers:    1,
		flushBytes: 1024,
	}

	data := []byte(`{"name":"tom"}`)
	err = pool.Put(data)
	if err != nil {
		t.Fatal("向缓冲池中写入数据失败:", err.Error())
	}

	s.Sink(pool)
}
