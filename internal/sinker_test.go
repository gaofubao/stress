package internal

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/gaofubao/stress/v1.0.0/buffer"
	"log"
	"testing"
)

func TestSinkToTCP_Sink(t *testing.T) {
	s, err := NewSinkToTCP("192.168.38.60:8881")
	if err != nil {
		t.Fatalf("实例化TCP发送器失败: %s\n", err.Error())
	}

	pool, err := buffer.NewPool(1000, 1)
	if err != nil {
		t.Fatalf("实例化缓冲池失败: %s\n", err.Error())
	}

	data := []byte("hello\n")
	for i := 0; i < 10; i++ {
		err = pool.Put(data)
		if err != nil {
			t.Fatalf("向缓冲池中写入数据失败: %s\n", err.Error())
		}
	}

	s.Sink(pool)
}

func TestSinkToUDP_Sink(t *testing.T) {
	s, err := NewSinkToUDP("192.168.38.60:8882")
	if err != nil {
		t.Fatalf("实例化UDP发送器失败: %s\n", err.Error())
	}

	pool, err := buffer.NewPool(1000, 1)
	if err != nil {
		t.Fatalf("实例化缓冲池失败: %s\n", err.Error())
	}

	data := []byte("hello\n")
	for i := 0; i < 10; i++ {
		err = pool.Put(data)
		if err != nil {
			t.Fatalf("向缓冲池中写入数据失败: %s\n", err.Error())
		}
	}

	s.Sink(pool)
}

func TestSinkToHTTP_Sink(t *testing.T) {
	s, err := NewSinkToHTTP("192.168.38.60:8883")
	if err != nil {
		t.Fatalf("实例化HTTP发送器失败: %s\n", err.Error())
	}

	pool, err := buffer.NewPool(1000, 1)
	if err != nil {
		t.Fatalf("实例化缓冲池失败: %s\n", err.Error())
	}

	data := []byte(`{"name":"tom"}`)
	for i := 0; i < 1000; i++ {
		err = pool.Put(data)
		if err != nil {
			t.Fatalf("向缓冲池中写入数据失败: %s\n", err.Error())
		}
	}

	s.Sink(pool)
}

func TestSinkToES_Sink(t *testing.T) {
	s, err := NewSinkToES("192.168.38.60:9200",
		"stress-2020-03-13-01",
		3,
		1,
		1,
		1000000)
	if err != nil {
		log.Printf("初始化ES发送器失败: %s", err.Error())
	}

	pool, err := buffer.NewPool(1000, 1)
	if err != nil {
		t.Fatalf("实例化缓冲池失败: %s", err.Error())
	}

	for i := 0; i < 10; i++ {
		data := []byte(`{"name":"tom"}`)
		err = pool.Put(data)
		if err != nil {
			t.Fatalf("向缓冲池中写入数据失败: %s", err.Error())
		}
	}

	s.Sink(pool)
}

func TestSinkToKafka_Sink(t *testing.T) {
	s, err := NewSinkToKafka("192.168.38.60:9092", "test")
	pool, err := buffer.NewPool(1000, 1)
	if err != nil {
		t.Fatalf("实例化缓冲池失败: %s", err.Error())
	}

	for i := 0; i < 10; i++ {
		data := []byte(`{"name":"tom"}`)
		err = pool.Put(data)
		if err != nil {
			t.Fatalf("向缓冲池中写入数据失败: %s", err.Error())
		}
	}

	s.Sink(pool)
}

func TestKafka(t *testing.T) {
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": "192.168.38.60",
	})
	if err != nil {
		t.Fatalf("%s\n", err.Error())
	}

	doneChan := make(chan bool)
	go func() {
		defer close(doneChan)
		for e := range p.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				m := ev
				if m.TopicPartition.Error != nil {
					fmt.Println(m.TopicPartition.Error)
				} else {
					fmt.Println(*m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)
				}
				return
			default:
				fmt.Println(ev)
			}
		}
	}()

	value := "hello"
	topic := "test"
	p.ProduceChannel() <- &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic: &topic,
			Partition: kafka.PartitionAny,
		},
		Value: []byte(value)}

	_ = <-doneChan
	p.Close()
}