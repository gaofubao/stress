package internal

import (
	"bytes"
	"context"
	"encoding/json"
	"github.com/cenkalti/backoff/v4"
	"github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esutil"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/gaofubao/stress/v1.0.0/buffer"
)

// NOTE: struct中的成员最好不对外，通过提供方法来查看或修改成员字面量

// 数据输出
type Sinker interface {
	Sink(pool buffer.Pool)
	TotalNum() uint64	// 统计发送成功的数据量
	ErrorNum() uint64	// 统计发送失败的数据量
}

// 监控指标
type sinkMetric struct {
	totalNum uint64 // 发送成功的数据量
	errorNum uint64 // 发送失败的数据量
}

func (s *sinkMetric) TotalNum() uint64 {
	return atomic.LoadUint64(&s.totalNum)
}

func (s *sinkMetric) ErrorNum() uint64 {
	return atomic.LoadUint64(&s.errorNum)
}

// 通过TCP协议输出数据
type sinkToTCP struct {
	address  string // 网络地址与端口
	sinkMetric
}

func NewSinkToTCP(address string) (Sinker, error) {
	// 校验address
	if _, _, err := net.SplitHostPort(address); err != nil {
		return nil, err
	}
	return &sinkToTCP{
		address: address,
	}, nil
}

func (s *sinkToTCP) Sink(pool buffer.Pool) {
	// 创建连接, 并控制连接超时为3秒
	var conn net.Conn
	conn, err := net.DialTimeout("tcp", s.address, 3 * time.Second)
	if err != nil {
		log.Panicf("建立TCP连接失败: %s\n", err.Error())
	}
	defer conn.Close()

	// 开始读取数据并发送
	for {
		datum, err := pool.Get()
		if err != nil {
			log.Printf("从缓冲池中读取数据失败: %s\n", err.Error())
			continue
		}
		// 判断数据类型，只支持发送[]byte类型数据
		switch v := datum.(type) {
		case []byte:
			// 设置发送超时时长为3秒
			if err = conn.SetWriteDeadline(time.Now().Add(3 * time.Second)); err != nil {
				log.Panicf("设置发送超时时长失败: %s\n", err.Error())
			}
			// FIXME: 传输过程中连接断开后恢复，数据一直发送失败(使用连接池)
			_, err = conn.Write(v)
			if err != nil {
				// TODO: 检查连接是否失效
				atomic.AddUint64(&s.errorNum, 1)
				log.Printf("发送数据失败: %s\n", err.Error())
				continue
			}
			atomic.AddUint64(&s.totalNum, 1)
		default:
			atomic.AddUint64(&s.errorNum, 1)
			log.Printf("数据类型不是[]byte: %T\n", v)
		}
	}
}

type sinkToUDP struct {
	address string
	sinkMetric
}

func NewSinkToUDP(address string) (Sinker, error) {
	// 校验address
	if _, _, err := net.SplitHostPort(address); err != nil {
		return nil, err
	}
	return &sinkToUDP{
		address: address,
	}, nil
}

func (s *sinkToUDP) Sink(pool buffer.Pool) {
	conn, err := net.DialTimeout("udp", s.address, 3 * time.Second)
	if err != nil {
		log.Panicf("建立UDP连接失败: %s\n", err.Error())
	}
	defer conn.Close()

	// 开始读取数据并发送
	for {
		datum, err := pool.Get()
		if err != nil {
			log.Printf("从缓冲池中读取数据失败: %s\n", err.Error())
			continue
		}
		// 判断数据类型，只支持发送[]byte类型数据
		switch v := datum.(type) {
		case []byte:
			// 设置发送超时时长为3秒
			if err = conn.SetWriteDeadline(time.Now().Add(3 * time.Second)); err != nil {
				log.Panicf("设置发送超时时长失败: %s\n", err.Error())
			}
			_, err = conn.Write(v)
			if err != nil {
				atomic.AddUint64(&s.errorNum, 1)
				// FIXME: write: no buffer space available
				log.Printf("发送数据失败: %s\n", err.Error())
				continue
			}
			atomic.AddUint64(&s.totalNum, 1)
		default:
			atomic.AddUint64(&s.errorNum, 1)
			log.Printf("数据类型不是[]byte: %T\n", v)
		}
	}
}

// 通过http协议输出数据
type sinkToHTTP struct {
	address string
	sinkMetric
}

func NewSinkToHTTP(address string) (Sinker, error) {
	// 校验address
	if _, _, err := net.SplitHostPort(address); err != nil {
		return nil, err
	}
	address = "http://" + address
	return &sinkToHTTP{
		address: address,
	}, nil
}

func (s *sinkToHTTP) Sink(pool buffer.Pool) {
	// 开启长连接
	client := &http.Client{Transport: &http.Transport{
		DialContext: (&net.Dialer{
			Timeout: 30 * time.Second,
			KeepAlive: 60 * time.Second,
		}).DialContext,
		MaxIdleConns: 500,
		IdleConnTimeout: 60 * time.Second,
		ExpectContinueTimeout: 30 * time.Second,
		MaxIdleConnsPerHost: 100,
	}}

	for {
		time.Sleep(time.Microsecond * 500)
		datum, err := pool.Get()
		if err != nil {
			log.Printf("从缓冲池中读取数据失败: %s\n", err.Error())
			continue
		}
		switch v := datum.(type) {
		case []byte:
			req, err := http.NewRequest(http.MethodPost, s.address, bytes.NewReader(v))
			if err != nil {
				atomic.AddUint64(&s.errorNum, 1)
				log.Printf("创建HTTP请求失败: %s\n", err.Error())
				continue
			}
			req.Header.Set("Content-Type", "application/json")
			res, err := client.Do(req)
			if err != nil || res.StatusCode != http.StatusOK {
				atomic.AddUint64(&s.errorNum, 1)
				log.Printf("数据发送失败: %s %d\n", err, res.StatusCode)
				continue
			}
			// NOTE: 一定要接收返回值，否则客户端会发起新的连接
			ioutil.ReadAll(res.Body)
			// 关闭response.Body, 否则会占用过多fd
			res.Body.Close()
			atomic.AddUint64(&s.totalNum, 1)
		default:
			atomic.AddUint64(&s.errorNum, 1)
			log.Printf("数据类型不是[]byte: %T\n", v)
		}
	}
}

// 直接发送数据到Elasticsearch
type sinkToES struct {
	address       string // ES访问地址
	indexName     string // 索引名
	shards        int    // 分片数
	replicas      int    // 副本数
	workers       int    // 工作goroutine数
	flushBytes    int    // flush阈值
	sinkMetric
}

func NewSinkToES(address, indexName string,
	shards, replicas, workers, flushBytes int) (Sinker, error) {
	// 校验address
	if _, _, err := net.SplitHostPort(address); err != nil {
		return nil, err
	}
	address = "http://" + address
	return &sinkToES{
		address: address,
		indexName: indexName,
		shards: shards,
		replicas: replicas,
		workers: workers,
		flushBytes: flushBytes,
	}, nil
}

func (s *sinkToES) Sink(pool buffer.Pool) {
	// 指数补偿算法
	retryBackoff := backoff.NewExponentialBackOff()
	// 1. 创建ES客户端
	es, err := elasticsearch.NewClient(elasticsearch.Config{
		Addresses: []string{
			s.address,
		},
		RetryOnStatus: []int{502, 503, 504, 429},
		RetryBackoff: func(attempt int) time.Duration {
			if attempt == 1 {
				retryBackoff.Reset()
			}
			return retryBackoff.NextBackOff()
		},
		MaxRetries: 5,
	})
	if err != nil {
		log.Panicf("创建elasticsearch客户端失败: %s\n", err.Error())
	}

	// 2. 创建索引
	// 如果存在同名索引则先删除再创建
	res, err := es.Indices.Delete([]string{s.indexName}, es.Indices.Delete.WithIgnoreUnavailable(true))
	if err != nil || res.IsError() {
		log.Panicf("删除索引失败: %s\n", err.Error())
	}
	res.Body.Close()
	// 创建索引
	// 设置索引settings, 字段首字母需大写
	type indexSettings struct {
		Shards   int `json:"number_of_shards"`
		Replicas int `json:"number_of_replicas"`
	}
	type settings struct {
		Settings indexSettings `json:"settings"`
	}
	set, err := json.Marshal(&settings{
		Settings: indexSettings{
			Shards:   s.shards,
			Replicas: s.replicas,
		},
	})
	res, err = es.Indices.Create(s.indexName, es.Indices.Create.WithBody(bytes.NewReader(set)))
	if err != nil || res.IsError() {
		log.Panicf("创建索引失败: %s\n", err.Error())
	}
	res.Body.Close()
	if err != nil {
		log.Panicf("序列化索引settings失败: %s\n", err.Error())
	}

	// 3. 创建一个bulk indexer
	bi, err := esutil.NewBulkIndexer(esutil.BulkIndexerConfig{
		Client:        es,                // es客户端
		NumWorkers:    s.workers,         // worker goroutine数
		FlushBytes:    s.flushBytes,      // flush阈值
		FlushInterval: 30 * time.Second,  // flush时间间隔
		Index:         s.indexName,       // 索引名
	})
	if err != nil {
		log.Panicf("创建indexer失败: %s\n", err.Error())
	}
	// 关闭indexer
	defer func() {
		err = bi.Close(context.Background())
		if err != nil {
			log.Panicf("关闭indexer失败: %s\n", err.Error())
		}
	}()

	// 4. 向ES中写入文档
	for {
		datum, err := pool.Get()
		if err != nil {
			log.Printf("从缓冲池中读取数据失败: %s", err.Error())
			continue
		}
		switch v := datum.(type) {
		case []byte:
			document := bytes.NewReader(v)
			err = bi.Add(context.Background(),
				esutil.BulkIndexerItem{
					Action: "index",
					Index:  s.indexName,
					Body:   document,
					OnSuccess: func(ctx context.Context, item esutil.BulkIndexerItem, item2 esutil.BulkIndexerResponseItem) {
						atomic.AddUint64(&s.totalNum, 1)
					},
					OnFailure: func(ctx context.Context, item esutil.BulkIndexerItem, item2 esutil.BulkIndexerResponseItem, err error) {
						atomic.AddUint64(&s.errorNum, 1)
					},
				})
			if err != nil {
				log.Printf("写入文档失败: %s\n", err.Error())
				continue
			}
		default:
			log.Printf("数据类型不是[]byte: %T\n", v)
		}
	}
}

// 直接发送数据到Kafka
type sinkToKafka struct {
	address string
	topic   string
	sinkMetric
}

func NewSinkToKafka(address, topic string) (Sinker, error) {
	// 校验address
	if _, _, err := net.SplitHostPort(address); err != nil {
		return nil, err
	}
	return &sinkToKafka{
		address: address,
		topic:   topic,
	}, nil
}

func (s *sinkToKafka) Sink(pool buffer.Pool)  {
	var wg sync.WaitGroup
	// 用于控制查看事件的goroutine的退出
	done := make(chan struct{})
	// 1. 创建Kafka生产者
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": s.address,
	})
	if err != nil {
		log.Panicf("创建Kafka生产者失败: %s\n", err.Error())
	}
	defer func() {
		// 关闭查看事件的goroutine
		done <- struct {}{}
		wg.Wait()
		// 关闭生产者连接
		p.Close()
	}()

	// 2. 查看事件
	wg.Add(1)
	go func(done <- chan struct{}) {
		defer wg.Done()
		for {
			select {
			case e := <- p.Events():
				switch m := e.(type) {
				case *kafka.Message:
					if m.TopicPartition.Error != nil {
						atomic.AddUint64(&s.errorNum, 1)
					} else {
						atomic.AddUint64(&s.totalNum, 1)
					}
				default:
					log.Printf("忽略该事件: %s\n", m)
				}
			case <-done:
				// 退出goroutine，防止泄漏
				return
			}
		}
	}(done)

	// 3. 写入数据, 使用produce channel方式
	for {
		datum, err := pool.Get()
		if err != nil {
			log.Printf("从缓冲池中读取数据失败: %s", err.Error())
			continue
		}
		switch v := datum.(type) {
		case []byte:
			p.ProduceChannel() <- &kafka.Message{
				TopicPartition: kafka.TopicPartition{Topic: &s.topic, Partition: kafka.PartitionAny},
				Value:          v,
			}
		default:
			log.Printf("数据: %s, 不是[]byte类型\n", v)
		}
	}
}

// TODO: 实现用户指定发送速率
