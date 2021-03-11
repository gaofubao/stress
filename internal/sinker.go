package internal

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"sync/atomic"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esutil"
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

// 通过TCP/UDP协议输出数据
type sinkToNet struct {
	protocol string // 协议
	address  string // 网络地址与端口
	//totalNum uint64 // 发送成功的数据量
	//errorNum uint64 // 发送失败的数据量
	sinkMetric
}

func (s *sinkMetric) TotalNum() uint64 {
	return atomic.LoadUint64(&s.totalNum)
}

func (s *sinkMetric) ErrorNum() uint64 {
	return atomic.LoadUint64(&s.errorNum)
}

func NewSinkToNet(protocol, address string) (Sinker, error) {
	if protocol != "tcp" && protocol != "udp" {
		errMsg := fmt.Sprintf("不支持的协议: %s\n", protocol)
		return nil, errors.New(errMsg)
	}
	return &sinkToNet{
		protocol: protocol,
		address: address,
	}, nil
}

func (s *sinkToNet) Sink(pool buffer.Pool) {
	// 创建连接, 并控制连接超时
	conn, err := net.DialTimeout(s.protocol, s.address, time.Second*3)
	if err != nil {
		log.Fatalf("连接失败: %s\n", err.Error())
	}
	defer conn.Close()

	for {
		datum, err := pool.Get()
		if err != nil {
			log.Println("从缓冲池中读取数据失败:", err.Error())
			continue
		}
		// 数据类型判断，只支持发送[]byte类型数据
		switch v := datum.(type) {
		case []byte:
			// TODO: 发送超时控制
			// FIXME: 传输过程中连接断开后恢复，数据一直发送失败(使用连接池)
			_, err = conn.Write(v)
			if err != nil {
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

// TODO: 通过http协议输出数据

// 直接发送数据到Elasticsearch
type sinkToES struct {
	address       string // ES访问地址
	indexName     string // 索引名
	shards        int    // 分片数
	replicas      int    // 副本数
	workers       int    // 工作goroutine数
	flushBytes    int    // flush阈值
	flushInterval time.Duration    // flush时间间隔
	sinkMetric
}

func NewSinkToES(address, indexName string,
	shards, replicas, workers, flushBytes int,
	flushInterval time.Duration) (Sinker, error) {
	return &sinkToES{
		address: address,
		indexName: indexName,
		shards: shards,
		replicas: replicas,
		workers: workers,
		flushBytes: flushBytes,
		flushInterval: flushInterval,
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
		log.Fatal("创建elasticsearch客户端失败:", err.Error())
	}

	// 2. 创建索引
	// 如果存在同名索引则先删除再创建
	res, err := es.Indices.Delete([]string{s.indexName}, es.Indices.Delete.WithIgnoreUnavailable(true))
	if err != nil || res.IsError() {
		log.Fatal("删除索引失败:", err.Error())
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
		log.Fatal("创建索引失败:", err.Error())
	}
	res.Body.Close()
	if err != nil {
		log.Fatal("序列化索引settings失败:", err.Error())
	}

	// 3. 创建一个bulk indexer
	bi, err := esutil.NewBulkIndexer(esutil.BulkIndexerConfig{
		Client:        es,               // es客户端
		NumWorkers:    s.workers,        // worker goroutine数
		FlushBytes:    s.flushBytes,     // flush阈值
		FlushInterval: 30 * time.Second, // flush时间间隔
		Index:         s.indexName,      // 索引名
	})
	if err != nil {
		log.Fatal("创建indexer失败:", err.Error())
	}
	// 关闭indexer
	defer func() {
		err = bi.Close(context.Background())
		if err != nil {
			log.Fatal("关闭indexer失败:", err.Error())
		}
	}()

	// 4. 向ES中写入文档
	for {
		datum, err := pool.Get()
		if err != nil {
			log.Println("从缓冲池中读取数据失败:", err.Error())
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
				log.Println("写入文档失败:", err.Error())
				continue
			}
		default:
			log.Printf("数据类型不是[]byte: %T\n", v)
			continue
		}
	}
}

// TODO: 直接发送数据到Kafka

// TODO: 实现用户指定发送速率
