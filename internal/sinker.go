package internal

import (
	"bytes"
	"context"
	"encoding/json"
	"github.com/cenkalti/backoff/v4"
	"github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esutil"
	"log"
	"net"
	"stress/buffer"
	"stress/monitor"
	"sync/atomic"
	"time"
)

// 数据输出
type Sinker interface {
	Sink(pool buffer.Pool)
}

// 通过TCP/UDP协议输出数据
type SinkToNet struct {
	Protocol string	   // 协议
	Address  string    // 网络地址与端口
	monitor.SinkMetrics
	//TotalNum uint64    // 发送成功的数据量
	//ErrorNum uint64	   // 发送失败的数据量
}

func (s *SinkToNet) Sink(pool buffer.Pool) {
	// 创建连接, 并控制连接超时
	conn, err := net.DialTimeout(s.Protocol, s.Address, time.Second * 3)
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
				atomic.AddUint64(&s.ErrorNum, 1)
				log.Printf("发送数据失败: %s\n", err.Error())
				continue
			}
			atomic.AddUint64(&s.TotalNum, 1)
		default:
			atomic.AddUint64(&s.ErrorNum, 1)
			log.Printf("数据类型不是[]byte: %T\n", v)
		}
	}
}



// TODO: 通过http协议输出数据

// 直接发送数据到Elasticsearch
type SinkToES struct {
	Address       string	// ES访问地址
	IndexName     string	// 索引名
	Shards		  int		// 分片数
	Replicas      int		// 副本数
	Workers   	  int		// 工作goroutine数
	FlushBytes    int		// flush阈值
	FlushInterval int		// flush时间间隔
}


func (s *SinkToES) Sink(pool buffer.Pool) {
	// 指数补偿算法
	retryBackoff := backoff.NewExponentialBackOff()
	// 1. 创建ES客户端
	es, err := elasticsearch.NewClient(elasticsearch.Config{
		Addresses: []string{
			s.Address,
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
	res, err := es.Indices.Delete([]string{s.IndexName}, es.Indices.Delete.WithIgnoreUnavailable(true))
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
			Shards: s.Shards,
			Replicas: s.Replicas,
		},
	})
	res, err = es.Indices.Create(s.IndexName, es.Indices.Create.WithBody(bytes.NewReader(set)))
	if err != nil || res.IsError() {
		log.Fatal("创建索引失败:", err.Error())
	}
	res.Body.Close()
	if err != nil {
		log.Fatal("序列化索引settings失败:", err.Error())
	}

	// 3. 创建一个bulk indexer
	bi, err := esutil.NewBulkIndexer(esutil.BulkIndexerConfig{
		Client: es,                      // es客户端
		NumWorkers: s.Workers,           // worker goroutine数
		FlushBytes: s.FlushBytes,        // flush阈值
		FlushInterval: 30 * time.Second, // flush时间间隔
		Index: s.IndexName,              // 索引名
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
					Index: s.IndexName,
					Body: document,
					OnSuccess: func(ctx context.Context, item esutil.BulkIndexerItem, item2 esutil.BulkIndexerResponseItem) {
						log.Println("success")
					},
					OnFailure: func(ctx context.Context, item esutil.BulkIndexerItem, item2 esutil.BulkIndexerResponseItem, err error) {
						log.Println("failed")
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

	// 返回indexer统计信息
	//biStats := bi.Stats()
	//log.Println("indexer统计信息:", biStats)
}

// TODO: 直接发送数据到Kafka

// TODO: 实现用户指定发送速率


