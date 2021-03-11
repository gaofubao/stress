package main

import (
	"github.com/gaofubao/stress/v1.0.0/buffer"
	"github.com/gaofubao/stress/v1.0.0/internal"
	"github.com/gaofubao/stress/v1.0.0/monitor"
	"log"
	"runtime"
	"time"
)

func init() {
	//address := flag.String("addr", "127.0.0.1:514", "请输入服务端的IP地址和端口，格式为: ip:port")
	//protocol := flag.String("prot", "tcp", "请输入传输协议，tcp或udp")
	//cacheSize := flag.Int("size", 1000, "请输入缓存队列大小")
	//
	//flag.Parse()
	//fmt.Println(*address)
	//fmt.Println(*protocol)
	//fmt.Println(*cacheSize)
}

func main() {
	// 实例化
	bufferCap := uint32(100)
	maxBufferNumber := uint32(10)
	pool, err := buffer.NewPool(bufferCap, maxBufferNumber)
	if err != nil {
		log.Fatalf("创建缓冲池失败: %s, 单个缓冲器大小为: %d, 缓冲器数量为: %d\n", err.Error(), bufferCap, maxBufferNumber)
	}

	g, _ := internal.NewGenFromFaker()

	m, _ := monitor.NewMonitor(9999)

	//protocol := "tcp"
	//address := "127.0.0.1:8888"
	//s, err := internal.NewSinkToNet(protocol, address)
	//if err != nil {
	//	log.Fatalf("初始化数据发送器失败: %s", err.Error())
	//}

	address := "http://192.168.38.60:9200"
	indexName := "stress-index"
	shards := 3
	replicas := 0
	workers := runtime.NumCPU()
	flushBytes := 1000000
	flushInterval := 30 * time.Second
	s, _ := internal.NewSinkToES(address, indexName, shards, replicas, workers, flushBytes, flushInterval)


	// 模块启动
	go g.Generate(pool)
	go s.Sink(pool)
	m.Start(pool, s)
}
