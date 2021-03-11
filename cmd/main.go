package main

import (
	"log"
	"time"

	"github.com/gaofubao/stress/v1/buffer"
	"github.com/gaofubao/stress/v1/internal"
	"github.com/gaofubao/stress/v1/monitor"
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

	g := &internal.GenerateFromFaker{}

	protocol := "tcp"
	address := "127.0.0.1:8888"
	s := &internal.SinkToNet{
		Protocol: protocol,
		Address:  address,
	}

	m := &monitor.Monitor{
		StartTime: time.Now(),
		Info:      monitor.ServiceMetrics{},
		Queue:     make([]uint64, 2),
	}

	// 模块启动
	go g.Generate(pool)
	go s.Sink(pool)
	m.Start(pool, s)
}
