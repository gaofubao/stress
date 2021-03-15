package main

import (
	"flag"
	"github.com/gaofubao/stress/v1.0.0/buffer"
	"github.com/gaofubao/stress/v1.0.0/internal"
	"github.com/gaofubao/stress/v1.0.0/monitor"
	"log"
	"os"
)

// 子命令
var tcpCmd, udpCmd, httpCmd, esCmd, kafkaCmd *flag.FlagSet
// tcp
var tcpAddress *string
var tcpGenNums, tcpWorkers, tcpBufCap, tcpMaxBufNum, tcpMonPort *int
// udp
var udpAddress *string
var udpGenNum, udpWorker, udpBufCap, udpMaxBufNum, udpMonPort *int
// http
var httpAddress *string
var httpGenNum, httpWorker, httpBufCap, httpMaxBufNum, httpMonPort *int
// es
var esAddress, esIndexName *string
var esGenNum, esWorker, esBufCap, esMaxBufNum, esMonPort, esShard, esReplica, esFlushByte *int
// kafka
var kafkaAddress, kafkaTopic *string
var kafkaGenNum, kafkaWorker, kafkaBufCap, kafkaMaxBufNum, kafkaMonPort *int

func init() {
	// tcp子命令
	tcpCmd       = flag.NewFlagSet("tcp", flag.ExitOnError)
	tcpAddress   = tcpCmd.String("addr", "127.0.0.1:8888", "(必填)TCP服务器地址,格式为: 'ip:port'")
	tcpGenNums   = tcpCmd.Int("num", 1, "数据生成组件的实例数")
	tcpWorkers   = tcpCmd.Int("worker", 1, "数据输出组件的实例数")
	tcpBufCap    = tcpCmd.Int("bufCap", 10000, "缓冲池内单个缓冲器的大小")
	tcpMaxBufNum = tcpCmd.Int("maxBufNum", 5, "缓冲池内缓冲器的最大数量")
	tcpMonPort   = tcpCmd.Int("monPort", 9999, "指标监控服务的端口")


	// udp子命令
	udpCmd       = flag.NewFlagSet("udp", flag.ExitOnError)
	udpAddress   = udpCmd.String("addr", "127.0.0.1:8888", "(必填)UDP服务器地址,格式为: 'ip:port'")
	udpGenNum    = udpCmd.Int("num", 1, "数据生成组件的实例数")
	udpWorker    = udpCmd.Int("worker", 1, "数据输出组件的实例数")
	udpBufCap    = udpCmd.Int("bufCap", 10000, "缓冲池内单个缓冲器的大小")
	udpMaxBufNum = udpCmd.Int("maxBufNum", 5, "缓冲池内缓冲器的最大数量")
	udpMonPort   = udpCmd.Int("monPort", 9999, "指标监控服务的端口")

	// http子命令
	httpCmd       = flag.NewFlagSet("http", flag.ExitOnError)
	httpAddress   = httpCmd.String("addr", "127.0.0.1:8888", "(必填)HTTP服务器地址,格式为: 'ip:port'")
	httpGenNum    = httpCmd.Int("num", 1, "数据生成组件的实例数")
	httpWorker    = httpCmd.Int("worker", 1, "数据输出组件的实例数")
	httpBufCap    = httpCmd.Int("bufCap", 10000, "缓冲池内单个缓冲器的大小")
	httpMaxBufNum = httpCmd.Int("maxBufNum", 5, "缓冲池内缓冲器的最大数量")
	httpMonPort   = httpCmd.Int("monPort", 9999, "指标监控服务的端口")

	// es子命令
	esCmd       = flag.NewFlagSet("es", flag.ExitOnError)
	esAddress   = esCmd.String("addr", "127.0.0.1:9200", "(必填)ES服务器地址,格式为: 'ip:port'")
	esGenNum    = esCmd.Int("num", 1, "数据生成组件的实例数")
	esWorker    = esCmd.Int("worker", 1, "数据输出组件的实例数")
	esBufCap    = esCmd.Int("bufCap", 10000, "缓冲池内单个缓冲器的大小")
	esMaxBufNum = esCmd.Int("maxBufNum", 5, "缓冲池内缓冲器的最大数量")
	esMonPort   = esCmd.Int("monPort", 9999, "指标监控服务的端口")
	esIndexName = esCmd.String("index", "stress-index", "索引名称")
	esShard     = esCmd.Int("shard", 3, "索引分片数")
	esReplica   = esCmd.Int("replica", 0, "索引副本数")
	esFlushByte  = esCmd.Int("flushByte", 1000000, "flush阈值(单位为字节)")

	// kafka子命令
	kafkaCmd       = flag.NewFlagSet("kafka", flag.ExitOnError)
	kafkaAddress   = kafkaCmd.String("addr", "127.0.0.1:9092", "(必填)Kafka服务器地址,格式为: 'ip:port'")
	kafkaTopic     = kafkaCmd.String("topic", "kafka_topic", "(必填)Kafka主题")
	kafkaGenNum    = kafkaCmd.Int("num", 1, "数据生成组件的实例数")
	kafkaWorker    = kafkaCmd.Int("worker", 1, "数据输出组件的实例数")
	kafkaBufCap    = kafkaCmd.Int("bufCap", 10000, "缓冲池内单个缓冲器的大小")
	kafkaMaxBufNum = kafkaCmd.Int("maxBufNum", 5, "缓冲池内缓冲器的最大数量")
	kafkaMonPort   = kafkaCmd.Int("monPort", 9999, "指标监控服务的端口")

	if len(os.Args) < 2 {
		log.Fatal("请输入子命令: 'tcp' or 'udp' or 'http' or 'es' or 'kafka'")
	}
}

func main() {
	var s internal.Sinker
	var err error

	switch os.Args[1] {
	case "tcp":
		tcpCmd.Parse(os.Args[2:])
		// 实例化发送器
		s, err = internal.NewSinkToTCP(*tcpAddress)
		if err != nil {
			log.Fatalf("初始化TCP数据发送客户端失败: %s\n", err.Error())
		}
		// 实例化缓冲池
		pool, err := buffer.NewPool(uint32(*tcpBufCap), uint32(*tcpMaxBufNum))
		if err != nil {
			log.Fatalf("创建缓冲池失败: %s\n", err.Error())
		}
		// 实例化数据生成器
		g, _ := internal.NewGenFromFaker()
		// 实例化服务监控
		m, _ := monitor.NewMonitor(*tcpMonPort)

		// 模块启动
		for i := 0; i < *tcpGenNums; i++ {
			go g.Generate(pool)
		}
		for j := 0; j < *tcpWorkers; j++ {
			go s.Sink(pool)
		}
		m.Start(pool, s)

	case "udp":
		udpCmd.Parse(os.Args[2:])
		s, err = internal.NewSinkToUDP(*udpAddress)
		if err != nil {
			log.Fatalf("初始化UDP数据发送客户端失败: %s\n", err.Error())
		}
		// 实例化缓冲池
		pool, err := buffer.NewPool(uint32(*udpBufCap), uint32(*udpMaxBufNum))
		if err != nil {
			log.Fatalf("创建缓冲池失败: %s\n", err.Error())
		}
		// 实例化数据生成器
		g, _ := internal.NewGenFromFaker()
		// 实例化服务监控
		m, _ := monitor.NewMonitor(*udpMonPort)

		// 模块启动
		for i := 0; i < *udpGenNum; i++ {
			go g.Generate(pool)
		}
		for j := 0; j < *udpWorker; j++ {
			go s.Sink(pool)
		}
		m.Start(pool, s)
	case "http":
		httpCmd.Parse(os.Args[2:])
		s, err = internal.NewSinkToHTTP(*httpAddress)
		if err != nil {
			log.Fatalf("初始化HTTP数据发送客户端失败: %s\n", err.Error())
		}
		// 实例化缓冲池
		pool, err := buffer.NewPool(uint32(*httpBufCap), uint32(*httpMaxBufNum))
		if err != nil {
			log.Fatalf("创建缓冲池失败: %s\n", err.Error())
		}
		// 实例化数据生成器
		g, _ := internal.NewGenFromFaker()
		// 实例化服务监控
		m, _ := monitor.NewMonitor(*httpMonPort)

		// 模块启动
		for i := 0; i < *httpGenNum; i++ {
			go g.Generate(pool)
		}
		for j := 0; j < *httpWorker; j++ {
			go s.Sink(pool)
		}
		m.Start(pool, s)
	case "es":
		esCmd.Parse(os.Args[2:])
		s, err = internal.NewSinkToES(*esAddress, *esIndexName, *esShard, *esReplica, *esWorker, *esFlushByte)
		if err != nil {
			log.Fatalf("初始化ES数据发送客户端失败: %s\n", err.Error())
		}
		// 实例化缓冲池
		pool, err := buffer.NewPool(uint32(*esBufCap), uint32(*esMaxBufNum))
		if err != nil {
			log.Fatalf("创建缓冲池失败: %s\n", err.Error())
		}
		// 实例化数据生成器
		g, _ := internal.NewGenFromFaker()
		// 实例化服务监控
		m, _ := monitor.NewMonitor(*esMonPort)

		// 模块启动
		for i := 0; i < *esGenNum; i++ {
			go g.Generate(pool)
		}
		go s.Sink(pool)
		m.Start(pool, s)
	case "kafka":
		kafkaCmd.Parse(os.Args[2:])
		s, err = internal.NewSinkToKafka(*kafkaAddress, *kafkaTopic)
		if err != nil {
			log.Fatalf("初始化Kafka数据发送客户端失败: %s\n", err.Error())
		}
		// 实例化缓冲池
		pool, err := buffer.NewPool(uint32(*kafkaBufCap), uint32(*kafkaMaxBufNum))
		if err != nil {
			log.Fatalf("创建缓冲池失败: %s\n", err.Error())
		}
		// 实例化数据生成器
		g, _ := internal.NewGenFromFaker()
		// 实例化服务监控
		m, _ := monitor.NewMonitor(*kafkaMonPort)

		// 模块启动
		for i := 0; i < *kafkaGenNum; i++ {
			go g.Generate(pool)
		}
		for j := 0; j < *kafkaWorker; j++ {
			go s.Sink(pool)
		}
		m.Start(pool, s)
	default:
		log.Fatal("请输入子命令: 'tcp' or 'udp' or 'http' or 'es' or 'kafka'")
	}
}
