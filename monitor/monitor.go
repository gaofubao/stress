package monitor

import (
	"encoding/json"
	"io"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/gaofubao/stress/v1.0.0/buffer"
	"github.com/gaofubao/stress/v1.0.0/internal"
)

type serviceMetrics struct {
	RunTime  string `json:"RunTime"`    // 服务运行时长
	TotalNum uint64  `json:"TotalNum"` // 发送成功数量
	ErrorNum uint64	 `json:"ErrNum"`   // 发送失败数量
	TPS 	 float64 `json:"TPS"`      // 吞吐量
	CacheLag uint64	 `json:"CacheLag"` // 缓存堆积数
}

type Monitor interface {
	Start(p buffer.Pool, s internal.Sinker)
}

type myMonitor struct {
	startTime time.Time      // 服务启动时间
	metrics   serviceMetrics // 服务监控指标
	queue     []uint64       // 队列，用于存储瞬时指标
	port	  int			 // 查看指标的端口
}

func NewMonitor(port int) (Monitor, error) {
	return &myMonitor{
		startTime: time.Now(),
		metrics:   serviceMetrics{},
		queue:     make([]uint64, 2),
		port: port,
	}, nil
}

// 注意：参数s应传递指针类型
func (m *myMonitor) Start(p buffer.Pool, s internal.Sinker) {
	// 统计TPS
	go m.histogram()

	http.HandleFunc("/metrics", func(writer http.ResponseWriter, request *http.Request) {
		// 统计服务运行时长与缓存堆积数
		m.gauge(p)

		// 统计发送成功的日志数与发送失败的日志数
		m.counter(s)

		// 序列化监控数据
		result, err := json.MarshalIndent(m.metrics, "", "\t")
		if err != nil {
			log.Println("监控数据序列化错误:", err.Error())
		}
		// 返回监控指标
		io.WriteString(writer, string(result))
	})

	http.ListenAndServe(":" + strconv.Itoa(m.port), nil)
}

// 获取Histogram类型指标, 即某些量化指标的平均值
// 需独立的goroutine来执行
func (m *myMonitor) histogram() {
	// 每5s计算一次TPS
	ticker := time.NewTicker(time.Second * 5)
	for {
		<-ticker.C
		m.queue = append(m.queue, m.metrics.TotalNum)
		if len(m.queue) > 2 {
			m.queue = m.queue[1:]
			m.metrics.TPS = float64(m.queue[1]-m.queue[0]) / 5
		}
	}
}

// 获取Gauge类型指标, 即可任意变化的指标
func (m *myMonitor) gauge(pool buffer.Pool) {
	m.metrics.CacheLag = pool.Total()
}

// 获取counter类型指标，即单调递增的指标
func (m *myMonitor) counter(s internal.Sinker) {
	m.metrics.RunTime = time.Now().Sub(m.startTime).String()

	m.metrics.TotalNum = s.TotalNum()
	m.metrics.ErrorNum = s.ErrorNum()
}
