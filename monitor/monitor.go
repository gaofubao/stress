package monitor

import (
	"encoding/json"
	"io"
	"log"
	"net/http"
	"stress/buffer"
	"stress/internal"
	"sync/atomic"
	"time"
)

type SinkMetrics struct {
	TotalNum uint64  `json:"TotalNum"` // 发送成功数量
	ErrorNum uint64	 `json:"ErrNum"`   // 发送失败数量
}

type BufferMetrics struct {
	CacheLag uint64	 `json:"CacheLag"` // 缓存堆积数
}

type ServiceMetrics struct {
	RunTime  string  `json:"RunTime"`  // 服务运行时长
	//TotalNum uint64  `json:"TotalNum"` // 发送成功数量
	//ErrorNum uint64	 `json:"ErrNum"`   // 发送失败数量
	TPS      float64 `json:"TPS"`      // 吞吐量
	//CacheLag uint64	 `json:"CacheLag"` // 缓存堆积数
	SinkMetrics
	BufferMetrics
}

type Monitor struct {
	StartTime time.Time      // 服务启动时间
	Info      ServiceMetrics // 服务监控指标
	Queue     []uint64       // 队列，用于存储瞬时指标
}

// 注意：参数s应传递指针类型
func (m *Monitor) Start(pool buffer.Pool, s *internal.SinkToNet) {
	// 统计TPS
	go m.histogram()

	http.HandleFunc("/monitor", func(writer http.ResponseWriter, request *http.Request) {
		// 统计服务运行时长与缓存堆积数
		m.gauge(pool)

		// 统计发送成功的日志数与发送失败的日志数
		m.counter(*s)

		// 序列化监控数据
		result, err := json.MarshalIndent(m.Info, "", "\t")
		if err != nil {
			log.Println("监控数据序列化错误:", err.Error())
		}
		// 返回监控指标
		io.WriteString(writer, string(result))
	})

	http.ListenAndServe(":9999", nil)
}

// 获取Histogram类型指标, 即某些量化指标的平均值
// 需独立的goroutine来执行
func (m *Monitor) histogram() {
	// 每5s计算一次TPS
	ticker := time.NewTicker(time.Second * 5)
	for {
		<-ticker.C
		m.Queue = append(m.Queue, m.Info.TotalNum)
		if len(m.Queue) > 2 {
			m.Queue = m.Queue[1:]
			m.Info.TPS = float64(m.Queue[1] - m.Queue[0]) / 5
		}
	}
}

// 获取Gauge类型指标, 即可任意变化的指标
func (m *Monitor) gauge(pool buffer.Pool)  {
	m.Info.CacheLag = pool.Total()
}

// 获取counter类型指标，即单调递增的指标
func (m *Monitor) counter(s internal.SinkToNet)  {
	m.Info.RunTime = time.Now().Sub(m.StartTime).String()

	m.Info.TotalNum = atomic.LoadUint64(&s.TotalNum)
	m.Info.ErrorNum = atomic.LoadUint64(&s.ErrorNum)
}
