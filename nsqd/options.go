package nsqd

import (
	"crypto/md5"
	"crypto/tls"
	"hash/crc32"
	"io"
	"log"
	"os"
	"time"

	"github.com/nsqio/nsq/internal/lg"
)

type Options struct {
	// basic options
	ID        int64       `flag:"node-id" cfg:"id"`
	LogLevel  lg.LogLevel `flag:"log-level"`
	// 日志输出格式的前缀
	LogPrefix string      `flag:"log-prefix"`
	Logger    Logger
	// 监听的tcp服务地址及端口
	TCPAddress               string        `flag:"tcp-address"`
	// 监听的http服务地址及端口
	HTTPAddress              string        `flag:"http-address"`
	HTTPSAddress             string        `flag:"https-address"`
	// 广播地址，默认主机名
	BroadcastAddress         string        `flag:"broadcast-address"`
	NSQLookupdTCPAddresses   []string      `flag:"lookupd-tcp-address" cfg:"nsqlookupd_tcp_addresses"`
	AuthHTTPAddresses        []string      `flag:"auth-http-address" cfg:"auth_http_addresses"`
	HTTPClientConnectTimeout time.Duration `flag:"http-client-connect-timeout" cfg:"http_client_connect_timeout"`
	HTTPClientRequestTimeout time.Duration `flag:"http-client-request-timeout" cfg:"http_client_request_timeout"`

	// diskqueue options
	DataPath        string        `flag:"data-path"`
	MemQueueSize    int64         `flag:"mem-queue-size"`
	MaxBytesPerFile int64         `flag:"max-bytes-per-file"`
	SyncEvery       int64         `flag:"sync-every"`
	SyncTimeout     time.Duration `flag:"sync-timeout"`
/*
   QueueScanWorkerPoolMax就是最大协程数，默认是4，这个数是扫描全部channel的最大协程数，固然channel的数量小于这个参数的话，就调整协程的数量，以最小的为准，好比channel的数量为2个，而默认的是4个，那就调扫描的数量为2个
   QueueScanSelectionCount 每次扫描最大的channel数量，默认是20，若是channel的数量小于这个值，则以channel的数量为准。
   QueueScanDirtyPercent 标识脏数据 channel的百分比，默认为0.25，eg: channel数量为10,则一次最多扫描10个，查看每一个channel是否有过时的数据，若是有，则标记为这个channel是有脏数据的，若是有脏数据的channel的数量 占此次扫描的10个channel的比例超过这个百分比,则直接再次进行扫描一次，而不用等到下一次时间点。
   QueueScanInterval 扫描channel的时间间隔，默认的是每100毫秒扫描一次。
   QueueScanRefreshInterval 刷新扫描的时间间隔 目前的处理方式是调整channel的协程数量。 这也就是nsq处理过时数据的算法，总结一下就是，使用协程定时去扫描随机的channel里是否有过时数据。
 */
	QueueScanInterval        time.Duration
	QueueScanRefreshInterval time.Duration
	QueueScanSelectionCount  int `flag:"queue-scan-selection-count"`
	QueueScanWorkerPoolMax   int `flag:"queue-scan-worker-pool-max"`
	QueueScanDirtyPercent    float64

	// msg and command options
	MsgTimeout    time.Duration `flag:"msg-timeout"`
	MaxMsgTimeout time.Duration `flag:"max-msg-timeout"`
	MaxMsgSize    int64         `flag:"max-msg-size"`
	MaxBodySize   int64         `flag:"max-body-size"`
	MaxReqTimeout time.Duration `flag:"max-req-timeout"`
	ClientTimeout time.Duration

	// client overridable configuration options
	MaxHeartbeatInterval   time.Duration `flag:"max-heartbeat-interval"`
	MaxRdyCount            int64         `flag:"max-rdy-count"`
	MaxOutputBufferSize    int64         `flag:"max-output-buffer-size"`
	MaxOutputBufferTimeout time.Duration `flag:"max-output-buffer-timeout"`
	MinOutputBufferTimeout time.Duration `flag:"min-output-buffer-timeout"`
	OutputBufferTimeout    time.Duration `flag:"output-buffer-timeout"`
	MaxChannelConsumers    int           `flag:"max-channel-consumers"`

	// statsd integration
	StatsdAddress       string        `flag:"statsd-address"`
	StatsdPrefix        string        `flag:"statsd-prefix"`
	StatsdInterval      time.Duration `flag:"statsd-interval"`
	StatsdMemStats      bool          `flag:"statsd-mem-stats"`
	StatsdUDPPacketSize int           `flag:"statsd-udp-packet-size"`

	// e2e message latency
	E2EProcessingLatencyWindowTime  time.Duration `flag:"e2e-processing-latency-window-time"`
	E2EProcessingLatencyPercentiles []float64     `flag:"e2e-processing-latency-percentile" cfg:"e2e_processing_latency_percentiles"`

	// TLS config
	TLSCert             string `flag:"tls-cert"`
	TLSKey              string `flag:"tls-key"`
	TLSClientAuthPolicy string `flag:"tls-client-auth-policy"`
	TLSRootCAFile       string `flag:"tls-root-ca-file"`
	TLSRequired         int    `flag:"tls-required"`
	TLSMinVersion       uint16 `flag:"tls-min-version"`

	// compression
	DeflateEnabled  bool `flag:"deflate"`
	MaxDeflateLevel int  `flag:"max-deflate-level"`
	SnappyEnabled   bool `flag:"snappy"`
}

//创建一个 Options 结构体
func NewOptions() *Options {
	//获取主机名
	hostname, err := os.Hostname()
	if err != nil { //如果报错，直接结束进程
		log.Fatal(err)
	}
	//实例化md5
	h := md5.New()
	//把主机名写入到md5
	io.WriteString(h, hostname)
	//计算出当前机器的id
	defaultID := int64(crc32.ChecksumIEEE(h.Sum(nil)) % 1024)

	//返回 newOptions 结构体指针 （一般我们都用指针比较方便)
	return &Options{
		ID:        defaultID,  //当前机器id
		LogPrefix: "[nsqd] ", //日志前缀
		LogLevel:  lg.INFO,   //日志级别

		TCPAddress:       "0.0.0.0:4150",  //默认tcp监听的端口
		HTTPAddress:      "0.0.0.0:4151",//默认http监听的端口
		HTTPSAddress:     "0.0.0.0:4152",//默认https监听的端口
		BroadcastAddress: hostname,     //广播地址，默认主机名

		NSQLookupdTCPAddresses: make([]string, 0),
		AuthHTTPAddresses:      make([]string, 0),

		HTTPClientConnectTimeout: 2 * time.Second,
		HTTPClientRequestTimeout: 5 * time.Second,

		MemQueueSize:    10000,
		MaxBytesPerFile: 100 * 1024 * 1024,
		SyncEvery:       2500,
		SyncTimeout:     2 * time.Second,   //同步超时时间

		QueueScanInterval:        100 * time.Millisecond,
		QueueScanRefreshInterval: 5 * time.Second,
		QueueScanSelectionCount:  20,
		QueueScanWorkerPoolMax:   4,
		QueueScanDirtyPercent:    0.25,

		MsgTimeout:    60 * time.Second,
		MaxMsgTimeout: 15 * time.Minute,
		MaxMsgSize:    1024 * 1024,
		MaxBodySize:   5 * 1024 * 1024,
		MaxReqTimeout: 1 * time.Hour,
		ClientTimeout: 60 * time.Second,

		MaxHeartbeatInterval:   60 * time.Second,
		MaxRdyCount:            2500,
		MaxOutputBufferSize:    64 * 1024,
		MaxOutputBufferTimeout: 30 * time.Second,
		MinOutputBufferTimeout: 25 * time.Millisecond,
		OutputBufferTimeout:    250 * time.Millisecond,
		MaxChannelConsumers:    0,

		StatsdPrefix:        "nsq.%s",
		StatsdInterval:      60 * time.Second,
		StatsdMemStats:      true,
		StatsdUDPPacketSize: 508,

		E2EProcessingLatencyWindowTime: time.Duration(10 * time.Minute),

		DeflateEnabled:  true,
		MaxDeflateLevel: 6,
		SnappyEnabled:   true,

		TLSMinVersion: tls.VersionTLS10,
	}
}
