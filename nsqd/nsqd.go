package nsqd

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"net"
	"os"
	"path"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nsqio/nsq/internal/clusterinfo"
	"github.com/nsqio/nsq/internal/dirlock"
	"github.com/nsqio/nsq/internal/http_api"
	"github.com/nsqio/nsq/internal/protocol"
	"github.com/nsqio/nsq/internal/statsd"
	"github.com/nsqio/nsq/internal/util"
	"github.com/nsqio/nsq/internal/version"
)

const (
	TLSNotRequired = iota
	TLSRequiredExceptHTTP
	TLSRequired
)

type errStore struct {
	err error
}

type Client interface {
	Stats() ClientStats
	IsProducer() bool
}

//上下文对象
type NSQD struct {
	// 64bit atomic vars need to be first for proper alignment on 32bit platforms
	clientIDSequence int64

	sync.RWMutex

	opts atomic.Value

	dl        *dirlock.DirLock
	isLoading int32
	errValue  atomic.Value
	startTime time.Time  //程序开始时间

	topicMap map[string]*Topic

	clientLock sync.RWMutex
	clients    map[int64]Client

	lookupPeers atomic.Value

	tcpServer     *tcpServer
	tcpListener   net.Listener
	httpListener  net.Listener
	httpsListener net.Listener
	tlsConfig     *tls.Config

	poolSize int

	notifyChan           chan interface{}
	optsNotificationChan chan struct{}
	exitChan             chan int
	waitGroup            util.WaitGroupWrapper  //简单封装一个  waitGroup

	ci *clusterinfo.ClusterInfo
}

func New(opts *Options) (*NSQD, error) {
	var err error
	//检测 datapath 是否设置
	dataPath := opts.DataPath
	if opts.DataPath == "" {
		cwd, _ := os.Getwd()
		dataPath = cwd
	}
	//默认  logger 是否设置
	if opts.Logger == nil {
		opts.Logger = log.New(os.Stderr, opts.LogPrefix, log.Ldate|log.Ltime|log.Lmicroseconds)
	}
	//实例化主类 也是结构体指针
	n := &NSQD{
		startTime:            time.Now(),
		topicMap:             make(map[string]*Topic),
		clients:              make(map[int64]Client),
		exitChan:             make(chan int),
		notifyChan:           make(chan interface{}),
		optsNotificationChan: make(chan struct{}, 1),
		dl:                   dirlock.New(dataPath),
	}
	//实例化 http_client 结构体
	httpcli := http_api.NewClient(nil, opts.HTTPClientConnectTimeout, opts.HTTPClientRequestTimeout)
	n.ci = clusterinfo.New(n.logf, httpcli)

	n.lookupPeers.Store([]*lookupPeer{})

	n.swapOpts(opts)
	n.errValue.Store(errStore{})

	err = n.dl.Lock()
	if err != nil {
		return nil, fmt.Errorf("failed to lock data-path: %v", err)
	}

	if opts.MaxDeflateLevel < 1 || opts.MaxDeflateLevel > 9 {
		return nil, errors.New("--max-deflate-level must be [1,9]")
	}

	if opts.ID < 0 || opts.ID >= 1024 {
		return nil, errors.New("--node-id must be [0,1024)")
	}

	if opts.StatsdPrefix != "" {
		var port string
		_, port, err = net.SplitHostPort(opts.HTTPAddress)
		if err != nil {
			return nil, fmt.Errorf("failed to parse HTTP address (%s) - %s", opts.HTTPAddress, err)
		}
		statsdHostKey := statsd.HostKey(net.JoinHostPort(opts.BroadcastAddress, port))
		prefixWithHost := strings.Replace(opts.StatsdPrefix, "%s", statsdHostKey, -1)
		if prefixWithHost[len(prefixWithHost)-1] != '.' {
			prefixWithHost += "."
		}
		opts.StatsdPrefix = prefixWithHost
	}

	if opts.TLSClientAuthPolicy != "" && opts.TLSRequired == TLSNotRequired {
		opts.TLSRequired = TLSRequired
	}

	tlsConfig, err := buildTLSConfig(opts)
	if err != nil {
		return nil, fmt.Errorf("failed to build TLS config - %s", err)
	}
	if tlsConfig == nil && opts.TLSRequired != TLSNotRequired {
		return nil, errors.New("cannot require TLS client connections without TLS key and cert")
	}
	n.tlsConfig = tlsConfig

	for _, v := range opts.E2EProcessingLatencyPercentiles {
		if v <= 0 || v > 1 {
			return nil, fmt.Errorf("invalid E2E processing latency percentile: %v", v)
		}
	}

	n.logf(LOG_INFO, version.String("nsqd"))
	n.logf(LOG_INFO, "ID: %d", opts.ID)

	//监听3个端口
	n.tcpServer = &tcpServer{}
	//监听tcp端口
	n.tcpListener, err = net.Listen("tcp", opts.TCPAddress)
	if err != nil {
		return nil, fmt.Errorf("listen (%s) failed - %s", opts.TCPAddress, err)
	}
	//监听http端口
	n.httpListener, err = net.Listen("tcp", opts.HTTPAddress)
	if err != nil {
		return nil, fmt.Errorf("listen (%s) failed - %s", opts.HTTPAddress, err)
	}

	//如果开了https
	if n.tlsConfig != nil && opts.HTTPSAddress != "" {
		//监听 https
		n.httpsListener, err = tls.Listen("tcp", opts.HTTPSAddress, n.tlsConfig)
		if err != nil {
			return nil, fmt.Errorf("listen (%s) failed - %s", opts.HTTPSAddress, err)
		}
	}
	//端口监听并不会阻塞程序，会直接返回，accept才会阻塞程序
	return n, nil
}

func (n *NSQD) getOpts() *Options {
	return n.opts.Load().(*Options)
}

func (n *NSQD) swapOpts(opts *Options) {
	n.opts.Store(opts)
}

func (n *NSQD) triggerOptsNotification() {
	select {
	case n.optsNotificationChan <- struct{}{}:
	default:
	}
}

func (n *NSQD) RealTCPAddr() *net.TCPAddr {
	return n.tcpListener.Addr().(*net.TCPAddr)
}

func (n *NSQD) RealHTTPAddr() *net.TCPAddr {
	return n.httpListener.Addr().(*net.TCPAddr)
}

func (n *NSQD) RealHTTPSAddr() *net.TCPAddr {
	return n.httpsListener.Addr().(*net.TCPAddr)
}

func (n *NSQD) SetHealth(err error) {
	n.errValue.Store(errStore{err: err})
}

func (n *NSQD) IsHealthy() bool {
	return n.GetError() == nil
}

func (n *NSQD) GetError() error {
	errValue := n.errValue.Load()
	return errValue.(errStore).err
}

func (n *NSQD) GetHealth() string {
	err := n.GetError()
	if err != nil {
		return fmt.Sprintf("NOK - %s", err)
	}
	return "OK"
}

func (n *NSQD) GetStartTime() time.Time {
	return n.startTime
}

func (n *NSQD) AddClient(clientID int64, client Client) {
	n.clientLock.Lock()
	n.clients[clientID] = client
	n.clientLock.Unlock()
}

func (n *NSQD) RemoveClient(clientID int64) {
	n.clientLock.Lock()
	_, ok := n.clients[clientID]
	if !ok {
		n.clientLock.Unlock()
		return
	}
	delete(n.clients, clientID)
	n.clientLock.Unlock()
}
//主流程接口
//在Main函数中，nsqd真正开始运行。Main监听tcp，https（如果设置了相关参数），http端口并通过WaitGroupWrapper的Wrap函数以goroutine方式启动主要的组件。
func (n *NSQD) Main() error {
	//实例化上下文对下，传入 NSQD 对象作为全局变量
	ctx := &context{n}
	//创建 channel error
	exitCh := make(chan error)
	//只执行一次
	var once sync.Once
	//匿名函数
	exitFunc := func(err error) {
		//执行匿名函数
		once.Do(func() {
			//如果 err 不等于 nil 打印错误
			if err != nil {
				n.logf(LOG_FATAL, "%s", err)
			}
			//发送到 exit channel
			exitCh <- err
		})
	}
	//设置上下文对象
	n.tcpServer.ctx = ctx
	//waitGroup  会开个协程   监听tcp连接 一直接受请求 accpet
	n.waitGroup.Wrap(func() {
		exitFunc(protocol.TCPServer(n.tcpListener, n.tcpServer, n.logf))
	})
	//监听http连接
	httpServer := newHTTPServer(ctx, false, n.getOpts().TLSRequired == TLSRequired)
	n.waitGroup.Wrap(func() {
		exitFunc(http_api.Serve(n.httpListener, httpServer, "HTTP", n.logf))
	})
	//如果 https配置不为空 那么监听 https 连接
	if n.tlsConfig != nil && n.getOpts().HTTPSAddress != "" {
		httpsServer := newHTTPServer(ctx, true, true)
		n.waitGroup.Wrap(func() {
			exitFunc(http_api.Serve(n.httpsListener, httpsServer, "HTTPS", n.logf))
		})
	}

	n.waitGroup.Wrap(n.queueScanLoop)
	n.waitGroup.Wrap(n.lookupLoop)
	if n.getOpts().StatsdAddress != "" {
		n.waitGroup.Wrap(n.statsdLoop)
	}


	//阻塞监听 exitCh  有问题直接返回
	err := <-exitCh
	return err
}

type meta struct {
	Topics []struct {
		Name     string `json:"name"`
		Paused   bool   `json:"paused"`
		Channels []struct {
			Name   string `json:"name"`
			Paused bool   `json:"paused"`
		} `json:"channels"`
	} `json:"topics"`
}

func newMetadataFile(opts *Options) string {
	return path.Join(opts.DataPath, "nsqd.dat")
}

func readOrEmpty(fn string) ([]byte, error) {
	data, err := ioutil.ReadFile(fn)
	if err != nil {
		if !os.IsNotExist(err) {
			return nil, fmt.Errorf("failed to read metadata from %s - %s", fn, err)
		}
	}
	return data, nil
}

func writeSyncFile(fn string, data []byte) error {
	f, err := os.OpenFile(fn, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return err
	}

	_, err = f.Write(data)
	if err == nil {
		err = f.Sync()
	}
	f.Close()
	return err
}

func (n *NSQD) LoadMetadata() error {
	//使用atomic包中的方法来保证方法执行前和执行后isLoading值的改变
	atomic.StoreInt32(&n.isLoading, 1)
	defer atomic.StoreInt32(&n.isLoading, 0)
	//得到文件路径 nsqd.dat
	fn := newMetadataFile(n.getOpts())
	//打开文件 读取所有数据
	data, err := readOrEmpty(fn)
	if err != nil {
		return err
	}
	if data == nil {
		return nil // fresh start
	}

	var m meta
	//json反序列化
	err = json.Unmarshal(data, &m)
	if err != nil {
		return fmt.Errorf("failed to parse metadata in %s - %s", fn, err)
	}
	//循环所有topic
	for _, t := range m.Topics {
		//判断是否是合法的topic 不是的话跳过
		if !protocol.IsValidTopicName(t.Name) {
			n.logf(LOG_WARN, "skipping creation of invalid topic %s", t.Name)
			continue
		}
		//使用GetTopic函数通过名字获得topic对象
		topic := n.GetTopic(t.Name)
		//判断当前topic对象是否处于暂停状态，是的话调用Pause函数暂停topic
		if t.Paused {
			topic.Pause()
		}
		//获取当前topic下所有的channel，并且遍历channel，执行的操作与topic基本一致
		for _, c := range t.Channels {
			if !protocol.IsValidChannelName(c.Name) {
				n.logf(LOG_WARN, "skipping creation of invalid channel %s", c.Name)
				continue
			}
			channel := topic.GetChannel(c.Name)
			if c.Paused {
				channel.Pause()
			}
		}
		//topic启动
		topic.Start()
	}
	return nil
}
//PersistMetadata将当前的topic和channel信息写入*nsqd.%d.dat*文件中,
//主要步骤是忽略#ephemeral结尾的topic和channel后将topic和channel列表json序列化后写回文件中
func (n *NSQD) PersistMetadata() error {
	// persist metadata about what topics/channels we have, across restarts
	fileName := newMetadataFile(n.getOpts())

	n.logf(LOG_INFO, "NSQ: persisting topic/channel metadata to %s", fileName)

	js := make(map[string]interface{})
	topics := []interface{}{}
	for _, topic := range n.topicMap {
		if topic.ephemeral {
			continue
		}
		topicData := make(map[string]interface{})
		topicData["name"] = topic.name
		topicData["paused"] = topic.IsPaused()
		channels := []interface{}{}
		topic.Lock()
		for _, channel := range topic.channelMap {
			channel.Lock()
			if channel.ephemeral {
				channel.Unlock()
				continue
			}
			channelData := make(map[string]interface{})
			channelData["name"] = channel.name
			channelData["paused"] = channel.IsPaused()
			channels = append(channels, channelData)
			channel.Unlock()
		}
		topic.Unlock()
		topicData["channels"] = channels
		topics = append(topics, topicData)
	}
	js["version"] = version.Binary
	js["topics"] = topics

	data, err := json.Marshal(&js)
	if err != nil {
		return err
	}

	tmpFileName := fmt.Sprintf("%s.%d.tmp", fileName, rand.Int())
/**
  写入文件时先创建扩展名为tmp的临时文件，
写入内容后并保存后再调用atomicRename函数将tmp文件重命名为*nsqd.%d.dat*。
其中atomicRename函数在windows和其他操作系统下实现方式不同，
分别位于nsqd/rename_windows.go 和rename.go中。
在Linux下直接调用了os.Rename函数，而Windows下则使用Win32 API实现了文件的重命名。
这是因为go的早期版本中Windows下调用os.Rename函数时如果重命名后的文件已经存在则会失败。
这个bug在os: make Rename atomic on Windows中提到， 并且已经在os: windows Rename should overwrite destination file.提交中被修复，
因此，Golang1.5不存在这一bug


*/
	err = writeSyncFile(tmpFileName, data)
	if err != nil {
		return err
	}
	err = os.Rename(tmpFileName, fileName)
	if err != nil {
		return err
	}
	// technically should fsync DataPath here

	return nil
}

func (n *NSQD) Exit() {
	if n.tcpListener != nil {
		n.tcpListener.Close()
	}
	if n.tcpServer != nil {
		n.tcpServer.CloseAll()
	}

	if n.httpListener != nil {
		n.httpListener.Close()
	}

	if n.httpsListener != nil {
		n.httpsListener.Close()
	}

	n.Lock()
	err := n.PersistMetadata()
	if err != nil {
		n.logf(LOG_ERROR, "failed to persist metadata - %s", err)
	}
	n.logf(LOG_INFO, "NSQ: closing topics")
	for _, topic := range n.topicMap {
		topic.Close()
	}
	n.Unlock()

	n.logf(LOG_INFO, "NSQ: stopping subsystems")
	close(n.exitChan)
	n.waitGroup.Wait()
	n.dl.Unlock()
	n.logf(LOG_INFO, "NSQ: bye")
}

// GetTopic performs a thread safe operation
// to return a pointer to a Topic object (potentially new)
//返回topic指针 如果没有就创建
//GetTopic函数用于获取topic对象，首先先尝试从topicMap表中获取，如果指定的topic存在，则直接返回topic对象。
//当topic不存在时需要新建一个topic，加入到topicMap中，
//如果启用了nsqlookupd则需要从lookupd中获取该topic的所有channel，在去除#ephemeral结尾的临时channel后加入到topic中。
func (n *NSQD) GetTopic(topicName string) *Topic {
	// most likely, we already have this topic, so try read lock first.
	//很有可能我们已经有这个主题了 先开个读锁 读取下
	n.RLock()
	t, ok := n.topicMap[topicName]
	n.RUnlock()
	if ok {  //有的话直接返回
		return t
	}
	//到这里还是没有,看来得加个锁 创建了
	n.Lock()
	//锁加上了 再读一次吧，如果有了 直接返回 因为再加锁等待期间 可能这个topic会被创建
	t, ok = n.topicMap[topicName]
	if ok {
		n.Unlock()
		return t
	}
	//删除回调函数
	deleteCallback := func(t *Topic) {
		n.DeleteExistingTopic(t.name)
	}
	//创建一个新的topic 保存 上下文指针
	t = NewTopic(topicName, &context{n}, deleteCallback)
	n.topicMap[topicName] = t //加入到hashMap里面

	n.Unlock() //解锁
	//打log 表示创建成功
	n.logf(LOG_INFO, "TOPIC(%s): created", t.name)
	// topic is created but messagePump not yet started

	// if loading metadata at startup, no lookupd connections yet, topic started after load
	//如果在启动时加载元数据，还没有查找连接，则在加载后启动主题
	//在启动的时候 到这一步就直接返回了
	if atomic.LoadInt32(&n.isLoading) == 1 {
		return t
	}

	//如果用了 lookupd
	// if using lookupd, make a blocking call to get the topics, and immediately create them.
	// this makes sure that any message received is buffered to the right channels
	lookupdHTTPAddrs := n.lookupdHTTPAddrs()
	if len(lookupdHTTPAddrs) > 0 {
		//获取topic的channels
		channelNames, err := n.ci.GetLookupdTopicChannels(t.name, lookupdHTTPAddrs)
		if err != nil {
			n.logf(LOG_WARN, "failed to query nsqlookupd for channels to pre-create for topic %s - %s", t.name, err)
		}
		for _, channelName := range channelNames {
			if strings.HasSuffix(channelName, "#ephemeral") {
				continue // do not create ephemeral channel with no consumer client
			}
			t.GetChannel(channelName)
		}
	} else if len(n.getOpts().NSQLookupdTCPAddresses) > 0 {
		n.logf(LOG_ERROR, "no available nsqlookupd to query for channels to pre-create for topic %s", t.name)
	}

	// now that all channels are added, start topic messagePump
	t.Start()
	return t
}

// GetExistingTopic gets a topic only if it exists
func (n *NSQD) GetExistingTopic(topicName string) (*Topic, error) {
	n.RLock()
	defer n.RUnlock()
	topic, ok := n.topicMap[topicName]
	if !ok {
		return nil, errors.New("topic does not exist")
	}
	return topic, nil
}

// DeleteExistingTopic removes a topic only if it exists
func (n *NSQD) DeleteExistingTopic(topicName string) error {
	n.RLock()
	topic, ok := n.topicMap[topicName]
	if !ok {
		n.RUnlock()
		return errors.New("topic does not exist")
	}
	n.RUnlock()

	// delete empties all channels and the topic itself before closing
	// (so that we dont leave any messages around)
	//
	// we do this before removing the topic from map below (with no lock)
	// so that any incoming writes will error and not create a new topic
	// to enforce ordering
	topic.Delete()

	n.Lock()
	delete(n.topicMap, topicName)
	n.Unlock()

	return nil
}

func (n *NSQD) Notify(v interface{}) {
	// since the in-memory metadata is incomplete,
	// should not persist metadata while loading it.
	// nsqd will call `PersistMetadata` it after loading
	persist := atomic.LoadInt32(&n.isLoading) == 0
	n.waitGroup.Wrap(func() {
		// by selecting on exitChan we guarantee that
		// we do not block exit, see issue #123
		select {
		case <-n.exitChan:
		case n.notifyChan <- v:
			if !persist {
				return
			}
			n.Lock()
			err := n.PersistMetadata()
			if err != nil {
				n.logf(LOG_ERROR, "failed to persist metadata - %s", err)
			}
			n.Unlock()
		}
	})
}

// channels returns a flat slice of all channels in all topics
func (n *NSQD) channels() []*Channel {
	var channels []*Channel
	n.RLock()
	for _, t := range n.topicMap {
		t.RLock()
		for _, c := range t.channelMap {
			channels = append(channels, c)
		}
		t.RUnlock()
	}
	n.RUnlock()
	return channels
}

// resizePool adjusts the size of the pool of queueScanWorker goroutines
//
// 	1 <= pool <= min(num * 0.25, QueueScanWorkerPoolMax)
//
func (n *NSQD) resizePool(num int, workCh chan *Channel, responseCh chan bool, closeCh chan int) {
	idealPoolSize := int(float64(num) * 0.25)
	if idealPoolSize < 1 {
		idealPoolSize = 1
	} else if idealPoolSize > n.getOpts().QueueScanWorkerPoolMax {
		idealPoolSize = n.getOpts().QueueScanWorkerPoolMax
	}
	for {
		if idealPoolSize == n.poolSize {
			break
		} else if idealPoolSize < n.poolSize {
			// contract
			closeCh <- 1
			n.poolSize--
		} else {
			// expand
			n.waitGroup.Wrap(func() {
				n.queueScanWorker(workCh, responseCh, closeCh)
			})
			n.poolSize++
		}
	}
}

// queueScanWorker receives work (in the form of a channel) from queueScanLoop
// and processes the deferred and in-flight queues
func (n *NSQD) queueScanWorker(workCh chan *Channel, responseCh chan bool, closeCh chan int) {
	for {
		select {
		case c := <-workCh:
			now := time.Now().UnixNano()
			dirty := false
			if c.processInFlightQueue(now) {
				dirty = true
			}
			if c.processDeferredQueue(now) {
				dirty = true
			}
			responseCh <- dirty
		case <-closeCh:
			return
		}
	}
}

// queueScanLoop runs in a single goroutine to process in-flight and deferred
// priority queues. It manages a pool of queueScanWorker (configurable max of
// QueueScanWorkerPoolMax (default: 4)) that process channels concurrently.
//
// It copies Redis's probabilistic expiration algorithm: it wakes up every
// QueueScanInterval (default: 100ms) to select a random QueueScanSelectionCount
// (default: 20) channels from a locally cached list (refreshed every
// QueueScanRefreshInterval (default: 5s)).
//
// If either of the queues had work to do the channel is considered "dirty".
//
// If QueueScanDirtyPercent (default: 25%) of the selected channels were dirty,
// the loop continues without sleep.
func (n *NSQD) queueScanLoop() {
	workCh := make(chan *Channel, n.getOpts().QueueScanSelectionCount)
	responseCh := make(chan bool, n.getOpts().QueueScanSelectionCount)
	closeCh := make(chan int)

	workTicker := time.NewTicker(n.getOpts().QueueScanInterval)
	refreshTicker := time.NewTicker(n.getOpts().QueueScanRefreshInterval)

	channels := n.channels()
	n.resizePool(len(channels), workCh, responseCh, closeCh)

	for {
		select {
		case <-workTicker.C:
			if len(channels) == 0 {
				continue
			}
		case <-refreshTicker.C:
			channels = n.channels()
			n.resizePool(len(channels), workCh, responseCh, closeCh)
			continue
		case <-n.exitChan:
			goto exit
		}

		num := n.getOpts().QueueScanSelectionCount
		if num > len(channels) {
			num = len(channels)
		}

	loop:
		for _, i := range util.UniqRands(num, len(channels)) {
			workCh <- channels[i]
		}

		numDirty := 0
		for i := 0; i < num; i++ {
			if <-responseCh {
				numDirty++
			}
		}

		if float64(numDirty)/float64(num) > n.getOpts().QueueScanDirtyPercent {
			goto loop
		}
	}

exit:
	n.logf(LOG_INFO, "QUEUESCAN: closing")
	close(closeCh)
	workTicker.Stop()
	refreshTicker.Stop()
}

func buildTLSConfig(opts *Options) (*tls.Config, error) {
	var tlsConfig *tls.Config

	if opts.TLSCert == "" && opts.TLSKey == "" {
		return nil, nil
	}

	tlsClientAuthPolicy := tls.VerifyClientCertIfGiven

	cert, err := tls.LoadX509KeyPair(opts.TLSCert, opts.TLSKey)
	if err != nil {
		return nil, err
	}
	switch opts.TLSClientAuthPolicy {
	case "require":
		tlsClientAuthPolicy = tls.RequireAnyClientCert
	case "require-verify":
		tlsClientAuthPolicy = tls.RequireAndVerifyClientCert
	default:
		tlsClientAuthPolicy = tls.NoClientCert
	}

	tlsConfig = &tls.Config{
		Certificates: []tls.Certificate{cert},
		ClientAuth:   tlsClientAuthPolicy,
		MinVersion:   opts.TLSMinVersion,
		MaxVersion:   tls.VersionTLS12, // enable TLS_FALLBACK_SCSV prior to Go 1.5: https://go-review.googlesource.com/#/c/1776/
	}

	if opts.TLSRootCAFile != "" {
		tlsCertPool := x509.NewCertPool()
		caCertFile, err := ioutil.ReadFile(opts.TLSRootCAFile)
		if err != nil {
			return nil, err
		}
		if !tlsCertPool.AppendCertsFromPEM(caCertFile) {
			return nil, errors.New("failed to append certificate to pool")
		}
		tlsConfig.ClientCAs = tlsCertPool
	}

	tlsConfig.BuildNameToCertificate()

	return tlsConfig, nil
}

func (n *NSQD) IsAuthEnabled() bool {
	return len(n.getOpts().AuthHTTPAddresses) != 0
}
