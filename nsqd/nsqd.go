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
	//	// 放在头部保证在 32 位机器上对其操作
	// 64bit atomic vars need to be first for proper alignment on 32bit platforms
	clientIDSequence int64  //递增的客户端ID，每个客户端连接均从这里取一个递增后的ID作为唯一标识

	sync.RWMutex      // 仅在操作 topic 的时候才会使用这个锁,使用读写锁

	opts atomic.Value  // 参数选项，真实类型是apps/nsqd/option.go:Options结构体

	dl        *dirlock.DirLock   // 自己实现的文件夹锁
	isLoading int32				// 标识当前正在从文件中加载元数据
	errValue  atomic.Value    // 错误标识，使用原子 value
	startTime time.Time  //程序开始时间

	topicMap map[string]*Topic  // 保存当前所有的topic

	clientLock sync.RWMutex     // 操作 Clients 时的读写锁
	clients    map[int64]Client   //Client是接口  管理所有Client

	lookupPeers atomic.Value  //用来记录所有 lookupPeer（和 lookupd 的连接）

	tcpServer     *tcpServer   //// 用来管理所有连接
	tcpListener   net.Listener  // 监听 TCP 连接
	httpListener  net.Listener  // 监听 HTTP 连接
	httpsListener net.Listener  // 监听 HTTPS 连接
	tlsConfig     *tls.Config  // 安全协议配置信息

	poolSize int   // // 用来运行 queueScanWorker 的协程数的多少

	notifyChan           chan interface{}   // 通知 lookupd 的管道
	optsNotificationChan chan struct{}    // 用来通知 lookupd IP 地址改变通知
	exitChan             chan int         // 用来通知各个 channel nsqd 退出从而停止循环监听
	waitGroup            util.WaitGroupWrapper // 简单封装了个waitGroup 用来在程序退出之前等待 goroutine 退出

	ci *clusterinfo.ClusterInfo   // 专门用来和 lookupd 通信
}
/**
实例化一个 nsqd 对象
 */
func New(opts *Options) (*NSQD, error) {
	var err error
	//检测 datapath 是否设置
	// 设置数据缓存路径,主要就是 .dat 文件,记录了 topic 和 channel 的信息
	dataPath := opts.DataPath
	//如果为空  就用当前目录
	if opts.DataPath == "" {
		cwd, _ := os.Getwd()
		dataPath = cwd
	}
	//默认  logger 是否设置,如果没设置 用系统的log
	if opts.Logger == nil {
		opts.Logger = log.New(os.Stderr, opts.LogPrefix, log.Ldate|log.Ltime|log.Lmicroseconds)
	}
	//实例化主类 也是结构体指针
	n := &NSQD{
		startTime:            time.Now(),
		topicMap:             make(map[string]*Topic),  // 管理 topic 的 map
		clients:              make(map[int64]Client),   // 管理所有客户端,Client是接口，这里也是指针 现在用的是ClientV2
		exitChan:             make(chan int),             // 退出通知管道
		notifyChan:           make(chan interface{}),     // 通知 lookupd 的管道
		optsNotificationChan: make(chan struct{}, 1),
		dl:                   dirlock.New(dataPath),//初始化目录锁   目录锁，保证只有一个 nsqd 使用该目录
	}
	//实例化 http_client 结构体，简单包了一层http 	 创建一个 HTTP 客户端，用来从 lookupd 中获取 topic 数据
	httpcli := http_api.NewClient(nil, opts.HTTPClientConnectTimeout, opts.HTTPClientRequestTimeout)
	//实例化clusterinfo
	n.ci = clusterinfo.New(n.logf, httpcli)

	n.lookupPeers.Store([]*lookupPeer{})
	// 保存初始化时传入的配置
	n.swapOpts(opts)
	n.errValue.Store(errStore{})

	// 对目录加锁
	err = n.dl.Lock()
	if err != nil {
		return nil, fmt.Errorf("failed to lock data-path: %v", err)
	}

	// 检查配置信息
	if opts.MaxDeflateLevel < 1 || opts.MaxDeflateLevel > 9 {
		return nil, errors.New("--max-deflate-level must be [1,9]")
	}

	if opts.ID < 0 || opts.ID >= 1024 {
		return nil, errors.New("--node-id must be [0,1024)")
	}


	// 先把统计前缀拼出来存到 opts.StatsdPrefix
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
		//拼好之后,在我的机器上是 opts.StatsdPrefix = nsq.XXXdeMacBook-Pro_local_4151.
		opts.StatsdPrefix = prefixWithHost
	}

	// TLS 客户端是否需要验证,默认是 no
	if opts.TLSClientAuthPolicy != "" && opts.TLSRequired == TLSNotRequired {
		opts.TLSRequired = TLSRequired
	}
	// 设置 TLS config
	tlsConfig, err := buildTLSConfig(opts)
	if err != nil {
		return nil, fmt.Errorf("failed to build TLS config - %s", err)
	}
	if tlsConfig == nil && opts.TLSRequired != TLSNotRequired {
		return nil, errors.New("cannot require TLS client connections without TLS key and cert")
	}
	n.tlsConfig = tlsConfig

	// 验证端到端处理延迟的百分比正确
	for _, v := range opts.E2EProcessingLatencyPercentiles {
		if v <= 0 || v > 1 {
			return nil, fmt.Errorf("invalid E2E processing latency percentile: %v", v)
		}
	}

	n.logf(LOG_INFO, version.String("nsqd"))
	n.logf(LOG_INFO, "ID: %d", opts.ID)   // ID 是编码生成的 [0,1023] 之间

	//初始化tcp server
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


/*
   程序启动时调用本方法，执行下面的动作：
       - 启动TCP/HTTP/HTTPS服务
       - 启动工作协程组：NSQD.queueScanLoop
       - 启动服务注册：NSQD.lookupLoop
*/
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
	//waitGroup开个协程 监听tcp连接 一直接受请求 accpet
	n.waitGroup.Wrap(func() {
		exitFunc(protocol.TCPServer(n.tcpListener, n.tcpServer, n.logf))
	})
	//监听http连接 初始化所有的路由 roter
	httpServer := newHTTPServer(ctx, false, n.getOpts().TLSRequired == TLSRequired)
	//开了个协程  监听http
	n.waitGroup.Wrap(func() {
		//调用 http_api.Serve 开始启动 HTTPServer 并在 4151 端口进行 HTTP 通信.
		exitFunc(http_api.Serve(n.httpListener, httpServer, "HTTP", n.logf))
	})
	//如果 https配置不为空 那么监听 https 连接
	if n.tlsConfig != nil && n.getOpts().HTTPSAddress != "" {
		httpsServer := newHTTPServer(ctx, true, true)
		//开了个协程
		n.waitGroup.Wrap(func() {
			exitFunc(http_api.Serve(n.httpsListener, httpsServer, "HTTPS", n.logf))
		})
	}
	//循环监控队列信息
	//queueScanLoop
	//n 个 queueScanWorker
	//开了个协程  负责处理延迟消息
	n.waitGroup.Wrap(n.queueScanLoop)    // 处理消息的优先队列
	//开了个协程  节点信息管理
	n.waitGroup.Wrap(n.lookupLoop)      // 如果 nsqd 发生变化，同步至 nsqloopdup，函数定义在 lookup 中

	if n.getOpts().StatsdAddress != "" {
		//统计信息
		// 定时间 nsqd 的状态通过短链接的方式发送至一个状态监护进程中
		// 包括 nsqd 的应用资源信息，以及 nsqd 上 topic 的信息
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
/**
如果 nsqd 中没有这个 topic 会进行创建,如果是从 nsqd.dat 加载文件时创建 topic 直接返回不执行 Start(),
如果是新的 topic,会尝试从 lookupd 中获取对应的所有 channels,保证 channel 列表的正确性,最后执行 topic.Start()启动 topic.
 */
func (n *NSQD) GetTopic(topicName string) *Topic {
	// most likely, we already have this topic, so try read lock first.
	//当需要获取一个 topic 的时候，先用读锁去读(此时如果有写锁将被阻塞)，若存在则直接返回，若不存在则使用写锁新建一个；
	//另外，使用 atomic.Value 进行结构体某些字段的并发存取值，保证原子性。
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


	// 从下面开始就不再使用 NSQD 的全局锁转而使用 topic 的细粒度锁
	// 如果使用了 lookupd 就需要阻塞获取该 topic 的所有 channel，并立即进行创建
	// 这样可以确保将收到的任何消息缓冲到正确的通道
	//如果用了 lookupd
	// if using lookupd, make a blocking call to get the topics, and immediately create them.
	// this makes sure that any message received is buffered to the right channels
	lookupdHTTPAddrs := n.lookupdHTTPAddrs()
	if len(lookupdHTTPAddrs) > 0 {
		//获取topic的channels
		// 在所有 lookupd 中获取这个 topic 所有的 channels，这一步同样不需要在 LoadMetadata 时执行
		channelNames, err := n.ci.GetLookupdTopicChannels(t.name, lookupdHTTPAddrs)
		if err != nil {
			n.logf(LOG_WARN, "failed to query nsqlookupd for channels to pre-create for topic %s - %s", t.name, err)
		}
		for _, channelName := range channelNames {
			// 临时的就不管了
			if strings.HasSuffix(channelName, "#ephemeral") {
				continue // do not create ephemeral channel with no consumer client
			}
			// 否则需要创建 channel
			t.GetChannel(channelName)
		}
	} else if len(n.getOpts().NSQLookupdTCPAddresses) > 0 {
		// 没有 lookupd 地址但是配置文件中却有。。。
		n.logf(LOG_ERROR, "no available nsqlookupd to query for channels to pre-create for topic %s", t.name)
	}
	// 已经添加了所有 channel 可以开始 topic 下的消息分发
	// 向 startChan 发送通知 messagePump 开始运行
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
		//如果执行那一刻 有exitChan 那么就走exit
		case <-n.exitChan:
			//否则就走正常逻辑 往notifyChan 里发个消息
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
// 	1 <= pool <= min(num * 0.25, 4)
// 修改 pool（运行 queueScanWorker 的协程） 的大小
// 参数: 每轮处理 channel 数量,channel 通道,反馈通道
//修改运行 queueScanWorker() 函数的 goroutine 个数,真正的 channel 扫描发生在 queueScanWorker() 中,也是通过单独的 goroutine 进行运行的.
//根据配置  增加或者减少对应的 work协程
func (n *NSQD) resizePool(num int, workCh chan *Channel, responseCh chan bool, closeCh chan int) {
	// num 是一次需要处理的 channel 个数
	idealPoolSize := int(float64(num) * 0.25)
	// idealPoolSize 范围 [1,4]
	if idealPoolSize < 1 {
		idealPoolSize = 1
	} else if idealPoolSize > n.getOpts().QueueScanWorkerPoolMax { //默认4个
		idealPoolSize = n.getOpts().QueueScanWorkerPoolMax
	}
	for {
		//最终判断poolsize 是否和现有的相等， 不用改变 Pool 的大小
		if idealPoolSize == n.poolSize {
			break
		} else if idealPoolSize < n.poolSize { // 关闭 queueScanWorker 协程直到剩余数量等于 idealPoolSize
			// contract
			closeCh <- 1   // 一次只关闭一个
			n.poolSize--
		} else {
			// expand
			//开个协程 添加 queueScanWorker 协程，直到数量等于 idealPoolSize
			n.waitGroup.Wrap(func() {
				n.queueScanWorker(workCh, responseCh, closeCh)
			})
			n.poolSize++
		}
	}
}

// queueScanWorker receives work (in the form of a channel) from queueScanLoop
// and processes the deferred and in-flight queues
//queueScanWorker从queueScanLoop接收工作（以通道的形式）

//并处理延迟和飞行中的队列
// 扫描 channel 的 deferredPQ 和 inFlightPQ  两个队列处理其中的消息
//queueScanWorker 从 workCh 中获取一个 channel,然后对它进行扫描,并处理其中的新数据,并通知给 queueScanLoop 做触发统计
func (n *NSQD) queueScanWorker(workCh chan *Channel, responseCh chan bool, closeCh chan int) {
	for {
		select {
		// 接收工作 channel
		case c := <-workCh:
			now := time.Now().UnixNano()
			dirty := false
			// 下面处理 channel 中 deferredPQ 和 inFlightPQ 两个消息优先队列
			if c.processInFlightQueue(now) {
				dirty = true
			}
			//处理延迟消息
			if c.processDeferredQueue(now) {
				dirty = true
			}
			// 把 dirty 标志传回去用于统计
			responseCh <- dirty
		case <-closeCh:
			// 关闭这个 queueScanWorker
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
//管道扫进程，他的逻辑是将tpic，channel中的数据读入到worker channel, 并每隔一定的时间更新worker数量，扫描chanel中的数据。

/**
以上函数的 loop 部分就是主要的 channel 扫描部分,在这里先使用 util.UniqRands 确定每次扫描的 channel.
多次运行 UniqRands(10,20) 结果如下:
[10 12 11 6 5 0 4 15 16 7]
[5 3 18 4 16 13 11 14 6 10]
[17 10 13 16 2 5 6 0 12 15]
可以看到就是在 20 个中选 10 个并随机排序而已.
找出需要扫描的 channel 之后判断这些 channel 中是否有数据需要处理,
默认的情况下,如果选中的 channel 中有 1/4 的channel 存在数据就不需要等待 100ms 的触发扫描,
而是直接跳转到 loop 继续进行扫描,这是一个优化

channel 有有两个重要队列： defer队列和inflight 队列, 事件处理主要是对两个队列的消息数据做处理

扫描channel 规则
更新 channels 的频率为100ms
刷新表的频率为 5s
默认随机选择20( queue-scan-selection-count ) 个channels 做消息队列调整
默认处理队列的协程数量不超过 4 ( queue-scan-worker-pool-max )
processInFlightQueue 做消息处理超时重发处理
flight 队列中，保存的是推送到消费端的消息，优先队列中，按照time排序, 消息已经发送的时间越久越靠前
定时从flight 队列中获取最久的消息，如果已超时( 超过 msg--time )，则将消息重新发送
processDeferdQueue 处理延迟队列的消息
deferd 队列中，保存的是延迟推送的消息，优先队列中，按照time排序，距离消息要发送的时间越短，越靠前
定时从deferd 队列中获取最近需要发送的消息，如果消息已达到发送时间，则pop 消息，将消息发送
 */
func (n *NSQD) queueScanLoop() {
	//实例化Channel的 channel   20个缓冲区
	workCh := make(chan *Channel, n.getOpts().QueueScanSelectionCount)
	//实例化 bool的channel   20个缓冲区
	responseCh := make(chan bool, n.getOpts().QueueScanSelectionCount)
	closeCh := make(chan int)

	// 默认 100ms 唤醒一次
	workTicker := time.NewTicker(n.getOpts().QueueScanInterval)
	// 5s 刷新一次 channel
	refreshTicker := time.NewTicker(n.getOpts().QueueScanRefreshInterval)
	// 获取所有 topic 中的所有 channel
	channels := n.channels()
	// 重新调整 pool 大小
	n.resizePool(len(channels), workCh, responseCh, closeCh)

	for {
		select {
		case <-workTicker.C: // 100 ms 强制扫描 channel
			if len(channels) == 0 {
				continue
			}
		case <-refreshTicker.C: // 每 5s 刷新 channel 并且更改 pool 的大小
			channels = n.channels()
			n.resizePool(len(channels), workCh, responseCh, closeCh)
			continue
		case <-n.exitChan:
			// 退出 goroutine
			goto exit
		}

		// 默认一轮最多扫描 20 个 channel
		num := n.getOpts().QueueScanSelectionCount
		if num > len(channels) {
			num = len(channels)
		}

	loop:
		// 随机从一个位置开始循环 channels
		for _, i := range util.UniqRands(num, len(channels)) {
			workCh <- channels[i]    // 把 channel 交给 work pool 处理
		}
		//每隔刷新间隔判断worker数量是否发生变化。
		// 判断 dirty 的个数

		numDirty := 0
		for i := 0; i < num; i++ {
			if <-responseCh {
				numDirty++
			}
		}

		// 大于 1/4 的话不进行睡眠，继续运行，因为这时大量的 channel 需要处理
		// TODO 这可能导致长时间 channels 得不到更新，因为如果一直处于忙碌状态的话
		// 不会执行 select 即使 channel 发生了变化也不会更新
		//脏的 除以 干净的      如果大于    0.25
		//queueScanLoop的处理方法模仿了Redis的概率到期算法
		//(probabilistic expiration algorithm)，
		//每过一个QueueScanInterval(默认100ms)间隔，进行一次概率选择，
		//从所有的channel缓存中随机选择QueueScanSelectionCount(默认20)个channel，
		//如果某个被选中channel存在InFlighting消息或者Deferred消息，
		//则认为该channel为“脏”channel。
		//如果被选中channel中“脏”channel的比例大于QueueScanDirtyPercent(默认25%)，
		//则不投入睡眠，直接进行下一次概率选择
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
