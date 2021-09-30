package nsqd

import (
	"bytes"
	"container/heap"
	"errors"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nsqio/go-diskqueue"
	"github.com/nsqio/nsq/internal/lg"
	"github.com/nsqio/nsq/internal/pqueue"
	"github.com/nsqio/nsq/internal/quantile"
)


/**
Channel 是消费者订阅特定 Topic 的一种抽象.对于发往 Topic 的消息,
nsqd 向该 Topic 下的所有 Channel 投递消息,而同一个 Channel 只投递一次,
Channel 下如果存在多个消费者,则随机选择一个消费者做投递.这种投递方式可以被用作消费者负载均衡.

Channel 从属于特定 Topic,可以认为是 Topic 的下一级.
在同一个 Topic 之下可以有零个或多个 Channel.
和 Topic 一样,Channel 同样有永久和临时之分,永久的 Channel 只能通过显式删除销毁,临时的Channel 在最后一个消费者断开连接的时候被销毁.

与服务于生产者的 Topic 不同,Channel 直接面向消费者

channel 也是通过 put 的操作将消息放到消息缓冲通道中,现在已经知道 channel 中的消息是 topic put 进来的,这里有两种类型的消息:

正常的消息直接添加到消息队列缓冲区或者通过 backend 落地
延时消息会通过 PutMessageDeferred -> StartDeferredTimeout -> pushDeferredMessage 添加到 channel 的延时队列中
向延迟队列的添加需要两步,分别将 msg 加入到 c.deferredMessages 中和 c.deferredPQ 中即可

channel 接收到正常消息会直接存放到消息缓冲区中,是否还记得在 nsqd 中我们启动了 queueScanLoop 来对所有的 channel 进行扫描,
判断是否有消息需要进行传递:
在 queueScanWorker 中会对每个待扫描的 channel 执行 processInFlightQueue, processDeferredQueue 两个函数.
分别处理正常消息和延迟消息

*/
//nsqd/channel.go文件，对应于每个channel实例
type Consumer interface {
	UnPause()
	Pause()
	Close() error
	TimedOutMessage()
	Stats() ClientStats
	Empty()
}

// Channel represents the concrete type for a NSQ channel (and also
// implements the Queue interface)
//
// There can be multiple channels per topic, each with there own unique set
// of subscribers (clients).
//
// Channels maintain all client and message metadata, orchestrating in-flight
// messages, timeouts, requeuing, etc.
type Channel struct {
	//	 放在前面保证 32 位系统对其
	// 64bit atomic vars need to be first for proper alignment on 32bit platforms

	requeueCount uint64             // 重新消费的message 个数
	messageCount uint64             // 消息总数
	timeoutCount uint64             // 消费超时的message 个数

	sync.RWMutex

	topicName string  // topic name
	name      string  // channel name
	ctx       *context  // 上下文

	backend BackendQueue	// 磁盘队列，当内存memoryMsgChan满时，写入硬盘队列

	memoryMsgChan chan *Message // 消息优先存入这个内存chan
	exitFlag      int32// 是否退出
	exitMutex     sync.RWMutex// 用来对 exitFlag 加锁

	// state tracking
	clients        map[int64]Consumer// 所有客户端
	paused         int32// 是否暂停
	ephemeral      bool// 是否是临时 channel
	deleteCallback func(*Channel)// 删除回调函数
	deleter        sync.Once// 保证删除回调函数仅执行一次

	// Stats tracking
	e2eProcessingLatencyStream *quantile.Quantile

	// TODO: these can be DRYd up	// 延迟队列相关的三个字段
	deferredMessages map[MessageID]*pqueue.Item   // 保存尚未到时间的延迟消费消息  defer 消息保存的map
	deferredPQ       pqueue.PriorityQueue        // 保存尚未到时间的延迟消费消息，最小堆 defer 队列 (优先队列保存)
	deferredMutex    sync.Mutex  //相关的互斥锁
	// 待确认队列相关的三个字段

	//每个 channel 维持两个队列,一个发送待确认队列,
	//一个延迟发送队列.
	//二者均使用优先队列进行实现,且存储消息使用的 map 以 msgID 作为 key.二者的实现也不一样,后面慢慢讲
	inFlightMessages map[MessageID]*Message    // 保存已推送尚未收到FIN的消息 正在消费的消息保存的map

	//inFlightPQ 的 pri也是时间戳，是投递消息的超时时间）
	//当消息发送出去之后，会进入in_flight_queue 队列
	inFlightPQ       inFlightPqueue             // 保存已推送尚未收到FIN的消息，最小堆 正在消费的消息保存在优先队列 (优先队列保存)


	inFlightMutex    sync.Mutex     //相关的互斥锁
}
/**
在 channel 中存在两个优先队列,一个待确认队列,一个延迟队列.每个队列均由一个 Mutex,
一个优先队列,一个 map 组成.其中 Mutex 保护 map 的读写,优先队列进行排序,
排队的 key 分别是待确认过期时间以及延迟发送时间,map 则用来根据优先队列获取的节点来快速获取 map 信息.


*/
// NewChannel creates a new instance of the Channel type and returns a pointer
// 创建一个 channel 并返回对应指针
//整个看下来,channel 好像和 topic 创建十分类似,区别仅仅就只有 E2EProcessingLatencyPercentiles 参数的处理和 c.initPQ(),前者暂时不看,先看看后者:
func NewChannel(topicName string, channelName string, ctx *context,
	deleteCallback func(*Channel)) *Channel {

	c := &Channel{
		topicName:      topicName,
		name:           channelName,
		memoryMsgChan:  nil,
		clients:        make(map[int64]Consumer),
		deleteCallback: deleteCallback,
		ctx:            ctx,
	}
	// create mem-queue only if size > 0 (do not use unbuffered chan)
	// 消息队列,默认 10000
	if ctx.nsqd.getOpts().MemQueueSize > 0 {
		c.memoryMsgChan = make(chan *Message, ctx.nsqd.getOpts().MemQueueSize)
	}
	// 初始化e2eProcessingLatencyStream值，用于统计延迟
	// E2EProcessingLatencyPercentiles 消息处理时间的百分比（通过逗号可以多次指定，默认为 none）
	if len(ctx.nsqd.getOpts().E2EProcessingLatencyPercentiles) > 0 {
		c.e2eProcessingLatencyStream = quantile.New(
			ctx.nsqd.getOpts().E2EProcessingLatencyWindowTime,
			ctx.nsqd.getOpts().E2EProcessingLatencyPercentiles,
		)
	}
	// 初始化延迟队列和待确认队列
	c.initPQ()
	// 判断是不是暂时 channel,选择消息存盘方式
	if strings.HasSuffix(channelName, "#ephemeral") {
		c.ephemeral = true
		c.backend = newDummyBackendQueue()
	} else {
		dqLogf := func(level diskqueue.LogLevel, f string, args ...interface{}) {
			opts := ctx.nsqd.getOpts()
			lg.Logf(opts.Logger, opts.LogLevel, lg.LogLevel(level), f, args...)
		}
		// backend names, for uniqueness, automatically include the topic...
		backendName := getBackendName(topicName, channelName)
		// 调用diskqueue，持久化到本地磁盘
		c.backend = diskqueue.New(
			backendName,
			ctx.nsqd.getOpts().DataPath,
			ctx.nsqd.getOpts().MaxBytesPerFile,
			int32(minValidMsgLength),
			int32(ctx.nsqd.getOpts().MaxMsgSize)+minValidMsgLength,
			ctx.nsqd.getOpts().SyncEvery,
			ctx.nsqd.getOpts().SyncTimeout,
			dqLogf,
		)
	}
	// 通知 lookupd 创建了 channel
	c.ctx.nsqd.Notify(c)

	return c
}
// 初始化两个队列,均使用优先队列
/**
initPQ函数创建了两个字典inFlightMessages、deferredMessages和两个队列inFlightPQ、deferredPQ。 在nsq中inFlight指的是正在投递但还没确认投递成功的消息，defferred指的是投递失败，等待重新投递的消息。 initPQ创建的字典和队列主要用于索引和存放这两类消息。其中两个字典使用消息ID作索引。
inFlightPQ使用newInFlightPqueue初始化，InFlightPqueue位于nsqd\in_flight_pqueue.go。 nsqd\in_flight_pqueue.go是nsq实现的一个优先级队列，提供了常用的队列操作，值得学习。
deferredPQ使用pqueue.New初始化，pqueue位于nsqd\pqueue.go，也是一个优先级队列。

 */
func (c *Channel) initPQ() {
	pqSize := int(math.Max(1, float64(c.ctx.nsqd.getOpts().MemQueueSize)/10))

	c.inFlightMutex.Lock()
	// 代表正在投递但是还没有投递成功的消息
	c.inFlightMessages = make(map[MessageID]*Message)
	c.inFlightPQ = newInFlightPqueue(pqSize)
	c.inFlightMutex.Unlock()

	c.deferredMutex.Lock()
	// 延迟消息和投递失败的消息
	c.deferredMessages = make(map[MessageID]*pqueue.Item)
	c.deferredPQ = pqueue.New(pqSize)
	c.deferredMutex.Unlock()
}

// Exiting returns a boolean indicating if this channel is closed/exiting
func (c *Channel) Exiting() bool {
	return atomic.LoadInt32(&c.exitFlag) == 1
}
// 删除 chanel
// Delete empties the channel and closes
func (c *Channel) Delete() error {
	return c.exit(true)
}
// 关闭 channel
// Close cleanly closes the Channel
func (c *Channel) Close() error {
	return c.exit(false)
}
// 删除或关闭 channel
func (c *Channel) exit(deleted bool) error {
	c.exitMutex.Lock()
	defer c.exitMutex.Unlock()
	// 设置不可用状态
	if !atomic.CompareAndSwapInt32(&c.exitFlag, 0, 1) {
		return errors.New("exiting")
	}

	if deleted {
		c.ctx.nsqd.logf(LOG_INFO, "CHANNEL(%s): deleting", c.name)

		// 通知 lookdup
		// since we are explicitly deleting a channel (not just at system exit time)
		// de-register this from the lookupd
		c.ctx.nsqd.Notify(c)
	} else {
		c.ctx.nsqd.logf(LOG_INFO, "CHANNEL(%s): closing", c.name)
	}

	// this forceably closes client connections
	c.RLock()
	// 所有关联客户端均关闭
	for _, client := range c.clients {
		client.Close()
	}
	c.RUnlock()

	if deleted {
		// 删除的话需要删除所有未发送消息
		// empty the queue (deletes the backend files, too)
		c.Empty()
		return c.backend.Delete()
	}
	// 仅关闭的话,将所有消息放入磁盘中
	// write anything leftover to disk
	c.flush()
	return c.backend.Close()
}
// 清空所有队列中的所有消息
func (c *Channel) Empty() error {
	c.Lock()
	defer c.Unlock()
	// 重置两个队列
	c.initPQ()
	// 清空所有客户端
	for _, client := range c.clients {
		client.Empty()
	}
	// 清空所有未接收消息
	for {
		select {
		case <-c.memoryMsgChan:
		default:
			goto finish
		}
	}
finish:
	return c.backend.Empty()	// 最后把磁盘中的消息清空

}
// 将消息全部落盘
// flush persists all the messages in internal memory buffers to the backend
// it does not drain inflight/deferred because it is only called in Close()
func (c *Channel) flush() error {
	var msgBuf bytes.Buffer

	if len(c.memoryMsgChan) > 0 || len(c.inFlightMessages) > 0 || len(c.deferredMessages) > 0 {
		c.ctx.nsqd.logf(LOG_INFO, "CHANNEL(%s): flushing %d memory %d in-flight %d deferred messages to backend",
			c.name, len(c.memoryMsgChan), len(c.inFlightMessages), len(c.deferredMessages))
	}
	// 队列中的消息落盘
	for {
		select {
		case msg := <-c.memoryMsgChan:
			err := writeMessageToBackend(&msgBuf, msg, c.backend)
			if err != nil {
				c.ctx.nsqd.logf(LOG_ERROR, "failed to write message to backend - %s", err)
			}
		default:
			goto finish
		}
	}

finish:
	c.inFlightMutex.Lock()
	// 存盘待确认的消息
	for _, msg := range c.inFlightMessages {
		err := writeMessageToBackend(&msgBuf, msg, c.backend)
		if err != nil {
			c.ctx.nsqd.logf(LOG_ERROR, "failed to write message to backend - %s", err)
		}
	}
	c.inFlightMutex.Unlock()

	c.deferredMutex.Lock()
	// 存盘延时消息
	for _, item := range c.deferredMessages {
		msg := item.Value.(*Message)
		err := writeMessageToBackend(&msgBuf, msg, c.backend)
		if err != nil {
			c.ctx.nsqd.logf(LOG_ERROR, "failed to write message to backend - %s", err)
		}
	}
	c.deferredMutex.Unlock()

	return nil
}

func (c *Channel) Depth() int64 {
	return int64(len(c.memoryMsgChan)) + c.backend.Depth()
}

func (c *Channel) Pause() error {
	return c.doPause(true)
}

func (c *Channel) UnPause() error {
	return c.doPause(false)
}

func (c *Channel) doPause(pause bool) error {
	if pause {
		atomic.StoreInt32(&c.paused, 1)
	} else {
		atomic.StoreInt32(&c.paused, 0)
	}

	c.RLock()
	for _, client := range c.clients {
		if pause {
			client.Pause()
		} else {
			client.UnPause()
		}
	}
	c.RUnlock()
	return nil
}

func (c *Channel) IsPaused() bool {
	return atomic.LoadInt32(&c.paused) == 1
}

// PutMessage writes a Message to the queue
//    将消息写入channel，逻辑与topic的一致，内存未满则优先写内存chan，否则写入磁盘队列
//做并发控制（加锁）后，调Topic.put(*Message) 来写入信息。
func (c *Channel) PutMessage(m *Message) error {
	c.RLock()
	defer c.RUnlock()
	if c.Exiting() {	// channel 是否可用
		return errors.New("exiting")
	}
	// 调用 channel.put(msg)
	err := c.put(m)
	if err != nil {
		return err
	}
	// 记录 msgCount
	atomic.AddUint64(&c.messageCount, 1)
	return nil
}
//实际写入
/**
put函数的操作是，将Message写入channel，如果该topic的memoryMsgChan长度满了，则通过default逻辑，写入buffer中.
buffer的实现是利用了sync.Pool包，相当于是一块缓存，在gc前释放，存储的长度受限于内存大小。

这里有两个问题：

存入memoryMsgChan就算完成topic写入了吗
buffer中的数据怎么办

经过查找，发现处理上述两个channel的函数是messagePump，而messagePump在创建一个新Topic时会被后台调用：
func NewTopic(topicName string, ctx *context, deleteCallback func(*Topic)) *Topic {
    ...
    t.waitGroup.Wrap(func() {t.messagePump()})
    ...
}

func (t *Topic) messagePump() {
    ...
    if len(chans) > 0 {
        memoryMsgChan = t.memoryMsgChan
        backendChan = t.backend.ReadChan()
    }
    select {
        case msg = <-memoryMsgChan:
        case buf = <-backendChan:
            msg, err = decodeMessage(buf)
            if err != nil {
                t.ctx.nsqd.logf(LOG_ERROR, "failed to decode message - %s", err)
                continue
            }
        ...
    }
    ...
    for i, channel := range chans {
        chanMsg := msg
        if i > 0 {
                chanMsg = NewMessage(msg.ID, msg.Body)
                chanMsg.Timestamp = msg.Timestamp
                chanMsg.deferred = msg.deferred
            }
        ...
        err := channel.PutMessage(chanMsg)
        ...
    }
    ...
}

上述调用了channel的PutMessage()完成了message写入channel的memoryMsgChan中，写入逻辑和写入topic逻辑类似。到这里完成了数据的写入流程分析。

// 将消息推送出去

*/
func (c *Channel) put(m *Message) error {
	select {
	// 存入 channel 的消息队列中
	case c.memoryMsgChan <- m:
	default:
		// 消息队列满了就存入 backend 队列中
		b := bufferPoolGet()  //先拿到一个 bytes.buffer
		// 存入磁盘
		err := writeMessageToBackend(b, m, c.backend)
		bufferPoolPut(b)
		c.ctx.nsqd.SetHealth(err)
		if err != nil {
			c.ctx.nsqd.logf(LOG_ERROR, "CHANNEL(%s): failed to write message to backend - %s",
				c.name, err)
			return err
		}
	}
	return nil
}
// 准备发送延迟消息,先添加到延时队列,时间到再 put
func (c *Channel) PutMessageDeferred(msg *Message, timeout time.Duration) {
	atomic.AddUint64(&c.messageCount, 1)
	c.StartDeferredTimeout(msg, timeout)
}

//TOUCH MessageID -> channel.TouchMessage : 重置待确认队列中的一条超时消息
// TouchMessage resets the timeout for an in-flight message
func (c *Channel) TouchMessage(clientID int64, id MessageID, clientMsgTimeout time.Duration) error {
	// 从待确认队列移除
	msg, err := c.popInFlightMessage(clientID, id)
	if err != nil {
		return err
	}
	c.removeFromInFlightPQ(msg)
	// 更新新的待确认超时时间
	// 最长确认时间 15min
	newTimeout := time.Now().Add(clientMsgTimeout)
	if newTimeout.Sub(msg.deliveryTS) >=
		c.ctx.nsqd.getOpts().MaxMsgTimeout {
		// we would have gone over, set to the max
		newTimeout = msg.deliveryTS.Add(c.ctx.nsqd.getOpts().MaxMsgTimeout)
	}

	msg.pri = newTimeout.UnixNano()
	// 重新加入待确认队列,仅仅修改了待确认超时时间
	err = c.pushInFlightMessage(msg)
	if err != nil {
		return err
	}
	c.addToInFlightPQ(msg)
	return nil
}

// FinishMessage successfully discards an in-flight message
func (c *Channel) FinishMessage(clientID int64, id MessageID) error {
	msg, err := c.popInFlightMessage(clientID, id)
	if err != nil {
		return err
	}
	c.removeFromInFlightPQ(msg)
	if c.e2eProcessingLatencyStream != nil {
		c.e2eProcessingLatencyStream.Insert(msg.Timestamp)
	}
	return nil
}

// RequeueMessage requeues a message based on `time.Duration`, ie:
//
// `timeoutMs` == 0 - requeue a message immediately
// `timeoutMs`  > 0 - asynchronously wait for the specified timeout
//     and requeue a message (aka "deferred requeue")
//
// `timeoutMs` == 0 - requeue a message immediately
// `timeoutMs`  > 0 - asynchronously wait for the specified timeout
//     and requeue a message (aka "deferred requeue")
// 重置一条消息,并判断时候需要延迟发送,以投递消息,但是客户端请求重新投递(它不想处理...)
// timeout == 0 立即重新发送该消息
// timeout > 0  延迟消息
func (c *Channel) RequeueMessage(clientID int64, id MessageID, timeout time.Duration) error {
	// remove from inflight first
	// 先从待确认消息队列移除
	msg, err := c.popInFlightMessage(clientID, id)
	if err != nil {
		return err
	}
	c.removeFromInFlightPQ(msg)
	// 投递消息数加一
	atomic.AddUint64(&c.requeueCount, 1)
	// 立即投递
	if timeout == 0 {
		c.exitMutex.RLock()
		if c.Exiting() {
			c.exitMutex.RUnlock()
			return errors.New("exiting")
		}
		// 正常推送消息
		err := c.put(msg)
		c.exitMutex.RUnlock()
		return err
	}

	// deferred requeue
	// 添加到延迟推送队列
	return c.StartDeferredTimeout(msg, timeout)
}

// AddClient adds a client to the Channel's client list
// 增加一个客户端
func (c *Channel) AddClient(clientID int64, client Consumer) error {
	c.Lock()
	defer c.Unlock()

	_, ok := c.clients[clientID]
	if ok {
		//已存在
		return nil
	}
	// 客户端过多的话报错,默认不设限制
	maxChannelConsumers := c.ctx.nsqd.getOpts().MaxChannelConsumers
	if maxChannelConsumers != 0 && len(c.clients) >= maxChannelConsumers {
		return errors.New("E_TOO_MANY_CHANNEL_CONSUMERS")
	}

	// 添加到 map 中
	c.clients[clientID] = client
	return nil
}

// 删除一个客户端
// RemoveClient removes a client from the Channel's client list
func (c *Channel) RemoveClient(clientID int64) {
	c.Lock()
	defer c.Unlock()

	_, ok := c.clients[clientID]
	if !ok {
		return
	}
	delete(c.clients, clientID)
	// 删除 channel
	if len(c.clients) == 0 && c.ephemeral == true {
		go c.deleter.Do(func() { c.deleteCallback(c) })
	}
}
// 推送一条消息给客户端,等待确认,timeout 是确认超时时间
//在发送给客户端之前，先把消息加入到 InFlight里面
func (c *Channel) StartInFlightTimeout(msg *Message, clientID int64, timeout time.Duration) error {
	now := time.Now()
	msg.clientID = clientID
	// 消息发送时间
	msg.deliveryTS = now
	// 等待确认超时时间,在优先队列中按照它进行排序的
	msg.pri = now.Add(timeout).UnixNano()
	// 加到 InFlightMessage 中
	err := c.pushInFlightMessage(msg)
	if err != nil {
		return err
	}
	// 记录进优先队列
	c.addToInFlightPQ(msg)
	return nil
}
// 准备添加延迟消息
func (c *Channel) StartDeferredTimeout(msg *Message, timeout time.Duration) error {
	// 获取消息延迟推送的目标时间
	absTs := time.Now().Add(timeout).UnixNano()
	// 包装成队列的一个节点
	item := &pqueue.Item{Value: msg, Priority: absTs}
	// 将消息存入 deferredMessage
	err := c.pushDeferredMessage(item)
	if err != nil {
		return err
	}
	// 将消添加到 deferredPQ 优先队列中,进行等待发送
	c.addToDeferredPQ(item)
	return nil
}
// 在 inFlightMessages 中存入一条消息
// pushInFlightMessage atomically adds a message to the in-flight dictionary
func (c *Channel) pushInFlightMessage(msg *Message) error {
	c.inFlightMutex.Lock()
	_, ok := c.inFlightMessages[msg.ID]
	if ok {
		c.inFlightMutex.Unlock()
		return errors.New("ID already in flight")
	}
	c.inFlightMessages[msg.ID] = msg
	c.inFlightMutex.Unlock()
	return nil
}

// popInFlightMessage atomically removes a message from the in-flight dictionary
func (c *Channel) popInFlightMessage(clientID int64, id MessageID) (*Message, error) {
	c.inFlightMutex.Lock()
	msg, ok := c.inFlightMessages[id]
	if !ok {
		c.inFlightMutex.Unlock()
		return nil, errors.New("ID not in flight")
	}
	if msg.clientID != clientID {
		c.inFlightMutex.Unlock()
		return nil, errors.New("client does not own message")
	}
	delete(c.inFlightMessages, id)
	c.inFlightMutex.Unlock()
	return msg, nil
}
// 添加到 inFlightPQ 中,等待确认或超时
func (c *Channel) addToInFlightPQ(msg *Message) {
	c.inFlightMutex.Lock()
	c.inFlightPQ.Push(msg)
	c.inFlightMutex.Unlock()
}

func (c *Channel) removeFromInFlightPQ(msg *Message) {
	c.inFlightMutex.Lock()
	if msg.index == -1 {
		// this item has already been popped off the pqueue
		c.inFlightMutex.Unlock()
		return
	}
	c.inFlightPQ.Remove(msg.index)
	c.inFlightMutex.Unlock()
}
// 向 deferredMessages 中存入一条消息
func (c *Channel) pushDeferredMessage(item *pqueue.Item) error {
	c.deferredMutex.Lock()
	// TODO: these map lookups are costly
	id := item.Value.(*Message).ID
	_, ok := c.deferredMessages[id]
	if ok {
		c.deferredMutex.Unlock()
		return errors.New("ID already deferred")
	}
	c.deferredMessages[id] = item
	c.deferredMutex.Unlock()
	return nil
}

func (c *Channel) popDeferredMessage(id MessageID) (*pqueue.Item, error) {
	c.deferredMutex.Lock()
	// TODO: these map lookups are costly
	item, ok := c.deferredMessages[id]
	if !ok {
		c.deferredMutex.Unlock()
		return nil, errors.New("ID not deferred")
	}
	delete(c.deferredMessages, id)
	c.deferredMutex.Unlock()
	return item, nil
}
// 添加到 deferredPQ 中
func (c *Channel) addToDeferredPQ(item *pqueue.Item) {
	c.deferredMutex.Lock()
	heap.Push(&c.deferredPQ, item)
	c.deferredMutex.Unlock()
}
/**
延迟消息队列
消息从 topic 送到 channel 中,自动添加到了延迟队列中,然后在 queueScanWorker 扫描 channel 的时候进行了处理
每次扫描就把最早的一条到时间的延迟消息当做正常消息发送即可
*/
func (c *Channel) processDeferredQueue(t int64) bool {
	c.exitMutex.RLock()
	defer c.exitMutex.RUnlock()

	if c.Exiting() {
		return false
	}

	dirty := false
	for {
		c.deferredMutex.Lock()
		// 等待时间到的第一条消息,可以投递
		item, _ := c.deferredPQ.PeekAndShift(t)
		c.deferredMutex.Unlock()

		if item == nil {
			goto exit
		}
		dirty = true

		msg := item.Value.(*Message)
		// 将消息取出来并进行 put
		_, err := c.popDeferredMessage(msg.ID)
		if err != nil {
			goto exit
		}
		// 此时已经是一条正常消息了,调用 put 投递即可
		c.put(msg)
	}

exit:
	return dirty
}
// 在 NSQD 协程中进行处理,传入当前时间,处理 channel 中的待发送消息
// inFlightPQ队列中储存了正在投递但还没确认投递成功的消息。  延迟时间60秒
// t是当前时间戳
func (c *Channel) processInFlightQueue(t int64) bool {
	c.exitMutex.RLock()
	defer c.exitMutex.RUnlock()

	if c.Exiting() {
		return false
	}

	// dirty = 是否有新消息

	dirty := false
	for {
		c.inFlightMutex.Lock()
		// 判断待确认的消息是否超时,获取等待确认超时的一条消息
		//用于判断根部的元素是否超过max，如果超过则返回nil，如果没有直接返回null
		msg, _ := c.inFlightPQ.PeekAndShift(t)
		c.inFlightMutex.Unlock()

		if msg == nil {
			// 没有消息等待确认超时,就不需要处理待确认队列
			goto exit
		}
		// 只要发送过消息，则标记此subchannel为dirty
		dirty = true
		// 删除超时消息对应channel的inFlightMessages消息map
		//从 channel InFlight拿到这条消息,如果有 表示超时没有确认，如果没有 就直接退出
		_, err := c.popInFlightMessage(msg.clientID, msg.ID)
		if err != nil {
			goto exit
		}
		// 这条消息超时没有确认,记录超时数据条数
		atomic.AddUint64(&c.timeoutCount, 1)
		c.RLock()
		client, ok := c.clients[msg.clientID]
		c.RUnlock()
		if ok {
			// 向超时消息对应client发送超时通知
			client.TimedOutMessage()
		}
		// 重新投递消息,调用 put 表示立即发送
		c.put(msg)
	}

exit:
	return dirty
}
