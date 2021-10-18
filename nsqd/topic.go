package nsqd

import (
	"bytes"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nsqio/go-diskqueue"
	"github.com/nsqio/nsq/internal/lg"
	"github.com/nsqio/nsq/internal/quantile"
	"github.com/nsqio/nsq/internal/util"
)


/**
nsqd/topic.go文件，对应于每个topic实例，由系统启动时创建或者发布消息/消费消息时自动创建。


Topic 和 Channel 都没有预先配置。Topic 由第一次发布消息到命名的 Topic 或第一次通过订阅一个命名 Topic 来创建。
Channel 被第一次订阅到指定的 Channel 创建。Topic 和 Channel 的所有缓冲的数据相互独立，
防止缓慢消费者造成对其他 Channel 的积压（同样适用于 Topic 级别）。

多路分发 - producer 会同时连上 nsq 集群中所有 nsqd 节点，当然这些节点的地址是在初始化时，通过外界传递进去；
当发布消息时，producer 会随机选择一个 nsqd 节点发布某个 Topic 的消息；consumer 在订阅 subscribe 某个Topic/Channel时，
会首先连上 nsqlookupd 获取最新可用的 nsqd 节点，然后通过 TCP 长连接方式连上所有发布了指定 Topic 的 producer 节点，
并在本地用 tornado 轮询每个连接，当某个连接有可读事件时，即有消息达到，处理即可。

负载均衡 - 当向某个 Topic 发布一个消息时，该消息会被复制到所有的 Channel，如果 Channel 只有一个客户端，
那么 Channel 就将消息投递给这个客户端；如果 Channel 的客户端不止一个，那么 Channel 将把消息随机投递给任何一个客户端，这也可以看做是客户端的负载均衡；
*/

/*
nsqd 是所有 topic 的管理者,topic 是它自己所有 channel 的管理者,同样也是使用了 map+RWMutex 来存储管理 channel.
*/
type Topic struct {
	// 64bit atomic vars need to be first for proper alignment on 32bit platforms
	// 这两个字段仅作统计信息,保证 32 位对其操作
	messageCount uint64  // 累计消息数
	messageBytes uint64// 累计消息体的字节数

	sync.RWMutex  // 加锁，包括 putMessage

	name              string // topic名，生产和消费时需要指定此名称
	channelMap        map[string]*Channel  // 保存每个channel name和channel指针的映射
	backend           BackendQueue    // 磁盘队列，当内存memoryMsgChan满时，写入硬盘队列
	memoryMsgChan     chan *Message    // 消息优先存入这个内存chan
	startChan         chan int  	// 接收开始信号的 channel，调用 start 开始 topic 消息循环

	exitChan          chan int  	// 判断 topic 是否退出

	// 在 select 的地方都要添加 exitChan
	// 除非使用 default 或者保证程序不会永远阻塞在 select 处,即可以退出循环
	// channel 更新时用来通知并更新消息循环中的 chan 数组
	channelUpdateChan chan int
	// 用来等待所有的子 goroutine
	waitGroup         util.WaitGroupWrapper
	exitFlag          int32     // topic 退出标识符
	idFactory         *guidFactory    // 生成 guid 的工厂方法

	ephemeral      bool  // 该 topic 是否是临时 topic
	deleteCallback func(*Topic)   // topic 删除时的回调函数
	deleter        sync.Once   // 确保 deleteCallback 仅执行一次

	paused    int32   // topic 是否暂停
	pauseChan chan int   // 改变 topic 暂停/运行状态的通道
	ctx *context  // topic 的上下文
}

/**
每个 topic 都有一个消息队列缓冲区,默认大小为 10000.并且多余的消息会放到 backend 中进行落地保存.
保存的方式有两种,如果这是一个临时的 topic 会直接进行丢弃,否则会使用 diskqueue 保存到磁盘中.
在 NewTopic 的最后启动了一个 goroutine 来执行 t.messagePump().
看到这里会好奇,topic 是怎么和 nsqd 联系起来的呢?当 nsqd 初始化时,会加载已经存在的 topic 然后执行 Start() 函数开始运行.
 */
// Topic constructor
func NewTopic(topicName string, ctx *context, deleteCallback func(*Topic)) *Topic {
	t := &Topic{
		name:              topicName, //topic名称
		channelMap:        make(map[string]*Channel),
		memoryMsgChan:     nil,
		startChan:         make(chan int, 1),
		exitChan:          make(chan int),
		channelUpdateChan: make(chan int),
		ctx:               ctx, //上下文指针
		paused:            0,
		pauseChan:         make(chan int),
		deleteCallback:    deleteCallback, //删除callback函数
		// 所有 topic 使用同一个 guidFactory，因为都是用的 nsqd 的 ctx.nsqd.getOpts().ID 为基础生成的
		idFactory:         NewGUIDFactory(ctx.nsqd.getOpts().ID),
	}
	// create mem-queue only if size > 0 (do not use unbuffered chan)
	//	// 根据消息队列生成消息 chan,default size = 10000
	if ctx.nsqd.getOpts().MemQueueSize > 0 {
		// 初始化一个消息队列
		t.memoryMsgChan = make(chan *Message, ctx.nsqd.getOpts().MemQueueSize)
	}
	// 判断这个 topic 是不是暂时的，暂时的 topic 消息仅仅存储在内存中
	// DummyBackendQueue 和 diskqueue 均实现了 backend 接口
	if strings.HasSuffix(topicName, "#ephemeral") {
		// 临时的 topic，设置标志并使用 newDummyBackendQueue 初始化 backend
		t.ephemeral = true
		t.backend = newDummyBackendQueue()   // 实现了 backend 但是并没有逻辑，所有操作仅仅返回 nil
	} else {
		dqLogf := func(level diskqueue.LogLevel, f string, args ...interface{}) {
			opts := ctx.nsqd.getOpts()
			lg.Logf(opts.Logger, opts.LogLevel, lg.LogLevel(level), f, args...)
		}
		// 使用 diskqueue 初始化 backend 队列
		t.backend = diskqueue.New(
			topicName,
			ctx.nsqd.getOpts().DataPath,
			ctx.nsqd.getOpts().MaxBytesPerFile,
			int32(minValidMsgLength),
			int32(ctx.nsqd.getOpts().MaxMsgSize)+minValidMsgLength,
			ctx.nsqd.getOpts().SyncEvery,
			ctx.nsqd.getOpts().SyncTimeout,
			dqLogf,
		)
	}
	// 使用一个新的协程来执行 messagePump
	//startChan 就发送给了它,messagePump 函数负责分发整个 topic 接收到的消息给该 topic 下的 channels.
	t.waitGroup.Wrap(t.messagePump)
	// 调用 Notify
	t.ctx.nsqd.Notify(t)

	return t
}
//topic 启动的代码很简单,就是发送一个 startChan:
/**
那么这个 startChan 发给哪里了呢?还记得 NewTopic 最后执行了 t.messagePump(),
startChan 就发送给了它,messagePump 函数负责分发整个 topic 接收到的消息给该 topic 下的 channels.

这里为什么要加 select 呢，因为 如果直接  t.startChan <- 1 可能会阻塞，如果使用select 那么发不了
他会走default 不会触发阻塞
*/
func (t *Topic) Start() {
	select {
	case t.startChan <- 1:
	default:
	}
}

// Exiting returns a boolean indicating if this topic is closed/exiting
func (t *Topic) Exiting() bool {
	return atomic.LoadInt32(&t.exitFlag) == 1
}

// GetChannel performs a thread safe operation
// to return a pointer to a Channel object (potentially new)
// for the given Topic
// 获取一个 channel 没有就创建一个
func (t *Topic) GetChannel(channelName string) *Channel {
	t.Lock()
	channel, isNew := t.getOrCreateChannel(channelName)
	t.Unlock()
	// 这里监听 exitChan 是一个值得探讨的点，通过监听 exitChan 可以获取 channelUpdateChan 是否关闭
	// 从而控制是否向 channelUpdateChan 中发送消息
	// 否则可能，channelUpdateChan 的对端已经不再监听，但是程序会阻塞在向 channelUpdateChan 发送消息不能释放
	if isNew {
		// 更新 messagePump 状态
		// update messagePump state
		select {
		// 创建新的 channel 就更新 topic
		case t.channelUpdateChan <- 1:
		case <-t.exitChan:
		}
	}

	return channel
}

// 获取或创建一个 Channel 这个函数期望调用者加锁
// 这个函数仅由内部进行调用，例如 GetChannel 调用
// this expects the caller to handle locking
//这里 getChannel 类似 getTopic 都是不存在就创建新的,需要注意的是 channel 的删除回调函数
// channel 删除的回调函数，仅在 channel 存在时才进行删除

func (t *Topic) getOrCreateChannel(channelName string) (*Channel, bool) {
	// 获取 channel
	channel, ok := t.channelMap[channelName]
	if !ok {
		// 没有就创建
		deleteCallback := func(c *Channel) {
			// channel 删除函数，用回调的方式添加到 channel 中
			t.DeleteExistingChannel(c.name)
		}
		// 创建新的 channel
		channel = NewChannel(t.name, channelName, t.ctx, deleteCallback)
		// 加入 channel map
		t.channelMap[channelName] = channel
		t.ctx.nsqd.logf(LOG_INFO, "TOPIC(%s): new channel(%s)", t.name, channel.name)
		return channel, true
	}
	// 存在直接返回
	return channel, false
}
// 获取一个 channel 不存在报错
func (t *Topic) GetExistingChannel(channelName string) (*Channel, error) {
	t.RLock()
	defer t.RUnlock()
	channel, ok := t.channelMap[channelName]
	if !ok {
		return nil, errors.New("channel does not exist")
	}
	return channel, nil
}

//// channel 删除的回调函数，仅在 channel 存在时才进行删除
// DeleteExistingChannel removes a channel from the topic only if it exists
func (t *Topic) DeleteExistingChannel(channelName string) error {
	t.Lock()
	channel, ok := t.channelMap[channelName]
	if !ok {
		t.Unlock()
		return errors.New("channel does not exist")
	}
	// 从 channelMap 中删除这个 channel
	delete(t.channelMap, channelName)
	// not defered so that we can continue while the channel async closes
	numChannels := len(t.channelMap)
	t.Unlock()

	t.ctx.nsqd.logf(LOG_INFO, "TOPIC(%s): deleting channel %s", t.name, channel.name)

	// delete empties the channel before closing
	// (so that we dont leave any messages around)
	// 删除 channel 前，会先清空 channel 中的 msg 再删除

	channel.Delete()
	// 更新 messagePump 状态

	// update messagePump state
	select {
	case t.channelUpdateChan <- 1:
	case <-t.exitChan:
	}
	// 当临时 topic 中不存在 channel 时，将其删除

	if numChannels == 0 && t.ephemeral == true {
		go t.deleter.Do(func() { t.deleteCallback(t) })
	}

	return nil
}

/**
topic 里的消息从哪里来?怎么放进来的?首先 topic 里的消息来自于客户端，暂时先不需要了解这部分，只需要知道 topic 怎么去接收一条或多条消息即可，代码如下：
// 向 topic 中写入一条消息

 */
// PutMessage writes a Message to the queue
func (t *Topic) PutMessage(m *Message) error {
	t.RLock()
	defer t.RUnlock()
	// topic 已经退出了
	if atomic.LoadInt32(&t.exitFlag) == 1 {
		return errors.New("exiting")
	}
	// 调用 put 放进一条消息
	err := t.put(m)
	if err != nil {
		return err
	}
	// 增加消息计数和数据传输大小
	atomic.AddUint64(&t.messageCount, 1)
	atomic.AddUint64(&t.messageBytes, uint64(len(m.Body)))
	return nil
}

/**

  下面两个方法负责将消息写入topic，底层均调用topic.put()方法
  1. topic.memoryMsgChan未满时，优先写入内存memoryMsgChan
  2. 否则，写入磁盘topic.backend

 */
// PutMessages writes multiple Messages to the queue
func (t *Topic) PutMessages(msgs []*Message) error {
	t.RLock()
	defer t.RUnlock()
	// topic 已经退出了

	if atomic.LoadInt32(&t.exitFlag) == 1 {
		return errors.New("exiting")
	}

	messageTotalBytes := 0

	for i, m := range msgs {
		// 调用 put 添加 msg
		err := t.put(m)
		if err != nil {
			// 增加消息计数
			atomic.AddUint64(&t.messageCount, uint64(i))
			atomic.AddUint64(&t.messageBytes, uint64(messageTotalBytes))
			return err
		}
		// 记录所有消息长度
		messageTotalBytes += len(m.Body)
	}
	// 增加消息计数和数据传输大小
	atomic.AddUint64(&t.messageBytes, uint64(messageTotalBytes))
	atomic.AddUint64(&t.messageCount, uint64(len(msgs)))
	return nil
}

// 投递一条消息到 channel 中
/**
向 topic 放入消息流程很简单,在放入的同时做一些记录,
然后使用 put 先尝试将消息存入 memoryMsgChan,
存不进去就会存入 backend 中,然后 topic 的 messagePump 会从 memoryMsgChan 和 backend 中获取消息进行投递.

client->topic->allChannel->clients

在一个 select 中 case 的优先级高于 default,put 中的 select 其实会被编译为 if{}else{}类型,所以会先判断 memoryMsgChan 能不能存消息,不能存的话,才进行消息的落地
 */
func (t *Topic) put(m *Message) error {
	select {
	case t.memoryMsgChan <- m:  // 内存中的消息队列还可以继续存放，直接放入 memoryMsgChan 交给 messagePump 处理即可
	default:
		// 内存中消息队列存放满了，需要存放到 backend 中
		// 临时的 topic 会直接进行丢弃，永久的则会存放到磁盘中
		b := bufferPoolGet()  // 从 buffer pool 中获取一个 buffer
		// 调用 writeMessageToBackend 将消息写入磁盘中
		err := writeMessageToBackend(b, m, t.backend)
		// 归还 buffer 对象
		bufferPoolPut(b)
		// 将错误存储在 nsqd 中
		t.ctx.nsqd.SetHealth(err)
		if err != nil {
			t.ctx.nsqd.logf(LOG_ERROR,
				"TOPIC(%s) ERROR: failed to write message to backend - %s",
				t.name, err)
			return err
		}
	}
	return nil
}

func (t *Topic) Depth() int64 {
	return int64(len(t.memoryMsgChan)) + t.backend.Depth()
}
/*
    NewTopic创建新的topic时会为每个topic启动一个独立线程来处理消息推送，即messagePump()
    此方法循环随机从内存memoryMsgChan和磁盘队列backend中取消息写入到topic下每一个chnnel中
*/
// messagePump selects over the in-memory and backend queue and
// writes messages to every channel for this topic
/**
// 负责消息循环，将进入 topic 中的消息投递到 channel 中
// 仅在 topic 创建时通过新的协程开始执行
整个 topic 的消息循环还是很简洁的,通过一个 for 循环来监听 channel 的修改,
topic 的暂停恢复,以及来自消息缓冲区的新消息和落地队列中读取出来的旧消息,
再将收到的消息给每个 channel 复制一份进行投递
*/
func (t *Topic) messagePump() {
	var msg *Message
	var buf []byte
	var err error
	var chans []*Channel
	var memoryMsgChan chan *Message
	var backendChan <-chan []byte

	//	// 仅在触发 startChan 开始运行，否则会阻塞住，直到退出
	// do not pass messages before Start(), but avoid blocking Pause() or GetChannel()
	for {
		select {
		case <-t.channelUpdateChan:
			continue
		case <-t.pauseChan:
			continue
		case <-t.exitChan:
			goto exit
		case <-t.startChan:
		}
		break
	}
	t.RLock()
	// 获取当前 topic 的所有 channel
	for _, c := range t.channelMap {
		chans = append(chans, c)
	}
	t.RUnlock()
	// 设置消息循环接收的通道
	if len(chans) > 0 && !t.IsPaused() {
		memoryMsgChan = t.memoryMsgChan   // 消息缓冲区
		backendChan = t.backend.ReadChan()   // 落地通道
	}

	// main message loop  	// 主消息循环
	for {
		select {
		case msg = <-memoryMsgChan:  // 消息队列收到了新消息,开始运行后面的代码
		case buf = <-backendChan:
			// 内存满了后放到磁盘中的消息读取出来，需要解数据才能使用
			// 解消息的消息结构见 writeMessageToBackend
			msg, err = decodeMessage(buf)
			if err != nil {
				t.ctx.nsqd.logf(LOG_ERROR, "failed to decode message - %s", err)
				continue
			}
		case <-t.channelUpdateChan:
			// channel 的更新，清空后重新获取即可
			chans = chans[:0]
			t.RLock()
			// 新的 channels
			for _, c := range t.channelMap {
				chans = append(chans, c)
			}
			t.RUnlock()
			// 更新一下监听 channel 即可，没有 channel 就暂停
			if len(chans) == 0 || t.IsPaused() {
				memoryMsgChan = nil
				backendChan = nil
			} else {
				memoryMsgChan = t.memoryMsgChan
				backendChan = t.backend.ReadChan()
			}
			continue
		case <-t.pauseChan:
			// 可能是暂停 topic 也可能是开启 topic
			if len(chans) == 0 || t.IsPaused() {
				// 暂停 topic
				memoryMsgChan = nil
				backendChan = nil
			} else {
				// 恢复 topic
				memoryMsgChan = t.memoryMsgChan
				backendChan = t.backend.ReadChan()
			}
			continue
		case <-t.exitChan:  // 退出 topic 消息循环
			goto exit
		}
		// 遍历所有 channel，在 memoryMsgChan 触发和 backendChan 解消息成功时执行
		for i, channel := range chans {
			// msg 就是那个消息
			chanMsg := msg
			// 每个 channel 都需要唯一的实例，第一个 channel 不需要复制，已经有一份了(msg)，小优化
			// copy the message because each channel
			// needs a unique instance but...
			// fastpath to avoid copy if its the first channel
			// (the topic already created the first copy)
			if i > 0 {
				// 复制新的 message
				chanMsg = NewMessage(msg.ID, msg.Body)
				chanMsg.Timestamp = msg.Timestamp
				chanMsg.deferred = msg.deferred
			}
			// 延迟消息，使用 PutMessageDeferred 进行投递
			if chanMsg.deferred != 0 {
				channel.PutMessageDeferred(chanMsg, chanMsg.deferred)
				continue
			}
			// 即时消息直接投递到所有 channel 即可
			err := channel.PutMessage(chanMsg)
			if err != nil {
				t.ctx.nsqd.logf(LOG_ERROR,
					"TOPIC(%s) ERROR: failed to put msg(%s) to channel(%s) - %s",
					t.name, msg.ID, channel.name, err)
			}
		}
	}

exit:
	t.ctx.nsqd.logf(LOG_INFO, "TOPIC(%s): closing ... messagePump", t.name)
}
// 删除 topic 并关闭所有 channel
// Delete empties the topic and all its channels and closes
func (t *Topic) Delete() error {
	return t.exit(true)
}

// 关闭 topic 存储所有未完成的 topic data 并关闭所有 channel
// Close persists all outstanding topic data and closes all its channels
func (t *Topic) Close() error {
	return t.exit(false)
}
// topic 删除和关闭均调用这个函数
/**
topic 的删除和关闭使用的都是 exit 函数,但是二者的区别在于删除会删除所有包括内存和磁盘的消息,但是关闭会把内存中未投递的消息保存至磁盘中.

*/
func (t *Topic) exit(deleted bool) error {
	// 打开退出标志
	if !atomic.CompareAndSwapInt32(&t.exitFlag, 0, 1) {
		return errors.New("exiting")
	}

	// 是否删除
	if deleted {
		t.ctx.nsqd.logf(LOG_INFO, "TOPIC(%s): deleting", t.name)

		// since we are explicitly deleting a topic (not just at system exit time)
		// de-register this from the lookupd
		// 需要在 lookupd 中删除这个 topic
		t.ctx.nsqd.Notify(t)
	} else {
		t.ctx.nsqd.logf(LOG_INFO, "TOPIC(%s): closing", t.name)
	}
	// 关闭 topic 所有 channel，真好用
	close(t.exitChan)

	// synchronize the close of messagePump()
	// 同步等待 messagePump 关闭
	t.waitGroup.Wait()

	if deleted {
		// 删除的话，先清空 channelMap
		t.Lock()
		for _, channel := range t.channelMap {
			// 手动释放了
			delete(t.channelMap, channel.name)
			channel.Delete()
		}
		t.Unlock()

		// empty the queue (deletes the backend files, too)
		// 删除所有未投递的消息
		t.Empty()
		return t.backend.Delete()
	}

	// 关闭所有 channel，并不删除消息
	// close all the channels
	for _, channel := range t.channelMap {
		err := channel.Close()
		if err != nil {
			// we need to continue regardless of error to close all the channels
			t.ctx.nsqd.logf(LOG_ERROR, "channel(%s) close - %s", channel.name, err)
		}
	}

	// write anything leftover to disk
	// 将 channel 中未投递的数据全部保存至磁盘中
	t.flush()
	return t.backend.Close()
}

// 清空未投递的消息
func (t *Topic) Empty() error {
	for {
		select {
		case <-t.memoryMsgChan:
			// 把所有消息队列中的消息删除，接受但不处理
		default:
			goto finish
		}
	}

finish:
	return t.backend.Empty()  	// 最后清除 backend 中的消息

}
// 将内存中的未投递消息保存至磁盘中
func (t *Topic) flush() error {
	var msgBuf bytes.Buffer

	if len(t.memoryMsgChan) > 0 {
		t.ctx.nsqd.logf(LOG_INFO,
			"TOPIC(%s): flushing %d memory messages to backend",
			t.name, len(t.memoryMsgChan))
	}

	for {
		select {
		case msg := <-t.memoryMsgChan:
			// 使用 writeMessageToBackend 写入磁盘即可
			err := writeMessageToBackend(&msgBuf, msg, t.backend)
			if err != nil {
				t.ctx.nsqd.logf(LOG_ERROR,
					"ERROR: failed to write message to backend - %s", err)
			}
		default:
			goto finish
		}
	}

finish:
	return nil
}

func (t *Topic) AggregateChannelE2eProcessingLatency() *quantile.Quantile {
	var latencyStream *quantile.Quantile
	t.RLock()
	realChannels := make([]*Channel, 0, len(t.channelMap))
	for _, c := range t.channelMap {
		realChannels = append(realChannels, c)
	}
	t.RUnlock()
	for _, c := range realChannels {
		if c.e2eProcessingLatencyStream == nil {
			continue
		}
		if latencyStream == nil {
			latencyStream = quantile.New(
				t.ctx.nsqd.getOpts().E2EProcessingLatencyWindowTime,
				t.ctx.nsqd.getOpts().E2EProcessingLatencyPercentiles)
		}
		latencyStream.Merge(c.e2eProcessingLatencyStream)
	}
	return latencyStream
}

func (t *Topic) Pause() error {
	return t.doPause(true)
}

func (t *Topic) UnPause() error {
	return t.doPause(false)
}

func (t *Topic) doPause(pause bool) error {
	if pause {
		atomic.StoreInt32(&t.paused, 1)
	} else {
		atomic.StoreInt32(&t.paused, 0)
	}

	select {
	case t.pauseChan <- 1:
	case <-t.exitChan:
	}

	return nil
}

func (t *Topic) IsPaused() bool {
	return atomic.LoadInt32(&t.paused) == 1
}

func (t *Topic) GenerateID() MessageID {
retry:
	id, err := t.idFactory.NewGUID()
	if err != nil {
		time.Sleep(time.Millisecond)
		goto retry
	}
	return id.Hex()
}
