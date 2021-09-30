package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"
	// toml开源包
	"github.com/BurntSushi/toml"
	// 命令行控制包svc 服务控制
	"github.com/judwhite/go-svc/svc"
	// go-options开源包
	"github.com/mreiferson/go-options"
	// 内部包 日志中间件 log
	"github.com/nsqio/nsq/internal/lg"
	// 内部版本号
	"github.com/nsqio/nsq/internal/version"
	// nsqd真正工作的区域
	"github.com/nsqio/nsq/nsqd"
)

/**
json 打印测试方法
 */
func JsonExit(v interface{}) {
	str,err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	fmt.Println(string(str))
	os.Exit(1)
}
/**
pub:  http://127.0.0.1:4151/pub?topic=name
 */

/*
定义业务program结构体
*/
type program struct {
	// once能确保实例化对象Do方法在多线程环境只运行一次，内部通过互斥锁实现
	once sync.Once
	nsqd *nsqd.NSQD
}
/*
采用SVC包进行服务控制，主要是统一管理服务，对于信号控制不用每次都写在业务上，在ctrl+c时，能正常监听defer结束，方便获取很多日志，参数等
SVC包具体食用可以看我博客：https://www.cnblogs.com/gwyy/p/13551571.html
*/
func main() {
	// 实例化 program 结构体
	prg := &program{}
	/*

	    // Implement this interface and pass it to the Run function to start your program.
	type Service interface {
	    // Init is called before the program/service is started and after it's
	    // determined if the program is running as a Windows Service.
	    Init(Environment) error

	    // Start is called after Init. This method must be non-blocking.
	    Start() error

	    // Stop is called in response to os.Interrupt, os.Kill, or when a
	    // Windows Service is stopped.
	    Stop() error
	}
	    svc 第一个参数需要实现Service接口才可以正常运行，
	    所以这也就是大伙看到的program 实现的init/start/stop三个函数
	    使用svc启动相关程序 监听关闭信号
	*/
	if err := svc.Run(prg, syscall.SIGINT, syscall.SIGTERM); err != nil {
		logFatal("%s", err)
	}
}
//svc 的init 方法 初始化方法
func (p *program) Init(env svc.Environment) error {
	// 检查是否是windows 服务。。。目测一般时候也用不到，基本上可以直接过
	if env.IsWindowsService() {
		dir := filepath.Dir(os.Args[0])
		return os.Chdir(dir)
	}
	return nil
}
//真正的启动方法
func (p *program) Start() error {
	/*
	   实例化并初始一些配置和默认值
	   /nsq/nsqd/options.go
	*/
	opts := nsqd.NewOptions()
	/*
	   封装了命令行的一些检查项,设置检查项的默认值
	   使用apps目录:/nsq/apps/nsqd/options.go
	   然后parse解析
	*/
	flagSet := nsqdFlagSet(opts)
	flagSet.Parse(os.Args[1:])  //解析用户传参
	// 初始化load 随机数种子 time.Now().UnixNano()  单位纳秒
	rand.Seed(time.Now().UTC().UnixNano())
	// 打印版本号,接收命令行参数version  默认值：false 然后直接结束
	/*
	   执行效果
	    go run ./ --version=true
	   nsqd v1.2.1-alpha (built w/go1.12.2)
	*/
	if flagSet.Lookup("version").Value.(flag.Getter).Get().(bool) {
		fmt.Println(version.String("nsqd"))
		os.Exit(0)
	}
	// 获取外部的配置文件，解析toml文件格式
	var cfg config
	/*
	    go run ./ --config=config.toml
	*/
	//拿到配置文件地址
	configFile := flagSet.Lookup("config").Value.String()
	// 如果不为空
	if configFile != "" {
		// 加载，读出的数据采用空_  抛弃,赋值给cfg
		_, err := toml.DecodeFile(configFile, &cfg)
		// 如果抛错
		if err != nil {
			logFatal("failed to load config file %s - %s", configFile, err)
		}
	}
	// 检查配置文件  tls_min_version tls_required 等必填
	cfg.Validate()
	// 采用优先级从高到低依次进行解析，最终
	options.Resolve(opts, flagSet, cfg)
	/*
	   传入用户自定义配置，实例化nsqd

	   nsqd.new以后做了那些事情，大概捋一下，后续看的时候能加深印象
	   1、检查命令行cli   opts.DataPath 、  opts.Logger没设置  设置默认值
	   2、实例NSQD主对象
	   3、监听tcp   net.Listen("tcp", opts.TCPAddress)
	   4、监听http  net.Listen("tcp", opts.HTTPAddress)
	   5、监听https tls.Listen("tcp", opts.HTTPSAddress, n.tlsConfig)

	   综合以上了解，基本做的事情就是实例化主对象，并对cli 自定义的命令一顿操作。。。然后就这样了,return (*NSQD, error)
	*/
	nsqd, err := nsqd.New(opts)
	if err != nil {
		logFatal("failed to instantiate nsqd - %s", err)
	}
	//加入到program类里面
	p.nsqd = nsqd
	/*
	   加载历史数据，数据来源nsqd.dat -> 历史数据格式{"topics":[],"version":"1.2.1-alpha"}
	   1、获取历史数据
	   2、解析成对应的结构体 json.Unmarshal(data, &m)
	   3、遍历 for _, t := range m.Topics ， 解析每个topic -> channel
	   4、启动N个topic.Start()（重点代码中有一个GetTopic，采用线程线程安全方式，重点学习）
	   5、func (n *NSQD) LoadMetadata() error
	加载 nsqd.dat 中的数据
	遍历所有的 topic,对暂停服务的 topic 设置暂停标识
	将这个 topic 中的所有 channel 添加,对暂停服务的 channel 设置暂停标识

	*/
	err = p.nsqd.LoadMetadata()
	if err != nil {
		logFatal("failed to load metadata - %s", err)
	}
	/*
	   持久化最新数据
	   1、获取原始数据文件名 fileName := newMetadataFile(n.getOpts())
	   2、遍历 nsqd.topicMap -> ndqd.channelMap  ,这是对topicMap和channelMap加了互斥锁
	   3、将最新数据写入到临时文件中，明文明文件名为：nsqd.dat.333569681738193261.tmp
	   4、func (n *NSQD) PersistMetadata() error

	加载完之后立即进行数据的存储,不保存临时 topic 和 channel,流程如下:

	遍历所有 topic 保存起来
	遍历每个 topic 下的所有 channel
	头部加上 version 版本号
	保存到 nsqd.dat 文件中
	加载完立刻存储可能是为了更新 nsqd.dat 文件中的信息,因为在加载的过程中可能会对原有信息做一些改变,下面是 nsqd.dat 中的数据结构

	*/
	err = p.nsqd.PersistMetadata()
	if err != nil {
		logFatal("failed to persist metadata - %s", err)
	}
	/*
	   开启协程进入nsqd.Main主函数
	   Main方法里重点使用了封装的WaitGroup
	   下列出现的n.waitGroup.Wrap均采用了封装groutine

	   方法：
	   func (n *NSQD) Main() error {

	   大体执行思路
	   1、实例化context
	   2、建立退出通道，保证退出函数只运行一次，创建了匿名函数exitFunc
	   3、初始化并监听TCPServer
	   3.1、exitFunc(protocol.TCPServer(n.tcpListener, n.tcpServer, n.logf))
	   3.2、TCPServer采用无限循环方式监听tcp client长连接，当有一个client连接，分配一个groutine进行处理
	   for -> listener.Accept() -> groutine
	   4、初始化HTTPServer
	   4.1、使用httprouter进行路由设置，然后初始化各种接口

	   5、初始化HttpsServer
	   6、监控循环队列：n.waitGroup.Wrap(n.queueScanLoop)
	   7、节点信息管理：n.waitGroup.Wrap(n.lookupLoop)
	   8、统计信息：n.waitGroup.Wrap(n.statsdLoop)
	*/
	go func() {
		err := p.nsqd.Main()
		if err != nil {
			//有问题直接停止掉结束进程
			p.Stop()
			os.Exit(1)
		}
	}()

	return nil
}

func (p *program) Stop() error {
	/*
	        /*
	    底层源码
	    func (o *Once) Do(f func()) {
	    if atomic.LoadUint32(&o.done) == 1 {
	        return
	    }
	    // Slow-path.
	    o.m.Lock()
	    defer o.m.Unlock()
	    if o.done == 0 {
	        defer atomic.StoreUint32(&o.done, 1)
	        f()
	    }
	}

	    可以看成这样的链式操作
	    p.once.Do == program.once.Do

	    确保在执行时只执行一次退出操作
	*/

	p.once.Do(func() {
		p.nsqd.Exit()
	})
	return nil
}

func logFatal(f string, args ...interface{}) {
	lg.LogFatal("[nsqd] ", f, args...)
}
