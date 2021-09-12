package nsqd

import (
	"io"
	"net"
	"sync"

	"github.com/nsqio/nsq/internal/protocol"
)
/**
nsqd/tcp.go文件，tcpServer通过Handle()方法接收TCP请求。
tcpServer是nsqd结构的成员，全局也就只有一个实例，
但在protocol包的TCPServer方法中，每创建一个新的连接，均会调用一次tcpServer.Handle()

// 管理所有 TCP 连接
// 通过实现 TCPHandler 接口来对新连接进行处理
 */
type tcpServer struct {
	ctx   *context
	conns sync.Map   // 这里使用了 syncmap，用 Addr 作为 key，Conn 作为 value
}
/*
   p.nsqd.Main()启动protocol.TCPServer()，
   这个方法里会为每个客户端连接创建一个新协程，协程执行tcpServer.Handle()方法
   本方法首先对新连接读取4字节校验版本，新连接必须首先发送4字节"  V2"。
   然后阻塞调用nsqd.protocolV2.IOLoop()处理客户端接下来的请求。
*/
func (p *tcpServer) Handle(clientConn net.Conn) {
	//每次请求处理 先打印客户端地址
	p.ctx.nsqd.logf(LOG_INFO, "TCP: new client(%s)", clientConn.RemoteAddr())

	// The client should initialize itself by sending a 4 byte sequence indicating
	//客户端应该初始化的时候 发送一个 4个字节的 序列化来初始化告诉他打算应用的通新版本，这样可以让
	// the version of the protocol that it intends to communicate, this will allow us
	//服务端进行优雅的判断 到底是走哪个版本。
	//说人话，就是先发个版本 看看到底是该用哪个版本的api承接
	// 客户端通过发送四个字节的消息来初始化自己，这里包括它通信的协议版本
	// to gracefully upgrade the protocol away from text/line oriented to whatever...
	buf := make([]byte, 4) //初始化的4个字节的buf
	// 获取协议版本,客户端建立连接后立即发送四字节版本初始化自己
	_, err := io.ReadFull(clientConn, buf) //先读4个字节
	if err != nil {
		// 初始化失败
		p.ctx.nsqd.logf(LOG_ERROR, "failed to read protocol version - %s", err)
		clientConn.Close()
		return
	}
	//转换成string
	protocolMagic := string(buf)
	// 打印协议版本，v1.2.0的协议版本打印出来的是 "  V2"
	p.ctx.nsqd.logf(LOG_INFO, "CLIENT(%s): desired protocol magic '%s'",
		clientConn.RemoteAddr(), protocolMagic)

	//实例化个接口
	// 创建对应的协议管理器
	var prot protocol.Protocol
	// TCP 解析 V2 协议，走内部协议封装的 prot.IOLoop(conn) 进行处理；
	//判断这4个字节到底是什么玩意
	switch protocolMagic {
	case "  V2":  //如果是v2  走 v2
		// 使用 V2 版本的协议操作来进行处理，创建对应的版本管理器即可
		prot = &protocolV2{ctx: p.ctx}
	default:  //默认的话
		// 暂时仅支持 V2 一个版本，其余的版本直接断开连接并返回
		protocol.SendFramedResponse(clientConn, frameTypeError, []byte("E_BAD_PROTOCOL"))
		clientConn.Close()
		p.ctx.nsqd.logf(LOG_ERROR, "client(%s) bad protocol magic '%s'",
			clientConn.RemoteAddr(), protocolMagic)
		return
	}
	// 将连接加入到 TCP 服务器的 sync.map 中
	p.conns.Store(clientConn.RemoteAddr(), clientConn)
	// 协议定义的操作，开始运行这个客户端的连接,进行数据的交换
	//处理该次请求  nsqd/protocol_v2.go
	//nsqd/protocol_v2.go  IOLoop
	err = prot.IOLoop(clientConn)
	if err != nil {
		p.ctx.nsqd.logf(LOG_ERROR, "client(%s) - %s", clientConn.RemoteAddr(), err)
	}
	//从连接管理器里删除
	p.conns.Delete(clientConn.RemoteAddr())
}
// nsqd 退出时调用
func (p *tcpServer) CloseAll() {
	p.conns.Range(func(k, v interface{}) bool {
		v.(net.Conn).Close()
		return true
	})
}
