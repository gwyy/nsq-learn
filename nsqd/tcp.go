package nsqd

import (
	"io"
	"net"
	"sync"

	"github.com/nsqio/nsq/internal/protocol"
)

type tcpServer struct {
	ctx   *context
	conns sync.Map
}

func (p *tcpServer) Handle(clientConn net.Conn) {
	//每次请求处理 先打印客户端地址
	p.ctx.nsqd.logf(LOG_INFO, "TCP: new client(%s)", clientConn.RemoteAddr())

	// The client should initialize itself by sending a 4 byte sequence indicating
	//客户端应该初始化的时候 发送一个 4个字节的 序列化来初始化告诉他打算应用的通新版本，这样可以让
	// the version of the protocol that it intends to communicate, this will allow us
	//服务端进行优雅的判断 到底是走哪个版本。
	//说人话，就是先发个版本 看看到底是该用哪个版本的api承接
	// to gracefully upgrade the protocol away from text/line oriented to whatever...
	buf := make([]byte, 4) //初始化的4个字节的buf
	_, err := io.ReadFull(clientConn, buf) //先读4个字节
	if err != nil {
		p.ctx.nsqd.logf(LOG_ERROR, "failed to read protocol version - %s", err)
		clientConn.Close()
		return
	}
	//转换成string
	protocolMagic := string(buf)

	p.ctx.nsqd.logf(LOG_INFO, "CLIENT(%s): desired protocol magic '%s'",
		clientConn.RemoteAddr(), protocolMagic)

	//实例化个接口
	var prot protocol.Protocol

	//判断这4个字节到底是什么玩意
	switch protocolMagic {
	case "  V2":  //如果是v2  走 v2
		prot = &protocolV2{ctx: p.ctx}
	default:  //默认的话
		protocol.SendFramedResponse(clientConn, frameTypeError, []byte("E_BAD_PROTOCOL"))
		clientConn.Close()
		p.ctx.nsqd.logf(LOG_ERROR, "client(%s) bad protocol magic '%s'",
			clientConn.RemoteAddr(), protocolMagic)
		return
	}
	//设置一些
	p.conns.Store(clientConn.RemoteAddr(), clientConn)

	//处理该次请求
	err = prot.IOLoop(clientConn)
	if err != nil {
		p.ctx.nsqd.logf(LOG_ERROR, "client(%s) - %s", clientConn.RemoteAddr(), err)
	}
	//从连接管理器里删除
	p.conns.Delete(clientConn.RemoteAddr())
}

func (p *tcpServer) CloseAll() {
	p.conns.Range(func(k, v interface{}) bool {
		v.(net.Conn).Close()
		return true
	})
}
