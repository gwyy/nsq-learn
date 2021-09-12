package protocol

import (
	"fmt"
	"net"
	"runtime"
	"strings"
	"sync"

	"github.com/nsqio/nsq/internal/lg"
)

/**
tcp 链接  handler 每次处理
 */
type TCPHandler interface {
	Handle(net.Conn)
}
//开启一个tcp server
func TCPServer(listener net.Listener, handler TCPHandler, logf lg.AppLogFunc) error {
	// 开始监听 TCP 端口默认是 4150
	logf(lg.INFO, "TCP: listening on %s", listener.Addr())

	var wg sync.WaitGroup

	for {
		//死循环 一直等待
		clientConn, err := listener.Accept()
		if err != nil {
			if nerr, ok := err.(net.Error); ok && nerr.Temporary() {
				logf(lg.WARN, "temporary Accept() failure - %s", err)
				runtime.Gosched()
				continue
			}
			// 这里错误直接使用 fmt 进行打印的，因为没有将错误暴露出去？但是将这个错误传递出去了啊
			// 可能是因为，仅仅传递出去了错误，但是外面并没有进行处理，因为这个错误直接导致了系统的关闭
			// 传出去的错误是 LOG_FATAL 级别，并不会进行打印
			// theres no direct way to detect this error because it is not exposed
			if !strings.Contains(err.Error(), "use of closed network connection") {
				return fmt.Errorf("listener.Accept() error - %s", err)
			}
			break
		}
		// 这里进行 Add(1) 会一直保持已有连接不会因为错误连接导致 TCPServer 退出而断开，后面有 wg.Wait()
		wg.Add(1)
		//开个协程 处理 handler , 这个 handler是个接口 实际上依赖于nsqd/tcp.go
		go func() {
			// 执行连接处理函数，其实就是把连接放进 map 中进行管理而已
			//nsqd/tcp.go  func (p *tcpServer) Handle(clientConn net.Conn) {
			handler.Handle(clientConn)
			wg.Done()
		}()
	}

	// wait to return until all handler goroutines complete
	// 等待所有协程完成
	wg.Wait()

	logf(lg.INFO, "TCP: closing %s", listener.Addr())

	return nil
}
