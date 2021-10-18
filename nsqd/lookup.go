package nsqd

import (
	"bytes"
	"encoding/json"
	"net"
	"os"
	"strconv"
	"time"

	"github.com/nsqio/go-nsq"
	"github.com/nsqio/nsq/internal/version"
)

func connectCallback(n *NSQD, hostname string) func(*lookupPeer) {
	return func(lp *lookupPeer) {
		//鉴权
		ci := make(map[string]interface{})
		ci["version"] = version.Binary
		ci["tcp_port"] = n.RealTCPAddr().Port
		ci["http_port"] = n.RealHTTPAddr().Port
		ci["hostname"] = hostname
		ci["broadcast_address"] = n.getOpts().BroadcastAddress

		cmd, err := nsq.Identify(ci)
		if err != nil {
			lp.Close()
			return
		}
		//发送鉴权
		resp, err := lp.Command(cmd)
		if err != nil {
			n.logf(LOG_ERROR, "LOOKUPD(%s): %s - %s", lp, cmd, err)
			return
		} else if bytes.Equal(resp, []byte("E_INVALID")) {
			n.logf(LOG_INFO, "LOOKUPD(%s): lookupd returned %s", lp, resp)
			lp.Close()
			return
		} else {
			err = json.Unmarshal(resp, &lp.Info)
			if err != nil {
				n.logf(LOG_ERROR, "LOOKUPD(%s): parsing response - %s", lp, resp)
				lp.Close()
				return
			} else {
				//鉴权成功
				// LOOKUPD(127.0.0.1:4160): peer info {TCPPort:4160 HTTPPort:4161 Version:1.2.1-alpha BroadcastAddress:liangtiandeMacBook-Pro.local}
				n.logf(LOG_INFO, "LOOKUPD(%s): peer info %+v", lp, lp.Info)
				if lp.Info.BroadcastAddress == "" {
					n.logf(LOG_ERROR, "LOOKUPD(%s): no broadcast address", lp)
				}
			}
		}

		// build all the commands first so we exit the lock(s) as fast as possible
		var commands []*nsq.Command
		n.RLock()
		for _, topic := range n.topicMap {
			topic.RLock()
			if len(topic.channelMap) == 0 {
				commands = append(commands, nsq.Register(topic.name, ""))
			} else {
				for _, channel := range topic.channelMap {
					commands = append(commands, nsq.Register(channel.topicName, channel.name))
				}
			}
			topic.RUnlock()
		}
		n.RUnlock()

		for _, cmd := range commands {
			n.logf(LOG_INFO, "LOOKUPD(%s): %s", lp, cmd)
			_, err := lp.Command(cmd)
			if err != nil {
				n.logf(LOG_ERROR, "LOOKUPD(%s): %s - %s", lp, cmd, err)
				return
			}
		}
	}
}

/**
此处的事件循环，是用于和lookupd 交户使用的事件处理模块。例如Topic 增加或者删除，
channel 增加或者删除 需要对所有 nslookupd 模块做消息广播等处理逻辑，均在此处实现。
主要的事件:

定时心跳操作 每隔 15s 发送 PING 到 所有 nslookupd 的节点上
topic,channel新增删除操作 发送消息到所有 nslookupd 的节点上
配置修改的操作 如果配置修改，会重新从配置中刷新一次 nslookupd 节点
lookupdTCPAddrs  NSQLookupdTCPAddresses
go run main.go options.go  --lookupd-tcp-address=127.0.0.1:4160
 */
func (n *NSQD) lookupLoop() {
	var lookupPeers []*lookupPeer
	var lookupAddrs []string
	connect := true

	hostname, err := os.Hostname()
	if err != nil {
		n.logf(LOG_FATAL, "failed to get hostname - %s", err)
		os.Exit(1)
	}

	// for announcements, lookupd determines the host automatically
	//每15秒执行一次
	ticker := time.Tick(15 * time.Second)
	for {
		if connect {
			//循环所有的 nsqlookupd 地址 我们这边就一个 127.0.0.1:4160
			for _, host := range n.getOpts().NSQLookupdTCPAddresses {
				if in(host, lookupAddrs) {
					continue
				}
				//LOOKUP(127.0.0.1:4160): adding peer
				n.logf(LOG_INFO, "LOOKUP(%s): adding peer", host)
				//实例化newLookupPeer数据结构，并且拿到链接callback方法
				lookupPeer := newLookupPeer(host, n.getOpts().MaxBodySize, n.logf,
					connectCallback(n, hostname))
				//尝试链接
				lookupPeer.Command(nil) // start the connection
				//把nsqlookupd 写入lookupPeers
				lookupPeers = append(lookupPeers, lookupPeer)
				lookupAddrs = append(lookupAddrs, host)
			}
			n.lookupPeers.Store(lookupPeers)
			connect = false
		}

		select {
		//15分钟发送一次心跳
		case <-ticker:
			// send a heartbeat and read a response (read detects closed conns)
			for _, lookupPeer := range lookupPeers {
				n.logf(LOG_DEBUG, "LOOKUPD(%s): sending heartbeat", lookupPeer)
				cmd := nsq.Ping()
				_, err := lookupPeer.Command(cmd)
				if err != nil {
					n.logf(LOG_ERROR, "LOOKUPD(%s): %s - %s", lookupPeer, cmd, err)
				}
			}
		case val := <-n.notifyChan:
			var cmd *nsq.Command
			var branch string

			switch val.(type) {
			case *Channel:
				// notify all nsqlookupds that a new channel exists, or that it's removed
				branch = "channel"
				channel := val.(*Channel)
				if channel.Exiting() == true {
					cmd = nsq.UnRegister(channel.topicName, channel.name)
				} else {
					cmd = nsq.Register(channel.topicName, channel.name)
				}
			case *Topic:
				// notify all nsqlookupds that a new topic exists, or that it's removed
				branch = "topic"
				topic := val.(*Topic)
				if topic.Exiting() == true {
					cmd = nsq.UnRegister(topic.name, "")
				} else {
					cmd = nsq.Register(topic.name, "")
				}
			}

			for _, lookupPeer := range lookupPeers {
				n.logf(LOG_INFO, "LOOKUPD(%s): %s %s", lookupPeer, branch, cmd)
				_, err := lookupPeer.Command(cmd)
				if err != nil {
					n.logf(LOG_ERROR, "LOOKUPD(%s): %s - %s", lookupPeer, cmd, err)
				}
			}
		case <-n.optsNotificationChan:
			var tmpPeers []*lookupPeer
			var tmpAddrs []string
			for _, lp := range lookupPeers {
				if in(lp.addr, n.getOpts().NSQLookupdTCPAddresses) {
					tmpPeers = append(tmpPeers, lp)
					tmpAddrs = append(tmpAddrs, lp.addr)
					continue
				}
				n.logf(LOG_INFO, "LOOKUP(%s): removing peer", lp)
				lp.Close()
			}
			lookupPeers = tmpPeers
			lookupAddrs = tmpAddrs
			connect = true
		case <-n.exitChan:
			goto exit
		}
	}

exit:
	n.logf(LOG_INFO, "LOOKUP: closing")
}

func in(s string, lst []string) bool {
	for _, v := range lst {
		if s == v {
			return true
		}
	}
	return false
}

func (n *NSQD) lookupdHTTPAddrs() []string {
	var lookupHTTPAddrs []string
	lookupPeers := n.lookupPeers.Load()
	if lookupPeers == nil {
		return nil
	}
	for _, lp := range lookupPeers.([]*lookupPeer) {
		if len(lp.Info.BroadcastAddress) <= 0 {
			continue
		}
		addr := net.JoinHostPort(lp.Info.BroadcastAddress, strconv.Itoa(lp.Info.HTTPPort))
		lookupHTTPAddrs = append(lookupHTTPAddrs, addr)
	}
	return lookupHTTPAddrs
}
