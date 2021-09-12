package main

import (
	"fmt"
	"github.com/nsqio/go-nsq"
	"os"
	"os/signal"
	"time"
)
type MyTestHandler struct {
	consumer *nsq.Consumer
}

func (m MyTestHandler) HandleMessage(message *nsq.Message) error {
	fmt.Println(string(message.Body))
	return nil
}
//curl -d 'hello world 2' 'http://127.0.0.1:9102/pub?topic=testTopic1'
func main() {
	adds := []string{"127.0.0.1:7201", "127.0.0.1:8201"}
	config := nsq.NewConfig()
	config.MaxInFlight = 1000
	config.MaxBackoffDuration = 5 * time.Second
	config.DialTimeout = 10 * time.Second


	topicName := "testTopic1"
	c, _ := nsq.NewConsumer(topicName, "ch1", config)
	testHandler := &MyTestHandler{consumer: c}

	c.AddHandler(testHandler)
	//if err := c.ConnectToNSQDs(adds); err != nil {
	if err := c.ConnectToNSQLookupds(adds); err != nil {
		panic(err)
	}

	stats := c.Stats()
	if stats.Connections == 0 {
		panic("stats report 0 connections (should be > 0)")
	}
	stop := make(chan os.Signal)
	signal.Notify(stop, os.Interrupt)
	fmt.Println("server is running....")
	<-stop

}