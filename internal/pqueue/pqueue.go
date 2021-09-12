package pqueue

import (
	"container/heap"
)

/**
优先级队列（ Priority Queue ） - 通过数据结构最小堆（ min heap ）实现，
pub 一条消息时立即就排好序（优先级通过 Priority-timeout 时间戳排序），
最近到期的放到最小堆根节点；取出一条消息直接从最小堆的根节点取出，时间复杂度很低。

延时队列（ deferredPQ ） - 通过 DPUB 发布消息时带有 timeout 属性值实现，表示从当前时间戳多久后可以取出来消费；

运行队列（ inFlightPQ ） - 正在被消费者 consumer 消费的消息放入运行队列中，若处理失败或超时则自动重新放入（ Requeue ）队列，
待下一次取出再次消费；消费成功（ Finish ）则删除对应的消息。

 Push函数用于向队列中添加元素

  Pop函数用于取出队列中优先级最高的元素（即Priority值最小，也就是根部元素）

  PeekAndShift(max int64) 用于判断根部的元素是否超过max，如果超过则返回nil，如果没有则返回并移除根部元素（根部元素是最小值）

    在实际的项目中Priority字段存的是时间戳，比如说5分钟之后投递本条消息，则Priority字段存的就是5分钟之后的时间戳。而PeekAndShift(max int64)中max值就是当前时间，如果队列中的根部元素大于当前时间戳max的值，则说明队列中没有可以投递的消息，故返回nil。如果小于等于，则根部元素存放的消息可以投递，就是就返回并移除该根部元素。
————————————————
版权声明：本文为CSDN博主「思维的深度」的原创文章，遵循CC 4.0 BY-SA版权协议，转载请附上原文出处链接及本声明。
原文链接：https://blog.csdn.net/skh2015java/article/details/83416045
*/
type Item struct {
	Value    interface{}
	Priority int64
	Index    int
}

// this is a priority queue as implemented by a min heap
// ie. the 0th element is the *lowest* value
type PriorityQueue []*Item

func New(capacity int) PriorityQueue {
	return make(PriorityQueue, 0, capacity)
}

func (pq PriorityQueue) Len() int {
	return len(pq)
}

func (pq PriorityQueue) Less(i, j int) bool {
	return pq[i].Priority < pq[j].Priority
}

func (pq PriorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].Index = i
	pq[j].Index = j
}

func (pq *PriorityQueue) Push(x interface{}) {
	n := len(*pq)
	c := cap(*pq)
	if n+1 > c {
		npq := make(PriorityQueue, n, c*2)
		copy(npq, *pq)
		*pq = npq
	}
	*pq = (*pq)[0 : n+1]
	item := x.(*Item)
	item.Index = n
	(*pq)[n] = item
}

func (pq *PriorityQueue) Pop() interface{} {
	n := len(*pq)
	c := cap(*pq)
	if n < (c/2) && c > 25 {
		npq := make(PriorityQueue, n, c/2)
		copy(npq, *pq)
		*pq = npq
	}
	item := (*pq)[n-1]
	item.Index = -1
	*pq = (*pq)[0 : n-1]
	return item
}

func (pq *PriorityQueue) PeekAndShift(max int64) (*Item, int64) {
	if pq.Len() == 0 {
		return nil, 0
	}

	item := (*pq)[0]
	if item.Priority > max {
		return nil, item.Priority - max
	}
	heap.Remove(pq, 0)

	return item, 0
}
