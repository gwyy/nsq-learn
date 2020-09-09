package util

import (
	"sync"
)
//WaitGroupWrapper是对sync.WaitGroup的简单包装
type WaitGroupWrapper struct {
	sync.WaitGroup
}

func (w *WaitGroupWrapper) Wrap(cb func()) {
	w.Add(1)
	go func() {
		cb()
		w.Done()
	}()
}
