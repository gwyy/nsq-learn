package main

import (
	"math/rand"
	"os"
	"testing"
	"time"
)

func TestFirst(t *testing.T) {

	rand.Seed(time.Now().UTC().UnixNano())
	//for i:=0;i<100;i++ {
	//	t.Log(time.Now().UTC().UnixNano())
	//}
	t.Log(os.Getwd())
}
