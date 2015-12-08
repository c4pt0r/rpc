package main

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/c4pt0r/rpc"
	"github.com/ngaut/log"
)

type simpleRPCHandler struct{}

var cnt int64

func (s *simpleRPCHandler) Handle(method string, param []byte, cb rpc.Callback) {
	atomic.AddInt64(&cnt, 1)
	cb.Done([]byte("echo:"+string(param)), nil)
}

func main() {
	go func() {
		s := rpc.NewServer()
		s.Register("hello", &simpleRPCHandler{})
		s.ListenAndServe("localhost:1234")
	}()
	time.Sleep(2 * time.Second)
	c := rpc.NewClient("localhost:1234")
	if err := c.Connect(); err != nil {
		log.Fatal(err)
	}

	ct := time.Now()
	wg := sync.WaitGroup{}
	for i := 0; i < 10000; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			r, err := c.Call("hello.echo", []byte(fmt.Sprintf("%d", i)))
			if err != nil {
				log.Fatal(err)
			}
			log.Info(string(r))
		}(i)
	}
	wg.Wait()
	log.Info(time.Since(ct), cnt)
}
