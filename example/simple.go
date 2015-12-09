package main

import (
	"fmt"
	"net/http"
	_ "net/http/pprof"
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
	go func() {
		http.ListenAndServe("localhost:4321", nil)
	}()
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
			for j := 0; j < 100; j++ {
				_, err := c.Call("hello.echo", []byte(fmt.Sprintf("%d", i)))
				if err != nil {
					log.Fatal(err)
				}
			}
		}(i)
	}
	wg.Wait()
	log.Info(time.Since(ct), cnt)
}
