package rpc

import (
	"bufio"
	"errors"
	"net"
	"sync"

	"github.com/ngaut/log"
)

type Callback struct {
	reqID int64
	rc    chan *resp
}

func (c Callback) Done(r []byte, err error) {
	c.rc <- &resp{
		reqID:  c.reqID,
		result: r,
		err:    err,
	}
}

type Handler interface {
	Handle(method string, param []byte, done Callback)
}

type Server struct {
	mu       sync.Mutex
	services map[string]Handler
}

func NewServer() *Server {
	return &Server{
		services: make(map[string]Handler),
	}
}

func (s *Server) Register(serviceName string, h Handler) Handler {
	s.mu.Lock()
	defer s.mu.Unlock()
	if old, ok := s.services[serviceName]; ok {
		s.services[serviceName] = h
		return old
	}
	s.services[serviceName] = h
	return nil
}

func (s *Server) handle(c net.Conn) {
	rdr := bufio.NewReader(c)
	wr := bufio.NewWriter(c)
	respChan := make(chan *resp, 10)
	go func() {
		for {
			select {
			case r := <-respChan:
				_, err := wr.Write(r.marshal())
				if err != nil {
					log.Warn(err)
					return
				}
				if len(respChan) == 0 {
					err := wr.Flush()
					if err != nil {
						log.Warn(err)
						return
					}
				}
			}
		}
	}()
	for {
		call, err := unmarshalCall(rdr)
		if err != nil {
			log.Warn(err)
			break
		}
		s.mu.Lock()
		handler, ok := s.services[call.serviceName]
		s.mu.Unlock()
		cb := Callback{
			reqID: call.reqID,
			rc:    respChan,
		}
		if !ok {
			cb.Done(nil, errors.New("no such service"))
		} else {
			go handler.Handle(call.methodName, call.param, cb)
		}
	}
	c.Close()
}

func (s *Server) ListenAndServe(addr string) error {
	l, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	for {
		c, err := l.Accept()
		if err != nil {
			log.Error(err)
			continue
		}
		go s.handle(c)
	}
}
