package rpc

import (
	"bufio"
	"errors"
	"net"
	"strings"
	"sync"
	"sync/atomic"
)

type Client struct {
	addr         string
	mu           sync.Mutex
	ongoingCalls map[int64]*clientCall
	callID       int64
	conn         net.Conn
	wc           chan *clientCall
	workers      sync.WaitGroup
}

type clientCall struct {
	call
	rc chan *resp
}

func NewClient(addr string) *Client {
	return &Client{
		addr:         addr,
		wc:           make(chan *clientCall, 20),
		ongoingCalls: make(map[int64]*clientCall),
	}
}

func (cli *Client) Connect() error {
	cli.mu.Lock()
	defer cli.mu.Unlock()
	conn, err := net.Dial("tcp", cli.addr)
	if err != nil {
		return err
	}
	cli.conn = conn
	go cli.writeLoop()
	go cli.readLoop()
	return nil
}

func (cli *Client) Reset() {
	cli.mu.Lock()
	defer cli.mu.Unlock()
	if cli.conn != nil {
		cli.conn.Close()
		cli.conn = nil
	}
	cli.callID = 0
	cli.ongoingCalls = make(map[int64]*clientCall)
}

func (cli *Client) readLoop() {
	defer cli.Reset()
	cli.mu.Lock()
	if cli.conn == nil {
		cli.mu.Unlock()
		return
	}
	rdr := bufio.NewReader(cli.conn)
	cli.mu.Unlock()
	for {
		resp, err := unmarshalResp(rdr)
		if err != nil {
			break
		}
		cli.mu.Lock()
		call, ok := cli.ongoingCalls[resp.reqID]
		cli.mu.Unlock()
		if !ok {
			// if no such call, just discard it
			continue
		}
		call.rc <- resp
	}
}

func (cli *Client) writeLoop() {
	defer cli.Reset()
	cli.mu.Lock()
	if cli.conn == nil {
		cli.mu.Unlock()
		return
	}
	wr := bufio.NewWriter(cli.conn)
	cli.mu.Unlock()
	for {
		select {
		case call := <-cli.wc:
			cli.mu.Lock()
			cli.ongoingCalls[call.reqID] = call
			cli.mu.Unlock()
			_, err := wr.Write(call.marshal())
			if err != nil {
				break
			}
			if len(cli.wc) == 0 {
				if err := wr.Flush(); err != nil {
					return
				}
			}
		}
	}
}

func (cli *Client) Call(signature string, param []byte) ([]byte, error) {
	id := atomic.AddInt64(&cli.callID, 1)
	parts := strings.Split(signature, ".")
	respCh := make(chan *resp)
	call := &clientCall{
		call: call{
			reqID:       id,
			serviceName: parts[0],
			methodName:  parts[1],
			param:       param,
		},
		rc: respCh,
	}
	cli.mu.Lock()
	if cli.conn == nil {
		cli.mu.Unlock()
		return nil, errors.New("connection is not established")
	}
	cli.mu.Unlock()
	cli.wc <- call
	resp := <-call.rc
	return resp.result, resp.err
}
