package rpc

import (
	"bufio"
	"net"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/ngaut/log"
)

type Client struct {
	addr         string
	mu           sync.Mutex
	ongoingCalls map[int64]*clientCall
	callID       int64
	conn         net.Conn
	wc           chan *clientCall
	wr           *bufio.Writer
	rdr          *bufio.Reader
}

type clientCall struct {
	call
	rc chan *resp
}

func NewClient(addr string) *Client {
	return &Client{
		addr:         addr,
		wc:           make(chan *clientCall, 100),
		ongoingCalls: make(map[int64]*clientCall),
	}
}

func (cli *Client) Connect() error {
	conn, err := net.Dial("tcp", cli.addr)
	if err != nil {
		return err
	}
	cli.conn = conn
	cli.wr = bufio.NewWriter(conn)
	cli.rdr = bufio.NewReader(conn)
	go cli.writeLoop()
	go cli.readLoop()
	return nil
}

func (cli *Client) Close() {
	if cli.conn != nil {
		cli.conn.Close()
		cli.conn = nil
	}
}

func (cli *Client) readLoop() {
	defer cli.Close()
	for {
		resp, err := unmarshalResp(cli.rdr)
		if err != nil {
			log.Error(err)
			return
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
	defer cli.Close()
	for {
		select {
		case call := <-cli.wc:
			_, err := cli.wr.Write(call.marshal())
			if err != nil {
				log.Error(err)
				return
			}
			if len(cli.wc) == 0 {
				if err := cli.wr.Flush(); err != nil {
					log.Error(err)
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
	c := &clientCall{
		call: call{
			reqID:       id,
			serviceName: parts[0],
			methodName:  parts[1],
			param:       param,
		},
		rc: respCh,
	}
	cli.mu.Lock()
	cli.ongoingCalls[id] = c
	cli.mu.Unlock()
	cli.wc <- c
	resp := <-c.rc
	return resp.result, resp.err
}
