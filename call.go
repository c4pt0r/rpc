package rpc

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"strings"
)

type multiReader interface {
	ReadByte() (c byte, err error)
	Read(p []byte) (n int, err error)
}

type respType uint8

const (
	respErr respType = iota
	respSuccess
)

// RPC response object
type resp struct {
	reqID  int64
	result []byte
	err    error
}

// Response layout
// +------------------------------------------+
// | req-id  |  type      | len    | response |
// +------------------------------------------+
// | varint  |  uint8     | int32  | ...      |
// +------------------------------------------+
func (r *resp) marshal() []byte {
	idBuf := make([]byte, 8)
	offset := binary.PutVarint(idBuf, r.reqID)
	buf := make([]byte, 0, offset+1+4+len(r.result))
	b := bytes.NewBuffer(buf)
	b.Write(idBuf[0:offset])
	// write result type
	if r.err != nil {
		b.WriteByte(byte(respErr))
		binary.Write(b, binary.LittleEndian, int32(len(r.err.Error())))
		b.Write([]byte(r.err.Error()))
	} else {
		b.WriteByte(byte(respSuccess))
		binary.Write(b, binary.LittleEndian, int32(len(r.result)))
		b.Write(r.result)
	}
	return b.Bytes()
}

func unmarshalResp(r multiReader) (*resp, error) {
	ret := &resp{}
	reqID, err := binary.ReadVarint(r)
	if err != nil {
		return nil, err
	}
	ret.reqID = reqID
	var typ uint8
	err = binary.Read(r, binary.LittleEndian, &typ)
	if err != nil {
		return nil, err
	}
	// read body
	var total int32
	err = binary.Read(r, binary.LittleEndian, &total)
	if err != nil {
		return nil, err
	}
	if total <= 0 {
		return nil, errors.New("invalid response payload")
	}
	buf := make([]byte, total)
	_, err = io.ReadFull(r, buf)
	if err != nil {
		return nil, err
	}
	if respType(typ) == respErr {
		ret.err = errors.New(string(buf))
	} else {
		ret.result = buf
	}
	return ret, nil
}

// RPC request call object
type call struct {
	reqID       int64
	serviceName string
	methodName  string
	param       []byte
}

// Request layout
// +--------------------------------------------------------------+
// | req-id  | total len    | serviceName.methodName | \0 | param |
// +--------------------------------------------------------------+
// | varint  | int32        | ...                    | 1  | ...   |
// +--------------------------------------------------------------+
func (c *call) marshal() []byte {
	signature := c.serviceName + "." + c.methodName
	// put req id
	idBuf := make([]byte, 8)
	offset := binary.PutVarint(idBuf, c.reqID)
	buf := make([]byte, 0, offset+4+len(signature)+1+len(c.param))
	b := bytes.NewBuffer(buf)
	b.Write(idBuf[0:offset])
	// put total length
	binary.Write(b, binary.LittleEndian, int32(len(signature)+1+len(c.param)))
	// put service signiture
	b.Write([]byte(signature))
	b.WriteByte(byte(0))
	// put param
	b.Write(c.param)
	return b.Bytes()
}

// Unmarshal call
func unmarshalCall(r multiReader) (*call, error) {
	ret := &call{}
	reqID, err := binary.ReadVarint(r)
	if err != nil {
		return nil, err
	}
	ret.reqID = reqID
	var total int32
	err = binary.Read(r, binary.LittleEndian, &total)
	if err != nil {
		return nil, err
	}
	if total <= 0 {
		return nil, errors.New("invalid call payload")
	}

	buf := make([]byte, total)
	_, err = io.ReadFull(r, buf)
	if err != nil {
		return nil, err
	}
	var signature string
	var param []byte
	for i, b := range buf {
		if b == byte(0) {
			signature = string(buf[0:i])
			if i+1 <= len(buf) {
				param = buf[i+1 : len(buf)]
			}
			break
		}
	}
	if len(signature) == 0 {
		return nil, errors.New("invalid call signature")
	}
	parts := strings.Split(signature, ".")
	ret.serviceName, ret.methodName = parts[0], parts[1]
	ret.param = param
	return ret, nil
}
