package rpc

import (
	"bytes"
	"testing"
)

func TestCallMarshalAndUnmarshal(t *testing.T) {
	c := &call{
		reqID:       1024,
		serviceName: "a",
		methodName:  "a",
		param:       []byte("111"),
	}
	b := c.marshal()
	if len(b) == 0 {
		t.Error("marshal error")
	}
	buf := bytes.NewBuffer(b)
	newC, err := unmarshalCall(buf)
	if err != nil {
		t.Error(err)
	}
	if newC.reqID != c.reqID || len(newC.param) != len(c.param) {
		t.Error(newC)
	}
}
