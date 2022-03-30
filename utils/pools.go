package utils

import (
	"bytes"
	"net/http"
	"sync"
)

type BytesBufferPool struct {
	p sync.Pool
}

func (bbp *BytesBufferPool) Get() *bytes.Buffer {
	bbv := bbp.p.Get()
	if bbv == nil {
		return &bytes.Buffer{}
	}
	return bbv.(*bytes.Buffer)
}

func (bbp *BytesBufferPool) Put(bb *bytes.Buffer) {
	bb.Reset()
	bbp.p.Put(bb)
}

type HeaderPool struct {
	p sync.Pool
}

func (hdp *HeaderPool) Get() http.Header {
	hdv := hdp.p.Get()
	if hdv == nil {
		return make(http.Header)
	}
	return hdv.(http.Header)
}

func (hdp *HeaderPool) Put(hdv http.Header) {
	for key := range hdv {
		delete(hdv, key)
	}
	hdp.p.Put(hdv)
}

type PrepareSlicePool struct {
	p sync.Pool
}

func (psp *PrepareSlicePool) Get() *[]interface{} {
	psv := psp.p.Get()
	if psv == nil {
		return &[]interface{}{}
	}
	return psv.(*[]interface{})
}

func (psp *PrepareSlicePool) Put(ps *[]interface{}) {
	*ps = (*ps)[:0]
	psp.p.Put(ps)
}
