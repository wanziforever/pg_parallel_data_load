package main

import (
	"sync"
	"bytes"
)


type TupleData struct {
	d []byte
}

func (this *TupleData) data() []byte {
	return this.d
}


// no need to add lock for basket, since the basket access only by one go
type TupleBasket struct {
	//p []*TupleData
	blk *bytes.Buffer
	last bool
	readpos int
}


func (this *TupleBasket) Write(t []byte) {
	this.blk.Write(t)
}

func (this *TupleBasket) Read(d []byte) (n int, err error) {
	return this.blk.Read(d)
}

func (this *TupleBasket) Len() int {
	return this.blk.Len()
}


type DataQueue struct {
	q []*TupleBasket
	mux sync.Mutex
}


func (this *DataQueue) putQ(tb *TupleBasket) {
	this.mux.Lock()
	this.q = append(this.q, tb)
	this.mux.Unlock()
}


func (this *DataQueue) popQ() (*TupleBasket) {
	this.mux.Lock()
	if len(this.q) == 0 {
		this.mux.Unlock()
		return nil
	}
	tb := this.q[0]
	this.q = this.q[1:]
	this.mux.Unlock()
	return tb
}


func (this *DataQueue) size() int {
	return len(this.q)
}


func NewDataQueue() *DataQueue {
	dc := new(DataQueue)
	dc.q = make([]*TupleBasket, 0)
	return dc
}


func NewTupleBasket() (*TupleBasket) {
	tb := new(TupleBasket)
	tb.last = false
	tb.blk = &bytes.Buffer{}
	return tb
}


func NewTupleData(d []byte) (*TupleData) {
	td := new(TupleData)
	td.d = make([]byte, len(d))
	copy(td.d, d)
	return td
}
