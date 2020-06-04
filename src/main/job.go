package main

// #cgo CFLAGS: -g -Wall
// #cgo LDFLAGS: -L./ -lhash
// #include "hashfunc.h"
import "C"
import (
	"sync"
	"os"
	"fmt"
	"strconv"
	"time"
	"io"
)

type Chunk struct {
	chunksize int64
	bufsize int
	offset int64
}

type Job struct {
	rwg sync.WaitGroup
	swg sync.WaitGroup
	gwg sync.WaitGroup
	jwg *sync.WaitGroup
	senderlist []*Sender
	readerlist []*Reader
	nodedq []*DataQueue
	chunks []*Chunk
	tableinfo *TableInfo
	remainHolder *ChunkRemainHolder
	filesize int64
	jobid int
}

func (this *Job) process() {
	logger.Debug("the %d(th) job start to process...", this.jobid)
	recordFile := this.tableinfo.datapath
	fd, err := os.Open(recordFile)
	if err != nil {
		logger.Fatal("processParallel function fail for", err)
	}
	defer fd.Close()

	// setup sender connections
	for i, _ := range g_dbinfos {
		sender := NewSender(&g_dbinfos[i], i)
		sender.SetTable(this.tableinfo.name, this.tableinfo.columns...)
		sender.PrepareCopyTransaction()
		this.senderlist = append(this.senderlist, sender)
	}
	
	// start readers
	for i:=0; i<g_readernum; i++ {
		this.rwg.Add(1)
		r := NewReader(i, &this.rwg, this.nodedq, this.remainHolder)
		r.setPartitionField(this.tableinfo.partitionField)
		r.startReader(this.chunks, i, fd)
		this.readerlist = append(this.readerlist, r)
	}
	
	// start sender
	for i:=0; i<len(this.senderlist); i++ {
		sender := this.senderlist[i]
		sender.StartBackend(&this.swg)
	}

	this.GoThroughDataQueue()

	this.rwg.Wait()
	this.AnalyzeChunkHeadAndTail()
	this.FinishAllReadWork()
	this.WaitSendersStop()
	this.swg.Wait()
	this.gwg.Wait()
	this.jwg.Done()
	
	logger.Debug("the %d(th) job end...", this.jobid)
}

func (this *Job) validate() {
	
}


func (this *Job) makeChunks() {
	chunksize := ((this.filesize-1) / int64(g_readernum)) + 1
	left := this.filesize
	for i := 0; i < g_readernum; i++ {
		chunk := new(Chunk)
		if left > chunksize {
			chunk.chunksize = chunksize
		} else {
			chunk.chunksize = left
		}
		left -= chunksize
		chunk.bufsize = g_bufsize
		chunk.offset = chunksize * int64(i)
		this.chunks = append(this.chunks, chunk)
	}
}


func NewJob(index int, tinfo *TableInfo, jwg *sync.WaitGroup) *Job {
	logger.Debug("makeing the %dth new job with table %s", index, tinfo.name)
	j := &Job{
		senderlist: make([]*Sender, 0),
		readerlist: make([]*Reader, 0),
		nodedq: make([]*DataQueue, 0),
		chunks: make([]*Chunk, 0),
		tableinfo: tinfo,
		remainHolder: NewChunkRemainHolder(g_readernum),
		jobid: index,
		jwg: jwg,
	}

	for i:=0; i<g_nodenum; i++ {
		j.nodedq = append(j.nodedq, NewDataQueue())
	}
	datafile := j.tableinfo.datapath
	f, err := os.Stat(datafile)
	if err != nil {
		fmt.Println("fail to open", datafile)
		os.Exit(1)
	}

	j.filesize = f.Size()
	j.makeChunks()
	return j
}

func (this *Job) AnalyzeChunkHeadAndTail() {
	var count = g_readernum
	var remainTuples = make([]string, 0)
	var tuple, frontpart, endpart string

	if count < 1 {
		logger.Fatal("AnalyzeChunkHeadAndTail should not accept the count less than 1")
	}
	
	if this.remainHolder == nil {
		logger.Info("chunk remainer holder is None")
		return 
	}

	for i := 0; i < count; i++ {
		if i == 0 {
			tuple = this.remainHolder.holders[i].head
			remainTuples = append(remainTuples, tuple)
			frontpart = this.remainHolder.holders[i].tail
			continue
		}

		if i == count -1 {
			if len(this.remainHolder.holders[i].tail) != 0 {
				logger.Fatal("the last chunk should not have a tail, but %s",
					this.remainHolder.holders[i].tail)
			}
		}

		endpart = this.remainHolder.holders[i].head
		tuple = frontpart + endpart
		remainTuples = append(remainTuples, tuple)
		frontpart = this.remainHolder.holders[i].tail
	}

	for _, tuple = range remainTuples {
		bytetuple := []byte(tuple)
		s := GetFieldByIndex(bytetuple, 1, 1)
		if len(s) == 0 {
			logger.Fatal("fail to parse the field by index")
		}

		key, err := strconv.Atoi(string(s))
		if err != nil {
			logger.Fatal(err.Error())
		}
		mod := C.int(len(this.senderlist))

		size := C.get_matching_hash_bounds_int(C.int(key), mod)
		b := NewTupleBasket()
		b.Write(bytetuple)
		this.nodedq[int(size)].putQ(b)
	}
}

func (this *Job) GoThroughDataQueue() {
	for i:=0; i<g_nodenum; i++ {
		this.gwg.Add(1)
		go func(i int) {
			q := this.nodedq[i]
			s := this.senderlist[i]
			for {
				b := q.popQ()
				if b == nil {
					time.Sleep(time.Duration(10)*time.Millisecond)
					continue
				}

				buf := make([]byte, 2048)
				for {
					n, err := b.Read(buf)
					if err == io.EOF {
						break
					}

					s.w.Write(buf[:n])
				}
				if b.last == true {
					break
				}
			}
			s.w.Close()
			this.gwg.Done()
		}(i)
	}
}

func (this *Job) WaitSendersStop() {
	for i:=0; i<len(this.senderlist); i++ {
		this.senderlist[i].shutdown <- 0
	}
}

func (this *Job) FinishAllReadWork() {
	for i:=0; i<g_nodenum; i++ {
		b := NewTupleBasket()
		b.last = true
		this.nodedq[i].putQ(b)
	}
}
