package main

// Job is designed for a table distributing union, handle all distribution logic.
// each job has own senderlist and readerlist.


// there mainly serveral types of goroutings, like reader, sender, gothrough, job
// reader: read the file to chunk structure, there maybe multiple reader for a
//         large file (configurable) to speedup read, expecially for data read
//         bottleneck
// sender: call the pg copy function call, hold for copy finish
// gothrough: read the data basket chain and send data to sender by shared buffer
// job: the table related work holder
//
// the reader read the file data by chunk, examine and parse the tuple by newline,
// find the distributed field of the tuple, and run hash function to compute the
// hash index to determine the node index, setup a bulk of tuple to a basket, and
// send the basket to data chain per node(the basket is mainly used to decrease
// lock on the chain), gothrough goroutine check the data basket chain, and send
// data to sender.
//
// each reader start from a offset from a file, it maybe the middle of the tuple
// line, or end of the reader will left some data uncomplete tuple, just just
// handle it by head and tail, and rejoin them at last.



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
	displayName string
}

func (this *Job) process() {
	logger.Info("%s start...", this.displayName)
	recordFile := this.tableinfo.datapath
	fd, err := os.Open(recordFile)
	if err != nil {
		logger.Fatal("job process fail for", err)
	}
	defer fd.Close()

	// setup sender connections
	for i, _ := range g_dbinfos {
		sender := NewSender(&g_dbinfos[i], i)
		sender.SetTable(this.tableinfo.schema, this.tableinfo.name, this.tableinfo.columns...)
		sender.PrepareCopyTransaction()
		this.senderlist = append(this.senderlist, sender)
	}
	
	// start reader goroutines
	for i:=0; i<g_readernum; i++ {
		this.rwg.Add(1)
		r := NewReader(i, &this.rwg, this.nodedq, this.remainHolder)
		r.setPartitionField(this.tableinfo.partitionField)
		r.startReader(this.chunks, i, fd)
		this.readerlist = append(this.readerlist, r)
	}
	
	// start sender goroutines
	for i:=0; i<len(this.senderlist); i++ {
		sender := this.senderlist[i]
		sender.StartBackend(&this.swg)
	}

	this.GoThroughDataQueue()

	// wait for all own reader goroutine fninish
	this.rwg.Wait()

	// when the reading work is done, check the chunk header and tail data,
	// analyze them and try to join them all
	this.AnalyzeChunkHeadAndTail()
	this.FinishAllReadWork()
	
	this.WaitSendersStop()

	// wait for all sender work done
	this.swg.Wait()

	// wait for go through gorotine work down
	this.gwg.Wait()

	// wait for job work down, currently not actually used
	this.jwg.Done()
	
	logger.Info("%s end...", this.displayName)
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
		displayName: fmt.Sprintf("job[%d]-%s", index, tinfo.name),
	}

	for i:=0; i<g_slice_num; i++ {
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


// since multiple reader case, a reader can start at any place of a file,
// maybe in the middle of a line, so we just ignore the data before first
// newline as "head", the same as the tail, after all the reader work is
// done, collect head an tail for all readers, and join then as a valid
// record again, and send it to copy reader(simulate a new basket)
func (this *Job) AnalyzeChunkHeadAndTail() {
	var count = g_readernum
	var remainTuples = make([]string, 0)
	var tuple, frontpart, endpart string

	if count < 1 {
		logger.Fatal("AnalyzeChunkHeadAndTail should not accept the count less than 1")
	}
	
	if this.remainHolder == nil {
		logger.Warn("chunk remainer holder is None")
		return 
	}

	for i := 0; i < count; i++ {
		// if it is the first reader, the head is a valid tuple
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
		s := GetFieldByIndex(bytetuple, this.tableinfo.partitionField, 1)
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


// go through the data chain or queue, and create a gorouting go send basket
// data to copy data sender gorouting for each node
func (this *Job) GoThroughDataQueue() {
	for i:=0; i<g_slice_num; i++ {
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
					logger.Debug("%s meet the last basket", this.displayName)
					break
				}
			}
			s.w.Close()
			this.gwg.Done()
		}(i)
	}
}

// send data to the sender's shutdown chan, hope it can stop, and this
// operation is considered not a in hurry action, since the sender will
// finish the copy work before it shutdown, and operaiton more like
// indicator to indicat the sender can exit after copy (it is a little
// tricky control, sender also can quit whenever copy is down), but anyway
// it is a old design but not much problem here.
func (this *Job) WaitSendersStop() {
	for i:=0; i<len(this.senderlist); i++ {
		this.senderlist[i].shutdown <- 0
	}
}


// put a final basket to the data chain, indicate there is no data anymore,
// and the sender can send a EOF to copy data reader
func (this *Job) FinishAllReadWork() {
	for i:=0; i<g_slice_num; i++ {
		b := NewTupleBasket()
		b.last = true
		this.nodedq[i].putQ(b)
	}
}
