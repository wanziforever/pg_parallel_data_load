package main


// support multiple reader for each data file, use the chunk size to divide
// the reader start offset and end point, each reader can read the data in
// parallel

// #cgo CFLAGS: -g -Wall
// #cgo LDFLAGS: -L./ -lhash
// #include "hashfunc.h"
import "C"

import (
	"os"
	"io"
	"sync"
	"bytes"
	"time"
	"strconv"
)

var (
	Delim = ','
	g_BasketTupleSize = 4 * 1024 * 1024 // bytes number M
	g_DataQueueSize = 50
	crholder *ChunkRemainHolder = nil
)

type ChunkRemainer struct {
	index int
	head string
	tail string
}


type ChunkRemainHolder struct {
	holders []*ChunkRemainer
}


func NewChunkRemainHolder(count int) (*ChunkRemainHolder) {
	var crholder = &ChunkRemainHolder{}
	crholder.holders = make([]*ChunkRemainer, count)
	return crholder
}


func (this *ChunkRemainHolder) SetRemain(index int, head string, tail string) {
	this.holders[index] = &ChunkRemainer{index, head, tail}
}


func NewReader(
	i int,
	rwg *sync.WaitGroup,
	nodedq []*DataQueue,
	remainHolder *ChunkRemainHolder) (*Reader) {

	r := new(Reader)
	r.processMaxLineLimited = g_maxtuplechunk/int64(g_readernum)
	r.count = 0
	r.index = i
	r.rwg = rwg
	r.baskets = make([]*TupleBasket, g_slice_num)
	for i:=0; i<g_slice_num; i++ {
		r.baskets[i] = NewTupleBasket()
	}
	r.nodedq = nodedq
	r.remainHolder = remainHolder
	
	return r
}

type Reader struct {
	handlecount int64
	processMaxLineLimited int64
	count int64
	baskets []*TupleBasket
	basketcount int
	nodedq []*DataQueue
	rwg *sync.WaitGroup
	remainHolder *ChunkRemainHolder
	index int
	partitionField int
}

func (this *Reader) setPartitionField(index int) {
	this.partitionField = index
}

func (this *Reader) putTupleToBasket(nodeid int, data []byte) {
	b := this.baskets[nodeid]
	// actually need firstly look at the basket exist
	b.Write(data)

	// not exactly same size as the basket limitation
	if b.Len() >= g_BasketTupleSize {
		for {
			if this.nodedq[nodeid].size() < g_DataQueueSize {
				break
			}
			//logger.Info("too many basket unhandled(%d) ...", DataQueueSize)
			time.Sleep(100*time.Millisecond)
		}
		this.nodedq[nodeid].putQ(b)
		this.baskets[nodeid] = NewTupleBasket()
		this.basketcount ++
	}
}


func (this *Reader) upLoadAllBasket() {
	// in case the basket is not full, and send it to data queue
	// usually called at the end of the read
	logger.Info("upload all the basket for readers")
	for i:=0; i<g_slice_num; i++ {
		b := this.baskets[i]
		this.nodedq[i].putQ(b)
	}
}


func (this *Reader) startReader(chunksizes []*Chunk, i int,	fd *os.File) {
	go this.Run(chunksizes, i, fd)
}

// all the dirty
func (this *Reader) Run(chunksizes []*Chunk, i int, fd *os.File) {
	// also need to check the offset and file size
	chunk := chunksizes[i]
	// maybe we can just don't need to read much data at the first time
	buffer := make([]byte, chunk.bufsize)
	var head, tail string
	var end = false
	bytelen, err := fd.ReadAt(buffer, chunk.offset)
	if err != nil {
		if err == io.EOF {
			// do nothing at this point
		} else {
			logger.Error("reader: fail to readat %d", chunk.offset)
			logger.Fatal(err.Error())
		}
	}
	_ = end
	_ = bytelen
	pos := ReadSlice(buffer, '\n')
	if pos < 0 {
		logger.Error("bufsize too small, no newline found in first buffer read")
		logger.Error("suggest larger the bufsize")
		logger.Fatal("")
	}
	
	head = string(buffer[:pos+1])

	offset := chunk.offset + int64(pos) + 1

	targetOffset := chunk.offset + chunk.chunksize

	var lastBuffer bool = false
	_ = lastBuffer
	remain := 0
	var bufSlice []byte

	left := chunk.chunksize - int64(pos) - 1
	
	if left < int64(chunk.bufsize) {
		bufSlice = buffer[:left]
	} else {
		bufSlice = buffer
	}
	
mainloop:
	for end != true {
		bytesread, err := fd.ReadAt(bufSlice, offset)
		if err != nil {
			if err == io.EOF {
				end = true
			}
			if bytesread == 0 {
				break
			}
		}
		
		actualLen := bytesread + remain
		start := 0
		for {
			l := ReadSlice(buffer[start:actualLen], '\n')
			if l < 0 {
				break
			}
			
			s := GetFieldByIndex(buffer[start:start+l+1], this.partitionField, l)
			if len(s) == 0 {
				logger.Error(string(buffer[start:start+l+1]))
				logger.Fatal("fail to parse the field by index")
			}
			
			key, err := strconv.Atoi(string(s))
			if err != nil {
				logger.Fatal(err.Error())
			}
			mod := C.int(g_slice_num)
			
			size := C.get_matching_hash_bounds_int(C.int(key), mod)

			this.putTupleToBasket(int(size), buffer[start:start+l+1])
			this.count++
			if this.processMaxLineLimited != 0 && this.count >= this.processMaxLineLimited {
				logger.Info("reader[%d] reach the max tuple limit %d", i, this.processMaxLineLimited)
				end = true
				break mainloop
			}
			
			start += l + 1
			this.handlecount++
		}

		remain = actualLen - start
		
		p := start
		for i:=0; i<remain; i++ {
			buffer[i] = buffer[p]
			p++
		}
		if lastBuffer == true {
			tail = string(buffer[:remain])
			break
		}

		bufSlice = buffer[remain:]

		offset += int64(bytesread)
		if int64(offset) == targetOffset {
			tail = string(buffer[:remain])
			break
		}
		
		if targetOffset - int64(offset) < int64(chunk.bufsize) {
			lastBuffer = true
			bufSlice = buffer[remain:int64(remain)+targetOffset-offset]
		}
	}
	this.upLoadAllBasket()

	this.remainHolder.SetRemain(i, head, tail)
	this.rwg.Done()
}

func ReadSlice(buffer []byte, delim byte) (pos int) {
	// if i == -1, means not found the delim
	return bytes.IndexByte(buffer, delim)
}

func GetFieldByIndex(c []byte, index int, max int) ([]byte) {
	// currently max is not used

	var r = make([]byte, 0)
	for i:=0; i<index; i++ {
		r = GetNextField(c, byte(Delim)) // actually here need to define a delim
		c = c[len(r)+1:]
	}
	return r
}

// lengthNL reports the number of bytes for the trailing \n.
func lengthNL(b []byte) int {
	if len(b) > 0 && b[len(b)-1] == '\n' {
		return 1
	}
	return 0
}

func GetNextField(c []byte, delim byte) ([]byte) {
	var field = make([]byte, 0)
	if c[0] != '"' {
		i := bytes.IndexByte(c, delim)
		field = c
		if i >= 0 {
			field = field[:i]
		} else {
			field = field[:len(field) - lengthNL(field)]
		}
	} else {
		c = c[1:]
		i := bytes.IndexByte(c, '"')
		if i >= 0 {
			field = c[:i]
		} else {
			// currently we only support oneline tuple, so we normally should
			// find the paired sing
			logger.Fatal("cannot find pair double mark")
		}
	}
	return field
}
