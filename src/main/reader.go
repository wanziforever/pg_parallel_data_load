package main

// #cgo CFLAGS: -g -Wall
// #cgo LDFLAGS: -L./ -lhash
// #include "hashfunc.h"
import "C"

import (
	"log"
	"os"
	"io"
	"sync"
	"bytes"
	"strconv"
)

const AnalyzeStepSize = 1024
const Delim = ','
const BasketTupleSize = 2 * 1024 * 1024 // bytes number M
const DataQueueSize = 1000


type ChunkRemainer struct {
	index int
	head string
	tail string
}


type ChunkRemainHolder struct {
	holders []*ChunkRemainer
}


var crholder *ChunkRemainHolder = nil

func NewChunkRemainHolder(count int) (*ChunkRemainHolder) {
	var crholder = &ChunkRemainHolder{}
	crholder.holders = make([]*ChunkRemainer, count)
	//for i:=0; i<count; i++ {
	//	crholder.holders = append(crholder.holders, nil)
	//}
	return crholder
}


func (this *ChunkRemainHolder) SetRemain(index int, head string, tail string) {
	this.holders[index] = &ChunkRemainer{index, head, tail}
}


func NewReader(nodenum int, shardingBufSize int64) (*Reader) {
	r := new(Reader)
	r.processMaxLineLimited = g_maxtuplechunk/int64(g_readernum)
	r.count = 0
	return r
}

type Reader struct {
	handlecount int64
	processMaxLineLimited int64
	count int64
	baskets []*TupleBasket
	basketcount int
}


func (this *Reader) putTupleToBasket(nodeid int, data []byte) {
	b := this.baskets[nodeid]
	// actually need firstly look at the basket exist
	b.Write(data)

	if b.Len() >= BasketTupleSize {
		nodedq[nodeid].putQ(b)
		this.baskets[nodeid] = NewTupleBasket()
		this.basketcount ++
	}
}


func (this *Reader) upLoadAllBasket() {
	log.Println("upload all the basket for readers")
	for i:=0; i<g_nodenum; i++ {
		b := this.baskets[i]
		nodedq[i].putQ(b)
	}
}


func (this *Reader) startReader(
	chunksizes []*Chunk, i int, fd *os.File, wg *sync.WaitGroup) {
	this.baskets = make([]*TupleBasket, g_nodenum)
	for i:=0; i<g_nodenum; i++ {
		this.baskets[i] = NewTupleBasket()
	}
	go this.Run(chunksizes, i, fd, wg)
}


func (this *Reader) Run(
	chunksizes []*Chunk, i int, fd *os.File, wg *sync.WaitGroup) {
	//log.Printf("-----Reader[%d] enter-----\n", i)
	// also need to check the offset and file size
	chunk := chunksizes[i]
	buffer := make([]byte, chunk.bufsize)
	var head, tail string
	var end = false
	bytelen, err := fd.ReadAt(buffer, chunk.offset)
	if err != nil {
		if err == io.EOF {
			// do nothing at this point
		} else {
			log.Printf("reader: fail to readat %d", chunk.offset)
			log.Fatal(err)
		}
	}
	_ = end
	_ = bytelen
	pos := ReadSlice(buffer, '\n')
	if pos < 0 {
		log.Println("bufsize too small, no newline found in first buffer read")
		log.Println("suggest larger the bufsize")
		log.Fatal("")
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
		//log.Println(cap(bufSlice), remain)
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
		//log.Println("actualLen:", actualLen)
		start := 0
		for {
			//log.Println("start:actualLen", start, actualLen)
			l := ReadSlice(buffer[start:actualLen], '\n')
			if l < 0 {
				break
			}
			
			s := GetFieldByIndex(buffer[start:start+l+1], 1, l)
			if len(s) == 0 {
				log.Println(string(buffer[start:start+l+1]))
				log.Fatal("fail to parse the field by index")
			}
			
			key, err := strconv.Atoi(string(s))
			if err != nil {
				log.Fatal(err)
			}
			mod := C.int(len(senderlist))
			
			size := C.get_matching_hash_bounds_int(C.int(key), mod)
			this.putTupleToBasket(int(size), buffer[start:start+l+1])
			this.count++
			if this.processMaxLineLimited != 0 && this.count >= this.processMaxLineLimited {
				log.Printf("reader[%d] reach the max tuple limit %d", i, this.processMaxLineLimited)
				end = true
				break mainloop
			}
			
			start += l + 1
			this.handlecount++
		}

		remain = actualLen - start
		//log.Println("remain, bytesread, start, actualLen:", remain, bytesread, start, actualLen)
		
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
			//log.Println("remain, targetoffset, offset", remain, targetOffset, offset, i)
		}
	}
	this.upLoadAllBasket()

	crholder.SetRemain(i, head, tail)
	wg.Done()
}

func ReadSlice(buffer []byte, delim byte) (pos int) {
	// if i == -1, means not found the delim
	return bytes.IndexByte(buffer, delim)
}

func GetFieldByIndex(c []byte, index int, max int) ([]byte) {
	// currently max is not used
	var r = make([]byte, 0)
	for i:=0; i<index; i++ {
		r = GetNextField(c, ',') // actually here need to define a delim
		c = c[len(r):]
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
			log.Fatal("cannot find pair double mark")
		}
	}
	return field
}


// ignore the first line of the chunk
func AnalyzeChunkHead(c Chunk, fd *os.File) (head int64) {
	for {
		buffer := make([]byte, AnalyzeStepSize)
		start := c.offset
		
		bytesread, err := fd.ReadAt(buffer, start)
		_ = bytesread
		_ = err
	}
}

func AnalyzeChunkHeadAndTail(count int) {
	var remainTuples = make([]string, 0)
	var tuple, frontpart, endpart string

	if count < 1 {
		log.Fatal("AnalyzeChunkHeadAndTail should not accept the count less than 1")
	}
	
	if crholder == nil {
		log.Println("chunk remainer holder is None")
		return 
	}

	for i := 0; i < count; i++ {
		if i == 0 {
			tuple = crholder.holders[i].head
			remainTuples = append(remainTuples, tuple)
			frontpart = crholder.holders[i].tail
			continue
		}

		if i == count -1 {
			if len(crholder.holders[i].tail) != 0 {
				log.Fatal("the last chunk should not have a tail, but %s",
					crholder.holders[i].tail)
			}
		}

		endpart = crholder.holders[i].head
		tuple = frontpart + endpart
		remainTuples = append(remainTuples, tuple)
		frontpart = crholder.holders[i].tail
	}

	for _, tuple = range remainTuples {
		bytetuple := []byte(tuple)
		s := GetFieldByIndex(bytetuple, 1, 1)
		if len(s) == 0 {
			log.Fatal("fail to parse the field by index")
		}

		key, err := strconv.Atoi(string(s))
		if err != nil {
			log.Fatal(err)
		}
		mod := C.int(len(senderlist))

		size := C.get_matching_hash_bounds_int(C.int(key), mod)
		//c := nc.GetNodeChan(int(size))
		//*c.data <- bytetuple
		b := NewTupleBasket()
		b.Write(bytetuple)
		nodedq[int(size)].putQ(b)
	}
}

func FinishAllReadWork() {
	for i:=0; i<g_nodenum; i++ {
		b := NewTupleBasket()
		b.last = true
		nodedq[i].putQ(b)
	}
}
