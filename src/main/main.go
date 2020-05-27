package main

import (
	"fmt"
	"log"
	"sync"
	"os"
	"time"
	"loadconfig"
	"io"
)

var (
	g_bufsize = 1 * 1024 * 1024 // M default
	g_configfile string
	sd chan int = make(chan int)
	MAX_MODE = 100
	g_readernum = 1 // default
	g_nodenum = 0
	g_datafile = ""
	g_tablename = ""
	g_filesize = int64(0)
	g_chunks = make([]*Chunk, 0)
	g_maxtuplechunk int64 = 0
	g_shardingBuffer [][][]byte
	nodedq []*DataQueue
	g_dbinfos []DBInfo
)


var rwg sync.WaitGroup
var swg sync.WaitGroup
var gwg sync.WaitGroup

var senderlist []*Sender
var readerlist []*Reader

type NodeChan struct {
	data *chan []byte
	control *chan int
}


func GoThroughDataQueue() {
	for i:=0; i<g_nodenum; i++ {
		gwg.Add(1)
		go func(i int) {
			log.Println("Go through data queue")
			q := nodedq[i]
			s := senderlist[i]
			for {
				b := q.popQ()
				if b == nil {
					//log.Printf("through[%d] no basket found sleep for a well\n", i)
					time.Sleep(time.Duration(10)*time.Millisecond)
					continue
				}

				//log.Printf("gothrough[%d] swtich basket\n", i)
				buf := make([]byte, 2048)
				for {
					n, err := b.Read(buf)
					if err == io.EOF {
						break
					}

					//log.Printf("gothrough queue [%d], %s\n", i, string(t.data()))
					s.w.Write(buf[:n])
				}
				if b.last == true {
					break
				}
			}
			s.w.Close()
			//log.Printf("gothrough[%d] finish\n", i)
			gwg.Done()
		}(i)
	}
}


type Chunk struct {
	chunksize int64
	bufsize int
	offset int64
}

func StartSenders(conf *loadconfig.Config) {
	log.Println("StartSenders start...")
	for i, n := range conf.Nodes {
		sender := NewSender(n.Host, n.Port, conf.User,
			conf.Password, conf.Dbname, i)
		// currently hard code the table structure
		//sender.SetTable(
		//	"bmsql_item",
		//	"i_id",
		//	"i_name",
		//	"i_price",
		//	"i_data",
		//	"i_im_id")
		sender.StartBackend(&swg)
		//nc.SetNodeChan(i, NodeChan{c, s})
		senderlist = append(senderlist, sender)
	}
}

func WaitSendersStop() {
	for i:=0;i<len(senderlist);i++ {
		senderlist[i].shutdown <- 0
	}
	swg.Wait()
}

func loadConfig(configFile string) (*loadconfig.Config) {
	if g_configfile == "" {
		log.Println("configuration file not provided ..")
		os.Exit(1)
	}
	
	conf, err := loadconfig.ReadConfigData(g_configfile)
	if err != nil {
		log.Fatal(err)
	}
	return conf
}


func sysinit(conf *loadconfig.Config) {
	g_bufsize = conf.Buffersize * 1024 * 1024
	g_readernum = conf.Readers
	g_nodenum = len(conf.Nodes)
	g_shardingBuffer = make([][][]byte, g_nodenum)
	g_datafile = fmt.Sprintf("%s", conf.Datapath)
	g_tablename = conf.TableName
	f, err := os.Stat(g_datafile)
	if err != nil {
		fmt.Println("fail to open", g_datafile)
		os.Exit(1)
	}

	g_filesize = f.Size()
	g_maxtuplechunk = conf.Maxtuplechunk

	nodedq = make([]*DataQueue, g_nodenum)
	for i:=0; i<g_nodenum; i++ {
		nodedq[i] = NewDataQueue()
	}
	g_dbinfos = make([]DBInfo, g_nodenum)
	for i:=0; i<g_nodenum; i++ {
		n := conf.Nodes[i]
		g_dbinfos[i] = DBInfo{
			host: n.Host,
			port: n.Port,
			user: conf.User,
			password: conf.Password,
			dbname: conf.Dbname,
		}
	}

	makeChunks()
}


func showConfigInfo() {
	var info = "\n-----Distributed Database Data Loading Tool-----\n"
	info += fmt.Sprintf("  table name:\t%s\n", g_tablename)
	info += fmt.Sprintf("  node number:\t%d\n", g_nodenum)
	for i:=0; i<g_nodenum; i++ {
		d := g_dbinfos[i]
		info += fmt.Sprintf("    host: %s, port: %d, user: %s, db: %s\n",
			d.host, d.port, d.user, d.dbname)
	}

	buflen, size := sizeConvert(int64(g_bufsize))
	info += fmt.Sprintf("  buffer size:\t%d(%d%s)\n", g_bufsize, buflen, size)

	filelen, size := sizeConvert(g_filesize)
	info += fmt.Sprintf("  data file:\t%s [%d(%d%s)]\n",
		g_datafile, g_filesize, filelen, size)
	info += fmt.Sprintf("  reader numbber: %d\n", g_readernum)


	for i, c := range g_chunks {
		chunk_info := fmt.Sprintf("  chunk[%d] information\n", i)
		clen, size := sizeConvert(c.chunksize)
		csize := fmt.Sprintf("%d%s", clen, size)
		blen, size := sizeConvert(int64(c.bufsize))
		bsize := fmt.Sprintf("%d%s", blen, size)
		chunk_info += fmt.Sprintf("    chunksize:%d(%s), bufsize:%d(%s), offset:%d\n",
			c.chunksize, csize, c.bufsize, bsize, c.offset)
		info += chunk_info
	}
	
	info += "------ end of configuration ------\n"
	fmt.Println(info)
}

func main() {
	if len(os.Args) > 1 {
		g_configfile = os.Args[1]
	}

	conf := loadConfig(g_configfile)

	sysinit(conf)
	showConfigInfo()

	processParallel(conf)

	log.Println("all work done")
}

func makeChunks() {
	log.Println("makeChunks enter ...")
	chunksize := ((g_filesize-1) / int64(g_readernum)) + 1
	left := g_filesize
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
		g_chunks = append(g_chunks, chunk)
	}
	log.Println("makeChunks exit ...")
}


func processParallel(conf *loadconfig.Config) {
	recordFile := conf.Datapath
	fd, err := os.Open(recordFile)
	defer fd.Close()
	if err != nil {
		log.Fatal("processParallel function fail for", err)
	}

	time.Sleep(time.Duration(4)*time.Millisecond)

	
	crholder = NewChunkRemainHolder(g_readernum)

	fmt.Println("process enter to continue...")
	fmt.Scanln()
	fmt.Println("work start ...")

	start := time.Now()

	readerlist = make([]*Reader, 0)
	
	for i:=0; i<g_readernum; i++ {
		rwg.Add(1)
		r := NewReader(g_nodenum, 0)
		r.startReader(g_chunks, i, fd, &rwg)
		readerlist = append(readerlist, r)
	}


	StartSenders(conf)
	GoThroughDataQueue()

	rwg.Wait()
	AnalyzeChunkHeadAndTail(g_readernum)
	FinishAllReadWork()
	
	WaitSendersStop()
	swg.Wait()
	gwg.Wait()

	var total_handlecount int64 = 0
	for i, r := range readerlist {
		log.Printf("reader[%d] handlecount: %d\n", i, r.handlecount)
		total_handlecount += r.handlecount
		log.Printf("basket count %d\n", r.basketcount)
	}
	log.Printf("total handle count is %d\n", total_handlecount)
	log.Println("total execution interval is", time.Since(start))
}

