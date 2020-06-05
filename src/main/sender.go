package main

import (
	"strings"
	"fmt"
	"sync"
	"github.com/jackc/pgconn"
	"context"
	"io"
	"time"
	"bytes"
)


type TableInfo struct {
	name string
	columns []string
	datapath string
	partitionField int
}

type DBInfo struct {
	host string
	port int
	dbname string
	user string
	password string
	remainder int
	schema string
}

func (this DBInfo) MakeConnectionString() (string) {
	return fmt.Sprintf(
		"host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		this.host, this.port, this.user, this.password, this.dbname)
}

type Sender struct {
	dbi *DBInfo
	index int
	c chan []byte
	shutdown chan int
	tablename string
	fields []string

	db *pgconn.PgConn
	count int
	wg *sync.WaitGroup
	buffer []byte
	r *io.PipeReader
	w *io.PipeWriter
	datachan *bytes.Buffer
	name string
}

func (this *Sender) SetTable(name string, columns ... string) {
	this.tablename = name
	this.fields = append(this.fields, columns...)
}

func (this *Sender) Run() {
	logger.Debug("%s run enter", this.name)
	
	ctx, _ := context.WithTimeout(context.Background(), 1000*time.Second)

	_, err := this.db.CopyFrom(ctx, this.r, CopyIn(this.tablename, this.fields...))
	if err != nil {
		logger.Fatal(err.Error())
	}

	logger.Info("%s data has been copied", this.name)
loop:
	for {
		select {
		case s := <-this.c:
			this.Send(s)
		case <-this.shutdown:
			break loop
		}
	}
	this.FinishWork()
	logger.Info("sender work done for [%d]", this.index)
}

func (this *Sender) FinishWork() {
	logger.Debug("meet shutdown")
	ctx, _ := context.WithTimeout(context.Background(), 1000*time.Millisecond)
	this.db.Close(ctx)
	if this.wg != nil {
		this.wg.Done()
	}
}


func (this *Sender) PrepareCopyTransaction() {
	connstr := this.dbi.MakeConnectionString()
	logger.Info("going to connect %s", connstr)
	db, err := pgconn.Connect(context.Background(), connstr)
	if err != nil {
		logger.Fatal(err.Error())
	}
	this.db = db
}


func (this *Sender) Send(s []byte) {
	_, err := this.w.Write(s)
	if err != nil {
		logger.Fatal(err.Error())
	}
	this.count++
}


func (this *Sender) StartBackend(wg *sync.WaitGroup) {
	logger.Debug("sender backend start")
	this.c = make(chan []byte)
	this.shutdown = make(chan int)
	this.wg = wg
	this.wg.Add(1)
	this.buffer = append(this.buffer, 'd', 0, 0, 0, 0) // for binary copy only
	this.r, this.w = io.Pipe()
	go this.Run()
}


func NewSender(dbi *DBInfo, index int) (*Sender) {
	return &Sender{
		dbi: dbi,
		index: index,
		//buffer:  make([]byte, 0, ciBufferSize),
		name: fmt.Sprintf("Sender-%d", index),
	}
}


func QuoteIdentifier(name string) string {
	end := strings.IndexRune(name, 0)
	if end > -1 {
		name = name[:end]
	}
	return `"` + strings.Replace(name, `"`, `""`, -1) + `"`
}


func CopyIn(table string, columns...string) string {
	statement := "COPY " + table + " ("
	for i, col := range columns {
		if i != 0 {
			statement += ", "
		}
		statement += col
	}
	statement += ") FROM STDIN WITH (format csv)"
	return statement
}
