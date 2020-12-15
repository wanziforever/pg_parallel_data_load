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
	partitionFieldType string
	partitionField int
	schema string
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
	schema string

	db *pgconn.PgConn
	count int
	wg *sync.WaitGroup
	buffer []byte
	r *io.PipeReader
	w *io.PipeWriter
	datachan *bytes.Buffer
	name string

	remainder int
}

func (this *Sender) SetTable(schema string, name string, columns ... string) {
	this.schema = schema
	this.tablename = name
	this.fields = append(this.fields, columns...)
}


// this function will hang until the copy function call finish, so it should
// be run in a goroutine
func (this *Sender) Run() {
	logger.Debug("%s run enter", this.name)
	
	// ctx, _ := context.WithTimeout(context.Background(), 1000*time.Second)
        ctx := context.Background();

	_, err := this.db.CopyFrom(ctx, this.r,
		CopyIn(this.remainder, this.schema, this.tablename, this.fields...))
	if err != nil {
		logger.Fatal(err.Error())
	}

	logger.Info("%s data has been copied", this.name)

	// actually, the copy function call will return only when the copy work
	// is done (receive a EOF sign), and then going to monitor the shutdown
	// chan, so if when a goroutine send data to shutdown chan, it will not
	// break, until the copy finish, and this is desiered purpose.
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

// close the connection to pg
func (this *Sender) FinishWork() {
	logger.Debug("meet shutdown")
	ctx, _ := context.WithTimeout(context.Background(), 1000*time.Millisecond)
	this.db.Close(ctx)
	if this.wg != nil {
		this.wg.Done()
	}
}

// setup database connection
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
		remainder: dbi.remainder,
		//buffer:  make([]byte, 0, ciBufferSize),
		name: fmt.Sprintf("Sender-%d", index),
	}
}

// setup the copyin comamnd
func CopyIn(remainder int, schema string, table string, columns...string) string {
	statement := fmt.Sprintf("copy %s.%s (", schema, table)
	for i, col := range columns {
		if i != 0 {
			statement += ", "
		}
		statement += col
	}
	statement += ") FROM STDIN WITH CSV"
	if g_has_csv_header {
		statement += " header"
	}
	if len(g_encoding) > 0 {
		statement += " encoding '" + g_encoding + "'"
	}
	statement += " NULL AS 'NULL'"
	logger.Info(statement)
	return statement
}

func QuoteIdentifier(name string) string {
	end := strings.IndexRune(name, 0)
	if end > -1 {
		name = name[:end]
	}
	return `"` + strings.Replace(name, `"`, `""`, -1) + `"`
}
