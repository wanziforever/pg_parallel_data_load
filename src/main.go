package main

// #cgo CFLAGS: -g -Wall
// #cgo LDFLAGS: -L./ -lhash
// #include "hashfunc.h"
import "C"
import (
	"fmt"
	"gopkg.in/yaml.v2"
	"log"
	"io/ioutil"
	"os"
	"io"
	"encoding/csv"
	"strings"
	"strconv"
	_ "github.com/lib/pq"
	"database/sql"
	//"time"
//	"net/http"
//	"encoding/json"
//	"strings"
//	"strconv"
//	"runtime"
)

type NetworkNode struct {
	Host string `yaml:"host"`
	Port int `yaml:"port"`
}

type CopyConn struct {
	conn *sql.DB
	txn *sql.Tx
	stmt *sql.Stmt
}

var AllCopyConns []CopyConn

type Config struct {
	Dbname string `yaml:"dbname"`
	User string `yaml:"user"`
	Password string `yaml:"password"`
	TableName string `yaml:"tablename"`
	Datapath string `yaml:"datapath"`
	PartitionField int `yaml:"partitionField"`
	Nodes []NetworkNode `yaml:"nodes"`
}

func ReadConfigData(path string) (*Config, error) {
	var config Config
	source, err := ioutil.ReadFile(path)
	if err != nil {
		panic(err)
	}

	err = yaml.Unmarshal(source, &config)
	return &config, err
}

func main() {
	conf, err := ReadConfigData("h.yml")
	if err != nil {
		log.Fatal(err)
	}
	log.Println(conf)

	ConnectAllNodes(conf)
	SetupTransaction()

	PrepareCopyIn()
	ProcessCSV(conf.Datapath, conf)
	EndCopyIn()
	CloseTransaction()
	CloseAllConns()
}

func QuoteIdentifier(name string) string {
	end := strings.IndexRune(name, 0)
	if end > -1 {
		name = name[:end]
	}
	return `"` + strings.Replace(name, `"`, `""`, -1) + `"`
}

func CopyIn(table string, columns ...string) string {
	stmt := "COPY " + QuoteIdentifier(table) + " ("
	for i, col := range columns {
		if i != 0 {
			stmt += ", "
		}
		stmt += QuoteIdentifier(col)
	}
	stmt += ") FROM STDIN"
	return stmt
}

func SetupTransaction() {
	for i, db := range AllCopyConns {
		txn, err := db.conn.Begin()
		if err != nil {
			log.Fatal(err)
		}
		AllCopyConns[i].txn = txn
	}
}

func CloseTransaction() {
	for _, db := range AllCopyConns {
		if db.txn == nil {
			continue
		}
		db.txn.Commit()
	}
}

func ConnectAllNodes(conf *Config) {
	for _, n := range conf.Nodes {
		connstr := fmt.Sprintf(
			"host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
			n.Host, n.Port, conf.User, conf.Password, conf.Dbname)
		db, err := sql.Open("postgres", connstr)

		if err != nil {
			log.Printf("fail to connect to data node %s:%d\n", n.Host, n.Port)
			log.Fatal(err)
		}
		AllCopyConns = append(AllCopyConns, CopyConn{conn:db})
		log.Println("successfully connect to", connstr)
		//s := "select sum(numbackends) from pg_stat_database;"
		//var sum int
		//row := db.QueryRow(s)
		//switch err := row.Scan(&sum); err {
		//case sql.ErrNoRows:
		//	log.Println("No rows were returned!")
		//case nil:
		//	log.Println("the database connection number is ", sum)
		//default:
		//	log.Println(err)
		//}
	}
}

func PrepareCopyIn() {
	for i, db := range AllCopyConns {
		s := CopyIn("bmsql_item", "i_id", "i_name", "i_price", "i_data", "i_im_id")
		//s := CopyIn("temp")
		stmt, err := db.txn.Prepare(s)
		if err != nil {
			log.Fatal(err)
		}
		AllCopyConns[i].stmt = stmt
	}
	//time.Sleep(time.Duration(2)*time.Second)
}

func EndCopyIn() {
	log.Println("-----------------EndCopyIn------")
	for _, db := range AllCopyConns {
		_, err := db.stmt.Exec()
		if err != nil {
			log.Fatal(err)
		}
	}
}

func CloseAllConns() {
	for _, db := range AllCopyConns {
		err := db.stmt.Close()
		if err != nil {
			log.Fatal(err)
		}
		err = db.conn.Close()
		if err != nil {
			log.Fatal(err)
		}
	}
}

func ProcessCSV(filename string, conf *Config) {
	recordFile, err := os.Open(filename)
	if err != nil {
		log.Fatal(err)
	}
	r := csv.NewReader(recordFile)

	numOfNodes := len(conf.Nodes)
	_ = r
	_ = numOfNodes
	//offical code should consider the comments line
	i := 0
	for {
		rec, err := r.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			log.Fatal(err)
		}
		pkey := rec[conf.PartitionField-1]
		c, err := strconv.Atoi(pkey)
		//log.Println(c)
		
		mod := C.int(numOfNodes)
		size := C.get_matching_hash_bounds_int(C.int(c), mod)
		
		s := make([]interface{}, len(rec))
		for i, v := range rec {
			s[i] = v
		}
		_ = s
		i += 1
		//_, err = AllCopyConns[size].stmt.Exec(1, "asdf", 4.3, "324", 34)
		//_, err = AllCopyConns[size].stmt.Exec(int64(i), "asdf")
		_, err = AllCopyConns[size].stmt.Exec(s...)
		if err != nil {
			log.Fatal(err)
		}
	}


	//stmt := AllCopyConns[0].stmt
	//for i := 0; i < 500; i++ {
	//	//_, err = stmt.Exec(int64(i), longString)
	//	_, err = stmt.Exec(int64(i), "hello world")
	//	if err != nil {
	//		log.Fatal(err)
	//	}
	//}
}

func clibtest() {
	field := C.int(344)
	mod := C.int(18)
	size := C.get_matching_hash_bounds_int(field, mod)
	fmt.Println(size)
}
