package main

import (
	"os"
	"os/exec"
	"fmt"
	"bytes"
	"strings"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"sync"
)

var wg sync.WaitGroup

var (
	PSQL string = "/usr/local/pgsql/bin/psql"
	slice = 6
	alldbs = make([]*DBInfo, 0)
	alltables = make([]*TableInfo, 0)
)

type DBInfo struct {
	host string
	port int
	user string
	password string
	db string
}


type TableInfo struct {
	tname string
	schema string
}


type Config struct {
	User string `yaml:"user"`
	Db string `yaml:"db"`
	Schema string `yaml:"schema"`
	Tables string `yaml:"tables"`
	Hosts []struct {
		Ip string `yaml:"ip"`
		Port int `yaml:"port"`
	}
}


func sysinit(conf *Config) {
	for _, host := range conf.Hosts {
		alldbs = append(alldbs, &DBInfo{
			host:host.Ip,
			port: host.Port,
			user: conf.User,
			db: conf.Db})
	}
	
	for _, table := range strings.Split(conf.Tables, ",") {
		alltables = append(alltables, &TableInfo{tname: strings.TrimSpace(table), schema: conf.Schema })
	}
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


func loadConfig(configFile string) (*Config) {
	if configFile == "" {
		panic("configuration file not provided...")
	}
	
	conf, err := ReadConfigData(configFile)
	if err != nil {
		panic(err)
	}
	return conf
}


func cleardataHost(dbi *DBInfo) {
	connstr := makeConnectString(dbi)
	for _, table := range alltables {
		cleardataTable(table, connstr)
	}
}


func cleardataTable(ti *TableInfo, connstr string) {
	for i:=0; i<slice; i++ {
		sql := makeClearCommandStr(i, ti.schema, ti.tname)
		cmd := fmt.Sprintf("%s -c \"%s;\"", connstr, sql)
		wg.Add(1)
		go processcmd(cmd)
	}
}


func vacuumData(dbi *DBInfo) {
	connstr := makeConnectString(dbi)
	cmd := fmt.Sprintf("%s -c \"%s;\"", connstr, makeVauumCommandStr())
	wg.Add(1)
	go processcmd(cmd)
}

func processcmd(cmd string) {
	var stderr bytes.Buffer
	var stdout bytes.Buffer
	c := exec.Command("bash", "-c", cmd)
	c.Stdout = &stdout
	c.Stderr = &stderr
	err := c.Run()
	if err != nil {
		fmt.Println("handle command fail for ", err, stderr.String())
		fmt.Println("context:", cmd)
	} else {
		fmt.Println("command:", cmd, "success")
	}

	wg.Done()
}


func makeConnectString(dbi *DBInfo) (string) {
	connstr := fmt.Sprintf("%s -h %s -p %d -U %s -d %s",
		PSQL, dbi.host, dbi.port, dbi.user, dbi.db)
	return connstr
}

func makeVauumCommandStr() string {
	return "vacuum full"
}


func makeClearCommandStr(sid int, schema string, table string) (string) {
	return fmt.Sprintf("delete from %s_%d.%s", schema, sid, table)
}


func process() {
	for _, host := range alldbs {
		cleardataHost(host)
	}
	wg.Wait()
	fmt.Println("finish delete data in tables")
	fmt.Println("going to do vacuum for all database")
	for _, host := range alldbs {
		vacuumData(host)
	}
	wg.Wait()
}


func main() {
	if len(os.Args) != 2 {
		fmt.Println("invalid parameter")
		os.Exit(1)
	}
	confpath := os.Args[1]
	conf := loadConfig(confpath)
	sysinit(conf)
	process()
}
