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
	slice = 1
	alldbs = make([]*DBInfo, 0)
	schema = ""
	patten *SQLPatten = nil
	sliceDbMapping = make(map[int][]int)
	mode = "accurate"
)


type SQLPatten struct {
	orig string
	tablename string
	tokens []string
	pos int
	hascolon bool
}


type DBInfo struct {
	host string
	port int
	user string
	password string
	db string
	index int
}


type Config struct {
	User string `yaml:"user"`
	Db string `yaml:"db"`
	Schema string `yaml:"schema"`
	Mode string `yaml:"mode"`
	Slice int `yaml:"slice"`
	Hosts []struct {
		Ip string `yaml:"ip"`
		Port int `yaml:"port"`
	}
}


func sysinit(conf *Config) {
	for i, host := range conf.Hosts {
		alldbs = append(alldbs, &DBInfo{
			host:host.Ip,
			port: host.Port,
			user: conf.User,
			db: conf.Db,
			index: i,
		})
	}

	for i:=0; i<len(alldbs); i++ {
		sliceDbMapping[i] = make([]int, 0)
	}
	
	schema = conf.Schema
	for i:=0; i<slice; i++ {
		dbid := i % len(alldbs)
		sliceDbMapping[dbid] = append(sliceDbMapping[dbid], i)
	}

	mode = conf.Mode
	slice = conf.Slice
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


func findDataHost(dbi *DBInfo) {
	connstr := makeConnectString(dbi)
	fmt.Println("\n+***********************************************************+")
	fmt.Println("making result for ", connstr)
	if mode == "all" {
		for i:=0; i<slice; i++ {
			sql := generateSQL(patten, fmt.Sprintf("%s_%d", schema, i))
			cmd := fmt.Sprintf("%s -c \"%s;\"", connstr, sql)
			fmt.Println(cmd)
			processcmd(cmd)
		}
	} else if (mode == "accurate") {
		for _, id := range sliceDbMapping[dbi.index] {
			sql := generateSQL(patten, fmt.Sprintf("%s_%d", schema, id))
			cmd := fmt.Sprintf("%s -c \"%s;\"", connstr, sql)
			fmt.Println(cmd)
			processcmd(cmd)
		}
	}
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
		fmt.Println(stdout.String())
		//fmt.Println("command:", cmd, "success\n")
	}
}


func parseSQL(sql string) *SQLPatten {
	
	p := new(SQLPatten)
	if sql[len(sql)-1] == ';' {
		p.hascolon = true
		sql = strings.TrimRight(sql, ";")
	} else {
		p.hascolon = false
	}
	p.tokens = strings.Split(sql, " ") 
	pos := 0
	for _, token := range p.tokens {
		if token == "from" {
			p.pos = pos + 1
			break
		}
		pos += 1
	}
	p.tablename = p.tokens[pos+1]
	p.tokens = append(p.tokens[:pos+1], p.tokens[pos+2:]...)
	return p
}


func generateSQL(p *SQLPatten, schema string) string {
	newtable := schema + "." + p.tablename
	newtokens := make([]string, p.pos)
	copy(newtokens, p.tokens[:p.pos])
	newtokens = append(newtokens, newtable)
	newtokens = append(newtokens, p.tokens[p.pos:]...)
	return strings.Join(newtokens, " ")
}


func makeConnectString(dbi *DBInfo) (string) {
	connstr := fmt.Sprintf("%s -h %s -p %d -U %s -d %s",
		PSQL, dbi.host, dbi.port, dbi.user, dbi.db)
	return connstr
}


func process() {
	for _, host := range alldbs {
		findDataHost(host)
	}
}


func main() {
	if len(os.Args) != 3 {
		fmt.Println("invalid parameter")
		os.Exit(1)
	}
	confpath := os.Args[1]
	sql := strings.Join(os.Args[2:], " ")
	patten = parseSQL(sql)
	

	conf := loadConfig(confpath)
	sysinit(conf)
	process()
}
