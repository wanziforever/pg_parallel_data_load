package loadconfig

import (
	"gopkg.in/yaml.v2"
	"io/ioutil"
)

type NetworkNode struct {
	Host string `yaml:"host"`
	Port int `yaml:"port"`
}

type Table struct {
	Tablename string `yaml:"tablename"`
	Columns string `yaml:"columns"`
	PartitionField int `yaml:"partitionField"`
	Datapath string `yaml:datapath`
}

type Config struct {
	Dbname string `yaml:"dbname"`
	Schema string `yaml:"schema"`
	User string `yaml:"user"`
	Password string `yaml:"password"`
	Buffersize int `yaml:"buffersize"`
	Readers int `yaml:readers""`
	Slicenum int `yaml:"slicenum"`
	Maxtuplechunk int64 `yaml:maxtuplechunk`
	Loglevel string `yaml:"loglevel"`
	Encoding string `yaml:"encoding"`
	Csvheader bool `yaml:"csvheader"`
	Nodes []NetworkNode `yaml:"nodes"`
	Tables []Table `yaml:tables`
}

type SysConfig struct {
	Io_read_size int `yaml:"io_read_size"`
	Max_data_queue_sync_size int `yaml:"max_data_queue_sync_size"`
	Basket_tuple_size int `yaml:"basket_tuple_size"`
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

func ReadSysConfigData(path string) (*SysConfig, error) {
	var config SysConfig
	source, err := ioutil.ReadFile(path)
	if err != nil {
		panic(err)
	}

	err = yaml.Unmarshal(source, &config)
	return &config, err
}
