package loadconfig

import (
	"gopkg.in/yaml.v2"
	"io/ioutil"
)

type NetworkNode struct {
	Host string `yaml:"host"`
	Port int `yaml:"port"`
}

type Config struct {
	Dbname string `yaml:"dbname"`
	User string `yaml:"user"`
	Password string `yaml:"password"`
	TableName string `yaml:"tablename"`
	Datapath string `yaml:"datapath"`
	PartitionField int `yaml:"partitionField"`
	Buffersize int `yaml:"buffersize"`
	Readers int `yaml:readers""`
	Maxtuplechunk int64 `yaml:Maxtuplechunk`
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
