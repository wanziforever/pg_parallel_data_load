package main

import (
	"loadconfig"
	"log"
	"os"
)

var configfile string

func main() {
	if len(os.Args) > 1 {
		configfile = os.Args[1]
	}

	if configfile == "" {
		log.Println("configuration file not provided ..")
		os.Exit(1)
	}
	conf, err := loadconfig.ReadConfigData(configfile)
	if err != nil {
		log.Fatal(err)
	}
	log.Println(conf)
}

