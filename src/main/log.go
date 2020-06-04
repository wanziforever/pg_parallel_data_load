package main

import (
	"log"
	"runtime"
	"fmt"
	"sync"
	"strings"
)

const (
	LOG_LEVEL_DEBUG int = iota
	LOG_LEVEL_INFO
	LOG_LEVEL_WARNING
	LOG_LEVEL_ERROR
)

type Log struct {
	level int
	mu sync.Mutex
}

func NewLogger() *Log {
	l := &Log{
		level: LOG_LEVEL_WARNING,
	}
	return l
}

func (this *Log) set_log_level(level string) {
	level = strings.ToLower(level)
	
	switch level {
	case "debug":
		this.level = LOG_LEVEL_DEBUG
	case "info":
		this.level = LOG_LEVEL_INFO
	case "warning":
		this.level = LOG_LEVEL_WARNING
	case "error":
		this.level = LOG_LEVEL_ERROR
	default:
		this.level = LOG_LEVEL_WARNING
	}
}


func (this *Log) Debug(format string, v ...interface{}) {
	this.mu.Lock()
	defer this.mu.Unlock()
	
	if this.level > LOG_LEVEL_DEBUG {
		return
	}
	var file, short string
	var line int
	var ok bool
	_, file, line, ok = runtime.Caller(1)
	if !ok {
		file = "???"
		line =0
	}
	
	for i := len(file) - 1; i > 0; i-- {
		if file[i] == '/' {
			short = file[i+1:]
			break
		}
	}
	file = short
	header := fmt.Sprintf("DEBUG-%s:%d", file, line)
	log.Println(header, fmt.Sprintf(format, v...))
}


func (this *Log) Info(format string, v ...interface{}) {
	this.mu.Lock()
	defer this.mu.Unlock()
	
	if this.level > LOG_LEVEL_INFO {
		return
	}
	header := "INFO"
	log.Println(header, fmt.Sprintf(format, v...))
}


func (this *Log) Warn(format string, v ...interface{}) {
	this.mu.Lock()
	defer this.mu.Unlock()

	if this.level > LOG_LEVEL_WARNING {
		return
	}
	header := "WARNING"
	log.Println(header, fmt.Sprintf(format, v...))
}


func (this *Log) Error(format string, v ...interface{}) {
	this.mu.Lock()
	defer this.mu.Unlock()

	if this.level > LOG_LEVEL_ERROR {
		return
	}
	header := "ERROR"
	log.Println(header, fmt.Sprintf(format, v...))
}


func (this *Log) Fatal(format string, v ...interface {}) {
	this.mu.Lock()
	defer this.mu.Unlock()
	header := "Fatal "
	log.Fatal(header, fmt.Sprintf(format, v...))
}
