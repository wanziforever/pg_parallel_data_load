package main

import (
	"fmt"
	"log"
	"github.com/jackc/pgconn"
	"context"
	"time"
	"bufio"
	"os"
	"sync"
)

var swg sync.WaitGroup

func main() {
	go work1(&swg)
	swg.Add(1)
	//go work2(&swg)
	//swg.Add(1)
	//go work3(&swg)
	//swg.Add(1)

	swg.Wait()
}

func work1(swg *sync.WaitGroup) {
	filename := "/home/postgres/data/csv/bmsql_item_large1.csv"
	connstr := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		"127.0.0.1", 5432, "postgres", "1", "postgres")
	log.Println(connstr)
	db, err := pgconn.Connect(context.Background(), connstr)
	if err != nil {
		log.Fatal(err)
	}
	file, err := os.Open(filename)
	defer file.Close()
	br := bufio.NewReader(file)
	ctx, _ := context.WithTimeout(context.Background(), 400*time.Second)
	_, err = db.CopyFrom(ctx, br, CopyIn(
		"bmsql_item", "i_id", "i_name", "i_price", "i_data", "i_im_id"))

	
	ctx, _ = context.WithTimeout(context.Background(), 100*time.Millisecond)
	db.Close(ctx)
	swg.Done()
}

func work2(swg *sync.WaitGroup) {
	filename := "/home/postgres/data/csv/bmsql_item_large2.csv"
	connstr := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		"127.0.0.1", 5433, "postgres", "1", "postgres")
	log.Println(connstr)
	db, err := pgconn.Connect(context.Background(), connstr)
	if err != nil {
		log.Fatal(err)
	}
	file, err := os.Open(filename)
	defer file.Close()
	br := bufio.NewReader(file)
	ctx, _ := context.WithTimeout(context.Background(), 400*time.Second)
	_, err = db.CopyFrom(ctx, br, CopyIn(
		"bmsql_item", "i_id", "i_name", "i_price", "i_data", "i_im_id"))

	
	ctx, _ = context.WithTimeout(context.Background(), 100*time.Millisecond)
	db.Close(ctx)
	swg.Done()
}

func work3(swg *sync.WaitGroup) {
	filename := "/home/postgres/data/csv/bmsql_item_large3.csv"
	connstr := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		"127.0.0.1", 5434, "postgres", "1", "postgres")
	log.Println(connstr)
	db, err := pgconn.Connect(context.Background(), connstr)
	if err != nil {
		log.Fatal(err)
	}
	file, err := os.Open(filename)
	defer file.Close()
	br := bufio.NewReader(file)
	ctx, _ := context.WithTimeout(context.Background(), 400*time.Second)
	_, err = db.CopyFrom(ctx, br, CopyIn(
		"bmsql_item", "i_id", "i_name", "i_price", "i_data", "i_im_id"))

	
	ctx, _ = context.WithTimeout(context.Background(), 100*time.Millisecond)
	db.Close(ctx)
	swg.Done()
}

func CopyIn(table string, columns ...string) string {
	statement := "COPY " + table + " ("
	for i, col := range columns {
		if i != 0 {
			statement += ", "
		}
		statement += col
	}
	statement += ") FROM STDIN WITH (FORMAT csv)"
	return statement
}
