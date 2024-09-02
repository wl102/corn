package main

import (
	"corn"
	"fmt"
	"log"
)

func main() {
	db, err := corn.OpenWithOpts("./bitcask")
	if err != nil {
		log.Fatalf("open bitcask database fail:%s", err)
	}
	key := "wang"
	value := "zhen"
	err = db.Put(key, value, 0)
	if err != nil {
		log.Println(err)
	}
	if val, ok := db.Get(key); ok {
		fmt.Printf("key [%s] 's value is [%s]\n", key, val)
	}
	err = db.Put("wang_second", value, 0)
	if err != nil {
		log.Println(err)
	}
	fmt.Printf("list bitcask datastore keys: %v\n", db.ListKeys())
	err = db.Delete("wang_second")
	if err != nil {
		log.Println(err)
	}
	fmt.Printf("list bitcask datastore keys: %v\n", db.ListKeys())
}
