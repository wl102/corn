package main

import (
	"corn/grain"
	"fmt"
	"log"
	"path/filepath"
)

func main() {
	filename := "./corn.db"
	name := filepath.Base(filename)
	corn := grain.OpenDB(filename)
	_, err := corn.Get("wang")
	if err != nil {
		log.Println(err)
	}
	err = corn.Put(name, "wang", "zhen")
	if err != nil {
		log.Println(err)
	}
	v, err := corn.Get("wang")
	if err != nil {
		log.Println(err)
	} else {
		fmt.Println(v)
	}
}
