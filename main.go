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
	err = corn.Put(name, "wang1", "zhen1")
	if err != nil {
		log.Println(err)
	}
	fmt.Println(corn.List())
	err = corn.Delete("wang")
	if err != nil {
		log.Println(err)
	}
	v, err = corn.Get("wang")
	if err != nil {
		log.Println(err)
	} else {
		fmt.Println(v)
	}
	fmt.Println(corn.List())

	err = corn.Merge("./")
	if err != nil {
		log.Fatalln(err)
	}
}
