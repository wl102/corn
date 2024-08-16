package main

import (
	"corn/grain"
	"fmt"
	"log"
)

func main() {
	filename := "./corn.db"
	corn := grain.OpenDB(filename)
	v, err := corn.Get("wang")
	if err != nil {
		log.Println(err)
	} else {
		fmt.Println(v)
	}
	v, err = corn.Get("wang1")
	if err != nil {
		log.Println(err)
	} else {
		fmt.Println(v)
	}

	fmt.Println(corn.List())
}
