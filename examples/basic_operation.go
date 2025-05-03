package main

import (
	bitsack "SingleKVDataSet"
	"fmt"
)

func main() {
	opts := bitsack.DefaultOptions
	opts.DirPath = "../TestingFile"
	db, err := bitsack.Open(opts)
	if err != nil {
		panic(err)
	}
	err = db.Put([]byte("name"), []byte("bitsack"))
	if err != nil {
		panic(err)
	}
	val, err := db.Get([]byte("name"))
	if err != nil {
		panic(err)
	}
	fmt.Println("val:", string(val))

	err = db.Delete([]byte("name"))
	if err != nil {
		panic(err)
	}
}
