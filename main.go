package main

import "log"

func init() {
	log.SetFlags(log.Lshortfile | log.Ltime)
}

func main() {
	log.Println("Starting acquisition ...")
}
