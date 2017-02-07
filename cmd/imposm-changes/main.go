package main

import (
	"flag"
	"fmt"
	"log"

	"github.com/omniscale/imposm-changes"
)

func main() {
	configFilename := flag.String("config", "", "configuration file")
	version := flag.Bool("version", false, "print version and exit")

	flag.Parse()

	if *version {
		fmt.Println("imposm-changes", changes.Version)
		return
	}
	if *configFilename == "" {
		log.Fatal("missing -config")
	}
	config, err := changes.LoadConfig(*configFilename)
	if err != nil {
		log.Fatal(err)
	}
	if err := changes.Run(config); err != nil {
		log.Fatalf("%+v", err)
	}
}
