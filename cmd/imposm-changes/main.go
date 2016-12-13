package main

import (
	"flag"
	"log"

	"github.com/omniscale/imposm-changes"
)

func main() {
	configFilename := flag.String("config", "", "configuration file")

	flag.Parse()

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
