package main

import (
	"flag"
	"log"

	"github.com/omniscale/osm-changetracker"
)

func main() {
	configFilename := flag.String("config", "", "configuration file")

	flag.Parse()

	if *configFilename == "" {
		log.Fatal("missing -config")
	}
	config, err := changetracker.LoadConfig(*configFilename)
	if err != nil {
		log.Fatal(err)
	}
	if err := changetracker.Run(config); err != nil {
		log.Fatalf("%+v", err)
	}
}
