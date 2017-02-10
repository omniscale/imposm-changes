package main

import (
	"flag"
	"fmt"
	"log"

	"comail.io/go/colog"
	"github.com/omniscale/imposm-changes"
)

func main() {
	colog.Register()

	configFilename := flag.String("config", "", "configuration file")
	verbose := flag.Bool("verbose", false, "show current progress, otherwise only errors are shown")
	version := flag.Bool("version", false, "print version and exit")

	flag.Parse()

	if *verbose {
		colog.SetMinLevel(colog.LInfo)
	} else {
		colog.SetMinLevel(colog.LWarning)
	}

	if *version {
		fmt.Println("imposm-changes", changes.Version)
		return
	}
	if *configFilename == "" {
		log.Fatal("error: missing -config")
	}
	config, err := changes.LoadConfig(*configFilename)
	if err != nil {
		log.Fatal("error:", err)
	}
	if err := changes.Run(config); err != nil {
		log.Fatalf("error: %+v", err)
	}
}
