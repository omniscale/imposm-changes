package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/omniscale/imposm-changes/log"

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
		fmt.Println(changes.Version)
		return
	}
	if *configFilename == "" {
		flag.PrintDefaults()
		log.Fatal("[error] missing -config")
	}
	config, err := changes.LoadConfig(*configFilename)
	if err != nil {
		log.Fatalf("[error] loading config %q: %v", *configFilename, err)
	}

	if len(flag.Args()) < 1 {
		log.Fatal("[error] sub-command required: version, run, import")
	}

	switch flag.Args()[0] {
	case "run":
		if err := changes.Run(config); err != nil {
			log.Fatalf("[error] %+v", err)
		}
	case "import":
		if len(flag.Args()) < 2 {
			log.Fatal("[error] pbf file required")
		}
		if err := changes.ImportPBF(config, flag.Args()[1]); err != nil {
			log.Fatalf("[error] %+v", err)
		}
	case "version":
		fmt.Println(changes.Version)
		os.Exit(0)
	default:
		flag.PrintDefaults()
		log.Fatalf("[error] invalid command: '%s'", flag.Args()[1])
	}
	os.Exit(0)
}
