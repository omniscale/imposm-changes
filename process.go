package changetracker

import (
	"log"

	"github.com/omniscale/imposm3/diff/parser"
	"github.com/omniscale/osm-changetracker/changeset"

	"github.com/omniscale/osm-changetracker/database"
)

func New() {

	db, err := database.NewPostGIS("sslmode=disable", "changes")
	if err != nil {
		log.Fatal(err)
	}
	if err := db.Init(); err != nil {
		log.Fatal(err)
	}
	// elems, errc := parser.ParseFull("changeset/084.osc.gz")
	elems, errc := parser.ParseFull("changeset/612.osc.gz")
	for {
		select {
		case elem, ok := <-elems:
			if !ok {
				elems = nil
				break
			}
			if err := db.Import(elem); err != nil {
				log.Println(err)
			}
		case err, ok := <-errc:
			if !ok {
				errc = nil
				break
			}
			log.Fatal(err)
		}
		if errc == nil && elems == nil {
			break
		}
	}

	changes, err := changeset.Parse("changeset/999.osm.gz")
	if err != nil {
		log.Fatal(err)
	}
	for _, c := range changes {
		if err := db.Changeset(c); err != nil {
			log.Fatal(err)
		}
	}

	if err := db.Commit(); err != nil {
		log.Fatal(err)
	}
}
