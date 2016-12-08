package changetracker

import (
	"io"
	"log"
	"time"

	"github.com/omniscale/imposm3/parser/changeset"
	"github.com/omniscale/imposm3/parser/diff"
	"github.com/omniscale/imposm3/replication"
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

	// diffDl := replication.NewDiffDownloader("diffs", "http://planet.openstreetmap.org/replication/minute/", 2138860, time.Minute)
	diffDl := replication.NewDiffReader("diffs", 2138860)
	changeDl := replication.NewChangesetDownloader("changes", "http://planet.openstreetmap.org/replication/changesets/", 2138860, time.Minute)

	nextDiff := diffDl.Sequences()
	nextChange := changeDl.Sequences()

	for {
		if err := db.Begin(); err != nil {
			log.Fatal(err)
		}

		select {
		case seq := <-nextDiff:
			log.Print(seq)
			osc, err := diff.NewOscGzParser(seq.Filename)
			if err != nil {
				log.Fatal(err)
			}
			osc.SetWithMetadata(true)

			for {
				elem, err := osc.Next()
				if err == io.EOF {
					break
				}
				if err != nil {
					log.Fatal(err)
				}
				if err := db.ImportElem(elem); err != nil {
					log.Println(err)
				}
			}
		case seq := <-nextChange:
			log.Print(seq)
			changes, err := changeset.ParseAllOsmGz(seq.Filename)
			if err != nil {
				log.Fatal(err)
			}
			for _, c := range changes {
				if err := db.ImportChangeset(c); err != nil {
					log.Fatal(err)
				}
			}
		}
		if err := db.Commit(); err != nil {
			log.Fatal(err)
		}
	}

}
