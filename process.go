package changetracker

import (
	"io"
	"log"

	"github.com/omniscale/imposm3/parser/changeset"
	"github.com/omniscale/imposm3/parser/diff"
	"github.com/omniscale/imposm3/replication"
	"github.com/omniscale/osm-changetracker/database"
	"github.com/pkg/errors"
)

func Run(config *Config) error {
	db, err := database.NewPostGIS(config.Connection, config.Schemas.Changes)
	if err != nil {
		log.Fatal(err)
	}
	if err := db.Init(); err != nil {
		log.Fatal(err)
	}

	diffSeq, err := db.ReadDiffStatus()
	if err != nil {
		return errors.Wrap(err, "unable to read diff current status")
	}
	if diffSeq <= 0 {
		diffSeq, err = replication.CurrentDiff(config.DiffUrl)
		if err != nil {
			errors.Wrapf(err, "unable to read current diff from %s", config.DiffUrl)
		}
		diffSeq -= int(config.InitialHistory.Duration / config.DiffInterval.Duration)
	}
	changeSeq, err := db.ReadChangesetStatus()
	if err != nil {
		return errors.Wrap(err, "unable to read changeset current status")
	}
	if changeSeq <= 0 {
		changeSeq, err = replication.CurrentChangeset(config.ChangesetUrl)
		if err != nil {
			errors.Wrapf(err, "unable to read current changeset from %s", config.ChangesetUrl)
		}
		changeSeq -= int(config.InitialHistory.Duration / config.ChangesetInterval.Duration)
	}

	var diffDl replication.Source
	if config.DiffFromDiffDir {
		diffDl = replication.NewDiffReader(config.DiffDir, diffSeq)
	} else {
		diffDl = replication.NewDiffDownloader(config.DiffDir, config.DiffUrl, diffSeq, config.DiffInterval.Duration)
	}

	changeDl := replication.NewChangesetDownloader(config.ChangesDir, config.ChangesetUrl, changeSeq, config.ChangesetInterval.Duration)

	nextDiff := diffDl.Sequences()
	nextChange := changeDl.Sequences()

	var filter func(diff.Element) bool
	if config.LimitTo != nil {
		bf := &BboxFilter{5, 50, 10, 55}
		filter = bf.FilterElement
	}
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
				if filter != nil && filter(elem) {
					continue
				}
				if err := db.ImportElem(elem); err != nil {
					log.Println(err)
				}
			}
			if err := db.SaveDiffStatus(seq.Sequence, seq.Time); err != nil {
				log.Fatal(err)
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
			if err := db.SaveChangesetStatus(seq.Sequence, seq.Time); err != nil {
				log.Fatal(err)
			}
		}
		if err := db.Commit(); err != nil {
			log.Fatal(err)
		}

		if config.LimitTo != nil {
			if err := db.CleanupElements(*config.LimitTo); err != nil {
				log.Fatal(err)
			}
		}
	}

	return nil
}

type BboxFilter struct {
	Minx, Miny, Maxx, Maxy float64
}

func (b *BboxFilter) FilterElement(elem diff.Element) bool {
	if elem.Node != nil {
		if elem.Node.Long < b.Minx || elem.Node.Long > b.Maxx || elem.Node.Lat < b.Miny || elem.Node.Lat > b.Maxy {
			return true
		}
	}
	return false
}
