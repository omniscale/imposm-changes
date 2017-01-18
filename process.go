package changes

import (
	"io"
	"log"
	"time"

	"github.com/omniscale/imposm-changes/database"
	"github.com/omniscale/imposm3/parser/changeset"
	"github.com/omniscale/imposm3/parser/diff"
	"github.com/omniscale/imposm3/replication"
	"github.com/pkg/errors"
)

func Run(config *Config) error {
	db, err := database.NewPostGIS(config.Connection, config.Schemas.Changes)
	if err != nil {
		return errors.Wrap(err, "creating postgis connection")
	}
	if err := db.Init(); err != nil {
		return errors.Wrap(err, "init postgis changes database")
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

	cleanup := time.Tick(5 * time.Minute)

	for {
		select {
		case seq := <-nextDiff:
			log.Printf("importing diff %s from %s", seq.Filename, seq.Time)
			start := time.Now()
			if err := db.Begin(); err != nil {
				return errors.Wrap(err, "starting transaction")
			}
			osc, err := diff.NewOscGzParser(seq.Filename)
			if err != nil {
				return errors.Wrapf(err, "creating .osc.gz parser for %s", seq.Filename)
			}
			osc.SetWithMetadata(true)

			numElements := 0
			for {
				elem, err := osc.Next()
				if err == io.EOF {
					break
				}
				if err != nil {
					return errors.Wrapf(err, "parsing diff %s", seq.Filename)
				}
				numElements += 1
				if err := db.ImportElem(elem); err != nil {
					log.Println(err)
				}
			}
			if err := db.SaveDiffStatus(seq.Sequence, seq.Time); err != nil {
				return errors.Wrap(err, "saving diff status")
			}
			if err := db.Commit(); err != nil {
				return errors.Wrapf(err, "committing diff")
			}
			log.Printf("\timported %d elements in %s", numElements, time.Since(start))
		case seq := <-nextChange:
			log.Printf("importing changeset %s from %s", seq.Filename, seq.Time)
			start := time.Now()
			if err := db.Begin(); err != nil {
				return errors.Wrap(err, "starting transaction")
			}
			changes, err := changeset.ParseAllOsmGz(seq.Filename)
			if err != nil {
				return errors.Wrapf(err, "parsing changesets %s", seq.Filename)
			}
			for _, c := range changes {
				if err := db.ImportChangeset(c); err != nil {
					log.Println(err)
				}
			}
			if err := db.SaveChangesetStatus(seq.Sequence, seq.Time); err != nil {
				return errors.Wrapf(err, "saving changeset status")
			}
			if err := db.Commit(); err != nil {
				return errors.Wrapf(err, "committing changeset")
			}
			log.Printf("\timported %d changeset in %s", len(changes), time.Since(start))
		case <-cleanup:
			if config.LimitTo != nil {
				log.Printf("cleaning up elements/changesets")
				// Cleanup ways/relations outside of limitto (based on extent of the changesets)
				// Do this before CleanupChangesets, to prevent ways/relations that have no
				// changeset.
				if err := db.CleanupElements(*config.LimitTo); err != nil {
					return errors.Wrap(err, "cleaning up elements")
				}
				// Cleanup closed changesets outside of limitto
				if err := db.CleanupChangesets(*config.LimitTo, 24*time.Hour); err != nil {
					return errors.Wrap(err, "cleaning up changesets")
				}
			}
		}

	}

	return nil
}
