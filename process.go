package changes

import (
	"context"
	"log"
	"os"
	"time"

	osm "github.com/omniscale/go-osm"
	"github.com/omniscale/go-osm/parser/changeset"
	"github.com/omniscale/go-osm/parser/diff"
	"github.com/omniscale/go-osm/parser/pbf"
	"github.com/omniscale/imposm-changes/database"
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
			if err := ImportDiff(db, config.LimitTo, seq); err != nil {
				return err
			}
		case seq := <-nextChange:
			if err := ImportChangeset(db, seq); err != nil {
				return err
			}
		case <-cleanup:
			if config.LimitTo != nil {
				log.Printf("info: cleaning up elements/changesets")
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

func ImportPBF(db *database.PostGIS, limitTo *LimitTo, pbfFilename string) error {
	start := time.Now()
	if err := db.Begin(); err != nil {
		return errors.Wrap(err, "starting transaction")
	}

	// TODO defer rollback?
	f, err := os.Open(pbfFilename)
	if err != nil {
		return errors.Wrapf(err, "opening diff file %s", pbfFilename)
	}
	defer f.Close()

	nodes := make(chan []osm.Node)
	ways := make(chan []osm.Way)
	rels := make(chan []osm.Relation)
	conf := pbf.Config{
		Nodes:           nodes,
		Ways:            ways,
		Relations:       rels,
		IncludeMetadata: true,
	}
	parser := pbf.New(f, conf)

	ctx, stop := context.WithCancel(context.Background())
	go parser.Parse(ctx)

	var nNodes, nWays, nRelations int

	// If limitto is enabled, we record all node IDs that are inside our bbox.
	// Only ways that have at least one node in our bbox are inserted and
	// only relations that have at least one node or way in our bbox are inserted.

	// TODO: Using a map is not very memory efficient (~30-40 bytes per inserted node).
	// Expected memory consumption: ~12GB for Germany, ~3GB for NRW.
	// A bloomfilter would be more efficient.
	insertedNodes := map[int64]struct{}{}
	insertedWays := map[int64]struct{}{}
	insertedRelations := map[int64]struct{}{}

	for {
		select {
		case nds, ok := <-nodes:
			if !ok {
				nodes = nil
			}
			if limitTo != nil {
				nds = filterNodes(nds, limitTo, insertedNodes)
			}
			nNodes += len(nds)
			if err := db.ImportNodes(nds); err != nil {
				stop()
				return errors.Wrap(err, "importing nodes")
			}
		case ws, ok := <-ways:
			if !ok {
				ways = nil
			}
			if limitTo != nil {
				ws = filterWays(ws, insertedNodes, insertedWays)
			}
			nWays += len(ws)
			if err := db.ImportWays(ws); err != nil {
				stop()
				return errors.Wrap(err, "importing ways")
			}
		case rs, ok := <-rels:
			if !ok {
				rels = nil
			}
			if limitTo != nil {
				rs = filterRelations(rs, insertedNodes, insertedWays, insertedRelations)
			}
			nRelations += len(rs)
			if err := db.ImportRelations(rs); err != nil {
				stop()
				return errors.Wrap(err, "importing relations")
			}
		}
		log.Printf("imported %8d nodes, %7d ways, %6d relations in %s", nNodes, nWays, nRelations, time.Since(start))
		if nodes == nil && ways == nil && rels == nil {
			break
		}
	}

	if err := parser.Error(); err != nil {
		return errors.Wrapf(err, "parsing diff %s", pbfFilename)
	}

	if err := db.Commit(); err != nil {
		return errors.Wrapf(err, "committing diff")
	}
	return nil
}

// filterNodes removes nodes that are outside the given bbox. IDs of all keept
// nodes are added to insertedNodes. Alters nodes in-place and returns slice
// with updated len.
func filterNodes(nodes []osm.Node, limitTo *LimitTo, insertedNodes map[int64]struct{}) []osm.Node {
	i := 0
	for _, nd := range nodes {
		if limitTo.Contains(nd.Long, nd.Lat) {
			nodes[i] = nd
			insertedNodes[nd.ID] = struct{}{}
			i++
		}
	}
	return nodes[:i]
}

// filterWays removes ways were no referenced node ID is included in
// insertedNodes. I.e. ways are keept as long as a single referenced node was
// inserted before. IDs of all keept ways are added to insertedWays. Alters
// ways in-place and returns slice with updated len.
func filterWays(ways []osm.Way, insertedNodes map[int64]struct{}, insertedWays map[int64]struct{}) []osm.Way {
	i := 0
	for _, w := range ways {
		for _, n := range w.Refs {
			if _, ok := insertedNodes[n]; ok {
				ways[i] = w
				insertedWays[w.ID] = struct{}{}
				i++
				break
			}
		}
	}
	return ways[:i]
}

// filterRelations removes relations were no referenced node, way or relation
// ID is included in insertedNodes, insertedWays or insertedRelations. I.e
// relations are keept as long as a single referenced node, way or relation was
// inserted before. IDs of all keept relations are added to insertedRelations.
// Alters rels in-place and returns slice with updated len.
func filterRelations(rels []osm.Relation, insertedNodes map[int64]struct{}, insertedWays map[int64]struct{}, insertedRelations map[int64]struct{}) []osm.Relation {
	i := 0
	for _, r := range rels {
		found := false
	checkMembers:
		for _, m := range r.Members {
			switch m.Type {
			case osm.NODE:
				if _, ok := insertedNodes[m.ID]; ok {
					found = true
					break checkMembers
				}
			case osm.WAY:
				if _, ok := insertedWays[m.ID]; ok {
					found = true
					break checkMembers
				}
			case osm.RELATION:
				if _, ok := insertedRelations[m.ID]; ok {
					found = true
					break checkMembers
				}
			}
		}
		if found {
			rels[i] = r
			insertedRelations[r.ID] = struct{}{}
			i++
		}
	}
	return rels[:i]
}

func ImportDiff(db *database.PostGIS, limitTo *LimitTo, seq replication.Sequence) error {
	log.Printf("info: importing diff %s from %s", seq.Filename, seq.Time)
	start := time.Now()
	if err := db.Begin(); err != nil {
		return errors.Wrap(err, "starting transaction")
	}
	// TODO defer rollback?

	f, err := os.Open(seq.Filename)
	if err != nil {
		return errors.Wrapf(err, "opening diff file %s", seq.Filename)
	}
	defer f.Close()

	conf := diff.Config{
		Diffs:           make(chan osm.Diff),
		IncludeMetadata: true,
	}
	parser, err := diff.NewGZIP(f, conf)
	if err != nil {
		return errors.Wrapf(err, "creating .osc.gz parser for %s", seq.Filename)
	}

	ctx, stop := context.WithCancel(context.Background())
	go parser.Parse(ctx)

	numElements := 0
	for elem := range conf.Diffs {
		if elem.Node != nil && !limitTo.Contains(elem.Node.Long, elem.Node.Lat) {
			continue
		}
		numElements += 1
		if err := db.ImportElem(elem); err != nil {
			stop()
			return errors.Wrapf(err, "importing %v from %s", elem, seq.Filename)
		}
	}
	if err := parser.Error(); err != nil {
		return errors.Wrapf(err, "parsing diff %s", seq.Filename)
	}

	if err := db.SaveDiffStatus(seq.Sequence, seq.Time); err != nil {
		return errors.Wrap(err, "saving diff status")
	}
	if err := db.Commit(); err != nil {
		return errors.Wrapf(err, "committing diff")
	}
	log.Printf("info: \timported %d elements in %s", numElements, time.Since(start))
	return nil
}

func ImportChangeset(db *database.PostGIS, seq replication.Sequence) error {
	log.Printf("info: importing changeset %s from %s", seq.Filename, seq.Time)
	start := time.Now()
	if err := db.Begin(); err != nil {
		return errors.Wrap(err, "starting transaction")
	}
	// TODO defer rollback?

	f, err := os.Open(seq.Filename)
	if err != nil {
		return errors.Wrapf(err, "opening changeset file %s", seq.Filename)
	}
	defer f.Close()

	conf := changeset.Config{
		Changesets: make(chan osm.Changeset),
	}
	parser, err := changeset.NewGZIP(f, conf)
	if err != nil {
		return errors.Wrapf(err, "creating .osm.gz parser for %s", seq.Filename)
	}

	ctx, stop := context.WithCancel(context.Background())
	go parser.Parse(ctx)

	numChanges := 0
	for c := range conf.Changesets {
		numChanges += 1
		if err := db.ImportChangeset(c); err != nil {
			stop()
			return errors.Wrapf(err, "importing %v from %s", c, seq.Filename)
		}
	}
	if err := parser.Error(); err != nil {
		return errors.Wrapf(err, "parsing changesets %s", seq.Filename)
	}

	if err := db.SaveChangesetStatus(seq.Sequence, seq.Time); err != nil {
		return errors.Wrapf(err, "saving changeset status")
	}
	if err := db.Commit(); err != nil {
		return errors.Wrapf(err, "committing changeset")
	}
	log.Printf("info: \timported %d changeset in %s", numChanges, time.Since(start))
	return nil
}
