package changes

import (
	"context"
	"os"
	"time"

	"github.com/omniscale/imposm-changes/log"

	"github.com/pkg/errors"

	osm "github.com/omniscale/go-osm"
	"github.com/omniscale/go-osm/parser/changeset"
	"github.com/omniscale/go-osm/parser/diff"
	"github.com/omniscale/go-osm/parser/pbf"
	"github.com/omniscale/go-osm/replication"
	replChanges "github.com/omniscale/go-osm/replication/changeset"
	replDiff "github.com/omniscale/go-osm/replication/diff"

	"github.com/omniscale/imposm-changes/database"
)

func Run(config *Config) error {
	db, err := database.NewPostGIS(config.Connection, config.Schemas.Changes)
	if err != nil {
		return errors.Wrap(err, "creating postgis connection")
	}

	diffSeq, err := db.ReadDiffStatus()
	if err != nil {
		return errors.Wrap(err, "unable to read diff current status")
	}
	if diffSeq <= 0 {
		diffSeq, err = replDiff.CurrentSequence(config.DiffUrl)
		if err != nil {
			return errors.Wrapf(err, "unable to read current diff from %s", config.DiffUrl)
		}
		diffSeq -= int(config.InitialHistory.Duration/config.DiffInterval.Duration) - 1
	}
	changeSeq, err := db.ReadChangesetStatus()
	if err != nil {
		return errors.Wrap(err, "unable to read changeset current status")
	}
	if changeSeq <= 0 {
		changeSeq, err = replChanges.CurrentSequence(config.ChangesetUrl)
		if err != nil {
			return errors.Wrapf(err, "unable to read current changeset from %s", config.ChangesetUrl)
		}
		changeSeq -= int(config.InitialHistory.Duration/config.ChangesetInterval.Duration) - 1
	}

	var diffDl replication.Source
	if config.DiffFromDiffDir {
		diffDl = replDiff.NewReader(config.DiffDir, diffSeq+1)
	} else {
		diffDl = replDiff.NewDownloader(config.DiffDir, config.DiffUrl, diffSeq+1, config.DiffInterval.Duration)
	}

	defer diffDl.Stop()
	var changeDl replication.Source
	if config.ChangesetFromChangesDir {
		changeDl = replChanges.NewReader(config.ChangesDir, changeSeq+1)
	} else {
		changeDl = replChanges.NewDownloader(config.ChangesDir, config.ChangesetUrl, changeSeq+1, config.ChangesetInterval.Duration)
	}
	defer changeDl.Stop()

	nextDiff := diffDl.Sequences()
	nextChange := changeDl.Sequences()

	// For testing: Stop when no new changes/diffs appear for X seconds if
	// IMPOSM_CHANGES_STOP_AFTER_FIRST_MISSING env is set.
	// Can be used to import only existing files.
	var checkWaiting <-chan time.Time
	if os.Getenv("IMPOSM_CHANGES_STOP_AFTER_FIRST_MISSING") != "" {
		checkWaiting = time.Tick(time.Second)
	}
	var lastImport time.Time

	for {
		select {
		case seq := <-nextDiff:
			if err := ImportDiff(db, config.LimitTo, seq); err != nil {
				return err
			}
			lastImport = time.Now()
		case seq := <-nextChange:
			if err := ImportChangeset(db, config.LimitTo, seq); err != nil {
				return err
			}
			lastImport = time.Now()
		case <-checkWaiting:
			if time.Since(lastImport) > 2*time.Second {
				log.Println("[info] stopping after some files are not available immediately")
				return nil
			}
		}

	}
	return nil
}

func ImportPBF(config *Config, pbfFilename string) error {
	db, err := database.NewPostGIS(config.Connection, config.Schemas.Changes)
	if err != nil {
		return errors.Wrap(err, "creating postgis connection")
	}
	start := time.Now()
	if err := db.Begin(); err != nil {
		return errors.Wrap(err, "starting transaction")
	}
	defer db.Close()
	if err := db.Init(); err != nil {
		return errors.Wrap(err, "init postgis changes database")
	}
	if err := db.Truncate(); err != nil {
		return errors.Wrap(err, "truncate postgis changes tables")
	}
	if err := db.ResetLastState(); err != nil {
		return errors.Wrap(err, "reset last state in database")
	}

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
	h, err := parser.Header()
	if err != nil {
		return errors.Wrap(err, "parsing PBF header")
	}

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

	var lastLog time.Time
	for {
		select {
		case nds, ok := <-nodes:
			if !ok {
				nodes = nil
			}
			if config.LimitTo != nil {
				nds = filterNodes(nds, config.LimitTo, insertedNodes)
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
			if config.LimitTo != nil {
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
			if config.LimitTo != nil {
				rs = filterRelations(rs, insertedNodes, insertedWays, insertedRelations)
			}
			nRelations += len(rs)
			if err := db.ImportRelations(rs); err != nil {
				stop()
				return errors.Wrap(err, "importing relations")
			}
		}
		if time.Since(lastLog) > time.Second {
			log.Printf("[progress] imported %8d nodes, %7d ways, %6d relations in %s", nNodes, nWays, nRelations, time.Since(start))
			lastLog = time.Now()
		}
		if nodes == nil && ways == nil && rels == nil {
			break
		}
	}
	if err := parser.Error(); err != nil {
		return errors.Wrapf(err, "parsing diff %s", pbfFilename)
	}

	start = time.Now()
	if err := db.InitIndices(); err != nil {
		log.Fatal("[error] creating indices:", err)
	}
	log.Printf("[step] created indices in %s", time.Since(start))

	if !h.Time.IsZero() {
		if err := updateSequence(db, h.Time, config); err != nil {
			log.Println("[error] Unable to collect and save diff/changeset sequence. "+
				"You need to manually set sequences in current_status tables to prevent missing data:", err)
		}
	}
	if err := db.Commit(); err != nil {
		return errors.Wrapf(err, "committing diff")
	}
	return nil
}

func updateSequence(db *database.PostGIS, since time.Time, config *Config) error {
	diffSeq, err := replDiff.CurrentSequence(config.DiffUrl)
	if err != nil {
		errors.Wrapf(err, "unable to read current diff from %s", config.DiffUrl)
	}
	diffSeq -= int((time.Since(since)+config.InitialHistory.Duration)/config.DiffInterval.Duration) - 1
	if err := db.SaveDiffStatus(diffSeq, since.Add(-config.InitialHistory.Duration)); err != nil {
		return errors.Wrapf(err, "unable to save current status")
	}

	changeSeq, err := replChanges.CurrentSequence(config.ChangesetUrl)
	if err != nil {
		return errors.Wrapf(err, "unable to read current changeset from %s", config.ChangesetUrl)
	}
	changeSeq -= int((time.Since(since)+config.InitialHistory.Duration)/config.ChangesetInterval.Duration) - 1
	if err := db.SaveChangesetStatus(changeSeq, since.Add(-config.InitialHistory.Duration)); err != nil {
		return errors.Wrapf(err, "unable to save current status")
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
			case osm.NodeMember:
				if _, ok := insertedNodes[m.ID]; ok {
					found = true
					break checkMembers
				}
			case osm.WayMember:
				if _, ok := insertedWays[m.ID]; ok {
					found = true
					break checkMembers
				}
			case osm.RelationMember:
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
	log.Printf("[info] Importing diff #%d including changes till %s (%s behind)",
		seq.Sequence,
		seq.Time.Format(time.RFC3339),
		time.Since(seq.Time).Truncate(time.Second),
	)
	start := time.Now()
	if err := db.Begin(); err != nil {
		return errors.Wrap(err, "starting transaction")
	}
	defer db.Close() // will rollback if Commit was not called

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
		imported, err := db.ImportElem(elem)
		if err != nil {
			stop()
			return errors.Wrapf(err, "importing %v from %s", elem, seq.Filename)
		}
		if imported {
			numElements += 1
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
	log.Printf("[step] \timported %d elements in %s", numElements, time.Since(start))
	return nil
}

func ImportChangeset(db *database.PostGIS, limitTo *LimitTo, seq replication.Sequence) (err error) {
	log.Printf("[info] Importing changes #%d including data till %s (%s behind)",
		seq.Sequence,
		seq.Time.Format(time.RFC3339),
		time.Since(seq.Time).Truncate(time.Second),
	)
	start := time.Now()
	if err := db.Begin(); err != nil {
		return errors.Wrap(err, "starting transaction")
	}
	defer db.Close() // will rollback if Commit was not called

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
		if limitTo != nil {
		// do no import all changesets if whe have a limitto

		// skip open changesets as maxextent is not set on open changesets
		if c.Open {
			continue
		}
		// skip empty changesets
		if c.NumChanges == 0 {
			continue
		}
		// skip changesets outside of limitto
		if !limitTo.Intersects(c.MaxExtent) {
			continue
		}
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
	log.Printf("[step] \timported %d changeset in %s", numChanges, time.Since(start))
	return nil
}
