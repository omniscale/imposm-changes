package test

import (
	"database/sql"
	"io/ioutil"
	"os"
	"time"

	"testing"

	osm "github.com/omniscale/go-osm"
	"github.com/omniscale/go-osm/replication"
	changes "github.com/omniscale/imposm-changes"
)

func TestComplete(t *testing.T) {
	if testing.Short() {
		t.Skip("system test skipped with -test.short")
	}
	t.Parallel()

	ts := testSuite{
		name: "complete",
	}

	t.Run("Prepare", func(t *testing.T) {
		var err error

		ts.dir, err = ioutil.TempDir("", "imposm_test")
		if err != nil {
			t.Fatal(err)
		}
		ts.config = testConfig{
			connection: "postgis://?sslmode=disable",
			limitTo:    &changes.LimitTo{0, 0, 90, 90},
		}

		ts.sql, err = sql.Open("postgres", "sslmode=disable")
		if err != nil {
			t.Fatal(err)
		}
		ts.dropSchemas()
	})

	t.Run("Import", func(t *testing.T) {
		tables := []string{
			"nodes",
			"ways",
			"relations",
			"nds",
			"members",
		}
		for _, tbl := range tables {
			if ts.tableExists(t, ts.dbschemaImport(), tbl) != false {
				t.Errorf("table %s exists in schema %s", tbl, ts.dbschemaImport())
			}
		}
		ts.importPBF(t, "build/testdata_import.pbf")
		for _, tbl := range tables {
			if ts.tableExists(t, ts.dbschemaImport(), tbl) != true {
				t.Errorf("table %s does not exists in schema %s", tbl, ts.dbschemaImport())
			}
		}
	})

	t.Run("Check current_status", func(t *testing.T) {
		db := ts.db(t)

		// ReadXXXStatus is independent of an open transaction
		if s, err := db.ReadChangesetStatus(); err != nil || s != -1 {
			t.Error("unexpected changeset status", err, s)
		}
		if s, err := db.ReadDiffStatus(); err != nil || s != -1 {
			t.Error("unexpected diff status", err, s)
		}

		if err := db.Begin(); err != nil {
			t.Fatal("init transaction", err)
		}
		defer db.Close()
		if err := db.SaveChangesetStatus(12345, time.Now()); err != nil {
			t.Error("writing changes status", err)
		}
		if err := db.SaveDiffStatus(54321, time.Now()); err != nil {
			t.Error("writing diff status", err)
		}

		if err := db.Commit(); err != nil {
			t.Error("commiting current_status", err)
		}

		// ReadXXXStatus is independent of an open transaction
		if s, err := db.ReadChangesetStatus(); err != nil || s != 12345 {
			t.Error("unexpected changeset status", err, s)
		}
		if s, err := db.ReadDiffStatus(); err != nil || s != 54321 {
			t.Error("unexpected diff status", err, s)
		}

	})

	t.Run("Some nodes are filtered by limitto", func(t *testing.T) {
		ts.assertMissingNode(t, 10100)
		ts.assertNode(t, osm.Node{
			Element: osm.Element{
				ID: 10101,
				Metadata: &osm.Metadata{
					UserID:    10180,
					UserName:  "User10180",
					Version:   1,
					Timestamp: time.Date(2017, 5, 2, 14, 50, 31, 0, time.UTC),
					Changeset: 10190,
				},
			},
			Lat:  43,
			Long: 7,
		})
	})
	t.Run("Limitto way, no node included", func(t *testing.T) {
		ts.assertMissingNode(t, 10200)
		ts.assertMissingNode(t, 10201)
		ts.assertMissingWay(t, 10250)
	})
	t.Run("Limitto way, one node included", func(t *testing.T) {
		ts.assertMissingNode(t, 10300)
		ts.assertWay(t, osm.Way{
			Element: osm.Element{
				ID:   10350,
				Tags: osm.Tags{"highway": "primary"},
				Metadata: &osm.Metadata{
					UserID:    10380,
					UserName:  "User10380",
					Version:   1,
					Timestamp: time.Date(2017, 5, 2, 14, 0, 0, 0, time.UTC),
					Changeset: 10390,
				},
			},
		})
	})
	t.Run("Limitto way, all nodes included", func(t *testing.T) {
		ts.assertWay(t, osm.Way{
			Element: osm.Element{
				ID:   10450,
				Tags: osm.Tags{"highway": "primary"},
				Metadata: &osm.Metadata{
					UserID:    10480,
					UserName:  "User10480",
					Version:   1,
					Timestamp: time.Date(2017, 5, 2, 14, 0, 0, 0, time.UTC),
					Changeset: 10490,
				},
			},
		})
	})
	t.Run("Limitto relations", func(t *testing.T) {
		md := osm.Metadata{
			UserID:    10580,
			UserName:  "User10580",
			Version:   1,
			Timestamp: time.Date(2017, 5, 2, 14, 0, 0, 0, time.UTC),
			Changeset: 10590,
		}

		ts.assertMissingNode(t, 10500)
		ts.assertNode(t, osm.Node{
			Element: osm.Element{
				ID:       10501,
				Metadata: &md,
			},
			Lat:  43,
			Long: 8,
		})

		ts.assertWay(t, osm.Way{
			Element: osm.Element{
				ID:       10550,
				Tags:     osm.Tags{"highway": "primary"},
				Metadata: &md,
			},
		})

		ts.assertMissingRelation(t, 10570)
		ts.assertRelation(t, osm.Relation{Element: osm.Element{ID: 10571, Metadata: &md}})
		ts.assertRelation(t, osm.Relation{Element: osm.Element{ID: 10572, Metadata: &md}})
		ts.assertRelation(t, osm.Relation{Element: osm.Element{ID: 10573, Metadata: &md}})
		ts.assertRelation(t, osm.Relation{Element: osm.Element{ID: 10574, Metadata: &md}})

	})

	t.Run("Import Diff", func(t *testing.T) {
		ts.importDiff(t, replication.Sequence{Time: time.Now(), Filename: "build/testdata_diff.osc.gz"})
	})

	t.Run("Modify/delete existing nodes", func(t *testing.T) {
		ts.assertMissingNode(t, 20100)
		ts.assertNodeVersions(t, 20101, 1, 2)
		ts.assertMissingNode(t, 20102)
		ts.assertNodeVersions(t, 20103, 1, 2)
	})

	t.Run("Import of already imported elements", func(t *testing.T) {
		ts.assertMissingNode(t, 22000) // out of limitto
		ts.assertNodeVersions(t, 22001, 1)
		ts.assertRelation(t, osm.Relation{Element: osm.Element{ID: 22070, Metadata: nil}})
	})

	t.Run("Import of new elements in diff", func(t *testing.T) {
		ts.assertMissingNode(t, 30000) // out of limitto
		ts.assertNodeVersions(t, 30001, 1)
		ts.assertWay(t, osm.Way{
			Element: osm.Element{
				ID:   30050,
				Tags: osm.Tags{"highway": "primary"},
				Metadata: &osm.Metadata{
					UserID:    30080,
					UserName:  "User30080",
					Version:   1,
					Timestamp: time.Date(2017, 5, 2, 14, 0, 0, 0, time.UTC),
					Changeset: 30090,
				},
			},
		})

		ts.assertRelation(t, osm.Relation{Element: osm.Element{ID: 30070, Metadata: nil}})
		ts.assertRelation(t, osm.Relation{Element: osm.Element{ID: 30071, Metadata: nil}})
	})

	t.Run("Import Changesets", func(t *testing.T) {
		ts.assertMissingChangeset(t, 90090)
		ts.importChangeset(t, replication.Sequence{Time: time.Now(), Filename: "build/testdata_changes.osm.gz"})
	})

	t.Run("Imported changesets ", func(t *testing.T) {
		ts.assertMissingChangeset(t, 91090)
		ts.assertChangeset(t, osm.Changeset{
			ID:         90090,
			UserID:     90080,
			UserName:   "user01",
			NumChanges: 3,
			MaxExtent:  [4]float64{0, -10, 20, 10},
			CreatedAt:  time.Date(2018, 11, 12, 0, 0, 0, 0, time.UTC),
			ClosedAt:   time.Date(2018, 11, 12, 0, 0, 4, 0, time.UTC),
			Tags:       map[string]string{"comment": "changeset 90090"},
			Comments: []osm.Comment{
				{
					UserID:    90081,
					CreatedAt: time.Date(2018, 11, 12, 8, 0, 0, 0, time.UTC),
					UserName:  "user02",
					Text:      "Are you sure?",
				},
				{
					UserID:    90080,
					CreatedAt: time.Date(2018, 11, 12, 8, 0, 10, 0, time.UTC),
					UserName:  "user01",
					Text:      "Yes!",
				},
			},
		})
	})

	t.Run("Cleanup", func(t *testing.T) {
		ts.dropSchemas()
		if err := os.RemoveAll(ts.dir); err != nil {
			t.Fatal(err)
		}
	})
}
