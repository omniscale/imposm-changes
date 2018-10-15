package test

import (
	"database/sql"
	"io/ioutil"
	"os"
	"time"

	"testing"

	osm "github.com/omniscale/go-osm"
	changes "github.com/omniscale/imposm-changes"
	"github.com/omniscale/imposm3/replication"
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

		ts.db, err = sql.Open("postgres", "sslmode=disable")
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

	t.Run("Some nodes are filtered by limitto", func(t *testing.T) {
		ts.assertMissingNode(t, 10100)
		ts.assertNode(t, osm.Node{
			OSMElem: osm.OSMElem{
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
			OSMElem: osm.OSMElem{
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
			OSMElem: osm.OSMElem{
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
			OSMElem: osm.OSMElem{
				ID:       10501,
				Metadata: &md,
			},
			Lat:  43,
			Long: 8,
		})

		ts.assertWay(t, osm.Way{
			OSMElem: osm.OSMElem{
				ID:       10550,
				Tags:     osm.Tags{"highway": "primary"},
				Metadata: &md,
			},
		})

		ts.assertMissingRelation(t, 10570)
		ts.assertRelation(t, osm.Relation{OSMElem: osm.OSMElem{ID: 10571, Metadata: &md}})
		ts.assertRelation(t, osm.Relation{OSMElem: osm.OSMElem{ID: 10572, Metadata: &md}})
		ts.assertRelation(t, osm.Relation{OSMElem: osm.OSMElem{ID: 10573, Metadata: &md}})
		ts.assertRelation(t, osm.Relation{OSMElem: osm.OSMElem{ID: 10574, Metadata: &md}})

	})

	t.Run("Import Diff", func(t *testing.T) {
		ts.importDiff(t, replication.Sequence{Filename: "build/testdata_diff.osc.gz"})
	})

	t.Run("Modify/delete existing nodes", func(t *testing.T) {
		ts.assertMissingNode(t, 20100)
		ts.assertNodeVersions(t, 20101, 1, 2)
		ts.assertMissingNode(t, 20102)
		ts.assertNodeVersions(t, 20103, 1, 2)
	})

	t.Run("Import of already imported elements ", func(t *testing.T) {
		ts.assertMissingNode(t, 22000) // out of limitto
		ts.assertNodeVersions(t, 22001, 1)
	})

	t.Run("Cleanup", func(t *testing.T) {
		ts.dropSchemas()
		if err := os.RemoveAll(ts.dir); err != nil {
			t.Fatal(err)
		}
	})
}
