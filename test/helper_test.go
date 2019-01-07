package test

import (
	"database/sql"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/omniscale/imposm-changes/log"

	"github.com/kr/pretty"
	osm "github.com/omniscale/go-osm"
	"github.com/omniscale/go-osm/replication"
	"github.com/omniscale/imposm-changes/database"

	"github.com/lib/pq/hstore"

	"github.com/omniscale/imposm-changes"
)

type testConfig struct {
	connection string
	limitTo    *changes.LimitTo
}

type testSuite struct {
	dir    string
	name   string
	sql    *sql.DB
	config testConfig
}

func (s *testSuite) dbschemaImport() string { return "imposm_changes_test_" + s.name + "_import" }
func (s *testSuite) dbschemaProduction() string {
	return "imposm_changes_test_" + s.name + "_import"
	// TODO after rotate is implemented
	// return "imposm_changes_test_" + s.name + "_production"
}
func (s *testSuite) dbschemaBackup() string { return "imposm_changes_test_" + s.name + "_backup" }

func (s *testSuite) db(t *testing.T) *database.PostGIS {
	db, err := database.NewPostGIS(s.config.connection, s.dbschemaImport())
	if err != nil {
		t.Fatal("connecting to db", err)
	}
	return db
}

func (s *testSuite) importPBF(t *testing.T, filename string) {
	conf := changes.DefaultConfig
	conf.Connection = s.config.connection
	conf.LimitTo = s.config.limitTo
	conf.Schemas = changes.Schemas{Changes: s.dbschemaImport()}

	if err := changes.ImportPBF(&conf, filename); err != nil {
		t.Fatal(err)
	}
}

func (s *testSuite) importDiff(t *testing.T, seq replication.Sequence) {
	db := s.db(t)
	defer db.Close()
	if err := changes.ImportDiff(db, s.config.limitTo, seq); err != nil {
		t.Fatal(err)
	}
}

func (s *testSuite) importChangeset(t *testing.T, seq replication.Sequence) {
	db, err := database.NewPostGIS(s.config.connection, s.dbschemaProduction())
	if err != nil {
		t.Fatal("creating postgis connection", err)
	}
	defer db.Close()
	if err := changes.ImportChangeset(db, s.config.limitTo, seq); err != nil {
		t.Fatal(err)
	}
}

func (s *testSuite) dropSchemas() {
	var err error
	_, err = s.sql.Exec(fmt.Sprintf(`DROP SCHEMA IF EXISTS %s CASCADE`, s.dbschemaImport()))
	if err != nil {
		log.Fatal(err)
	}
	_, err = s.sql.Exec(fmt.Sprintf(`DROP SCHEMA IF EXISTS %s CASCADE`, s.dbschemaProduction()))
	if err != nil {
		log.Fatal(err)
	}
	_, err = s.sql.Exec(fmt.Sprintf(`DROP SCHEMA IF EXISTS %s CASCADE`, s.dbschemaBackup()))
	if err != nil {
		log.Fatal(err)
	}
}

func (s *testSuite) tableExists(t *testing.T, schema, table string) bool {
	row := s.sql.QueryRow(fmt.Sprintf(`SELECT EXISTS(SELECT * FROM information_schema.tables WHERE table_name='%s' AND table_schema='%s')`, table, schema))
	var exists bool
	if err := row.Scan(&exists); err != nil {
		t.Error(err)
		return false
	}
	return exists
}

func (s *testSuite) assertNodeVersions(t *testing.T, id int, versions ...int) {
	t.Helper()
	rows, err := s.sql.Query(fmt.Sprintf(`SELECT version FROM "%s".nodes WHERE id=$1 ORDER BY version`, s.dbschemaProduction()), id)
	if err != nil {
		t.Fatal(err)
	}
	got := []int{}

	for rows.Next() {
		var ver int
		if err := rows.Scan(&ver); err != nil {
			t.Fatal(err)
		}
		got = append(got, ver)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(got, versions) {
		t.Errorf("found unexpected versions of node %d\ngot:  %v\nwant: %v", id, got, versions)
	}
}

func (s *testSuite) assertMissingNode(t *testing.T, id int) {
	t.Helper()
	rows, err := s.sql.Query(fmt.Sprintf(`SELECT id FROM "%s".nodes WHERE id=$1`, s.dbschemaProduction()), id)
	if err != nil {
		t.Fatal(err)
	}
	if rows.Next() {
		t.Errorf("found node with id %d", id)
	}
}

func (s *testSuite) assertNode(t *testing.T, want osm.Node) {
	t.Helper()
	rows, err := s.sql.Query(fmt.Sprintf(`SELECT id, version, timestamp, user_id, user_name, changeset, ST_X(geometry) as long, ST_Y(geometry) AS lat, tags FROM "%s".nodes WHERE id=$1`, s.dbschemaProduction()), want.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !rows.Next() {
		t.Errorf("did not found node %#v", want)
		return
	}
	h := hstore.Hstore{}
	got := osm.Node{Element: osm.Element{Metadata: &osm.Metadata{}}}
	if err := rows.Scan(
		&got.ID,
		&got.Metadata.Version,
		&got.Metadata.Timestamp,
		&got.Metadata.UserID,
		&got.Metadata.UserName,
		&got.Metadata.Changeset,
		&got.Long,
		&got.Lat,
		&h,
	); err != nil {
		t.Fatal(err)
	}
	got.Tags = hstoreTags(h)
	got.Metadata.Timestamp = got.Metadata.Timestamp.UTC() // convert from +0 to UTC for DeepEqual

	if want.Metadata == nil {
		// do not compare metadata
		got.Metadata = nil
	}

	if !reflect.DeepEqual(got, want) {
		t.Errorf("unexpected result want:\n%# v\ngot:\n%# v\ndiffs:\n\t%s",
			pretty.Formatter(want),
			pretty.Formatter(got),
			strings.Join(pretty.Diff(want, got), "\n\t"),
		)
	}
}

func (s *testSuite) assertMissingWay(t *testing.T, id int) {
	t.Helper()
	rows, err := s.sql.Query(fmt.Sprintf(`SELECT id FROM "%s".ways WHERE id=$1`, s.dbschemaProduction()), id)
	if err != nil {
		t.Fatal(err)
	}
	if rows.Next() {
		t.Errorf("found way with id %d", id)
	}
}

func (s *testSuite) assertWay(t *testing.T, wants ...osm.Way) {
	t.Helper()
	rows, err := s.sql.Query(
		fmt.Sprintf(
			`SELECT id, version, timestamp, user_id, user_name, changeset, tags FROM "%s".ways WHERE id=$1 order by version`,
			s.dbschemaProduction()),
		wants[0].ID,
	)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range wants {
		if !rows.Next() {
			t.Errorf("did not found way %#v", want)
			return
		}
		h := hstore.Hstore{}
		got := osm.Way{Element: osm.Element{Metadata: &osm.Metadata{}}}
		if err := rows.Scan(
			&got.ID,
			&got.Metadata.Version,
			&got.Metadata.Timestamp,
			&got.Metadata.UserID,
			&got.Metadata.UserName,
			&got.Metadata.Changeset,
			&h,
		); err != nil {
			t.Fatal(err)
		}
		got.Tags = hstoreTags(h)
		got.Metadata.Timestamp = got.Metadata.Timestamp.UTC() // convert from +0 to UTC for DeepEqual

		if want.Metadata == nil {
			// do not compare metadata
			got.Metadata = nil
		}

		if !reflect.DeepEqual(got, want) {
			t.Errorf("unexpected result want:\n%# v\ngot:\n%# v\ndiffs:\n\t%s",
				pretty.Formatter(want),
				pretty.Formatter(got),
				strings.Join(pretty.Diff(want, got), "\n\t"),
			)
		}
	}
}

func (s *testSuite) assertMissingRelation(t *testing.T, id int) {
	t.Helper()
	rows, err := s.sql.Query(fmt.Sprintf(`SELECT id FROM "%s".relations WHERE id=$1`, s.dbschemaProduction()), id)
	if err != nil {
		t.Fatal(err)
	}
	if rows.Next() {
		t.Errorf("found relation with id %d", id)
	}
}

func (s *testSuite) assertRelation(t *testing.T, want osm.Relation) {
	t.Helper()
	rows, err := s.sql.Query(fmt.Sprintf(`SELECT id, version, timestamp, user_id, user_name, changeset, tags FROM "%s".relations WHERE id=$1`, s.dbschemaProduction()), want.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !rows.Next() {
		t.Errorf("did not found relation %#v", want)
		return
	}
	h := hstore.Hstore{}
	got := osm.Relation{Element: osm.Element{Metadata: &osm.Metadata{}}}
	if err := rows.Scan(
		&got.ID,
		&got.Metadata.Version,
		&got.Metadata.Timestamp,
		&got.Metadata.UserID,
		&got.Metadata.UserName,
		&got.Metadata.Changeset,
		&h,
	); err != nil {
		t.Fatal(err)
	}
	got.Tags = hstoreTags(h)
	got.Metadata.Timestamp = got.Metadata.Timestamp.UTC() // convert from +0 to UTC for DeepEqual

	if want.Metadata == nil {
		// do not compare metadata
		got.Metadata = nil
	}

	if !reflect.DeepEqual(got, want) {
		t.Errorf("unexpected result want:\n%# v\ngot:\n%# v\ndiffs:\n\t%s",
			pretty.Formatter(want),
			pretty.Formatter(got),
			strings.Join(pretty.Diff(want, got), "\n\t"),
		)
	}
}

func (s *testSuite) assertChangeset(t *testing.T, want osm.Changeset) {
	t.Helper()
	rows, err := s.sql.Query(fmt.Sprintf(`
	SELECT id, created_at, closed_at, num_changes, open, user_name, user_id, tags,
	st_xmin(bbox), st_ymin(bbox), st_xmax(bbox), st_ymax(bbox)
	FROM "%s".changesets WHERE id=$1`, s.dbschemaProduction()), want.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !rows.Next() {
		t.Errorf("did not found node %#v", want)
		return
	}
	h := hstore.Hstore{}
	got := osm.Changeset{}
	if err := rows.Scan(
		&got.ID,
		&got.CreatedAt,
		&got.ClosedAt,
		&got.NumChanges,
		&got.Open,
		&got.UserName,
		&got.UserID,
		&h,
		&got.MaxExtent[0],
		&got.MaxExtent[1],
		&got.MaxExtent[2],
		&got.MaxExtent[3],
	); err != nil {
		t.Fatal(err)
	}
	got.Tags = hstoreTags(h)
	got.CreatedAt = got.CreatedAt.UTC() // convert from +0 to UTC for DeepEqual
	got.ClosedAt = got.ClosedAt.UTC()   // convert from +0 to UTC for DeepEqual

	rows, err = s.sql.Query(fmt.Sprintf(`
	SELECT timestamp, user_name, user_id, text
	FROM "%s".comments WHERE changeset_id=$1 ORDER BY idx`, s.dbschemaProduction()), want.ID)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		c := osm.Comment{}
		if err := rows.Scan(
			&c.CreatedAt,
			&c.UserName,
			&c.UserID,
			&c.Text,
		); err != nil {
			t.Fatal(err)
		}
		c.CreatedAt = c.CreatedAt.UTC() // convert from +0 to UTC for DeepEqual
		got.Comments = append(got.Comments, c)
	}

	if !reflect.DeepEqual(got, want) {
		t.Errorf("unexpected result want:\n%# v\ngot:\n%# v\ndiffs:\n\t%s",
			pretty.Formatter(want),
			pretty.Formatter(got),
			strings.Join(pretty.Diff(want, got), "\n\t"),
		)
	}
}

func (s *testSuite) assertMissingChangeset(t *testing.T, id int) {
	t.Helper()
	rows, err := s.sql.Query(fmt.Sprintf(`SELECT id FROM "%s".changesets WHERE id=$1`, s.dbschemaProduction()), id)
	if err != nil {
		t.Fatal(err)
	}
	if rows.Next() {
		t.Errorf("found changeset with id %d", id)
	}
}

func hstoreTags(hs hstore.Hstore) osm.Tags {
	if len(hs.Map) == 0 {
		return nil
	}
	tags := make(osm.Tags)
	for k, v := range hs.Map {
		if v.Valid {
			tags[k] = v.String
		}
	}
	return tags
}
