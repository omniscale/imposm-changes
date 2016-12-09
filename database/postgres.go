package database

import (
	"database/sql"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/lib/pq"
	"github.com/pkg/errors"

	"github.com/omniscale/imposm3/element"
	"github.com/omniscale/imposm3/parser/changeset"
	"github.com/omniscale/imposm3/parser/diff"
)

var initSql = []string{
	`CREATE SCHEMA IF NOT EXISTS "%[1]s";`,
	`CREATE TABLE IF NOT EXISTS "%[1]s".current_status (
        type VARCHAR UNIQUE,
        sequence INT,
        timestamp TIMESTAMP WITH TIME ZONE
);`,
	`CREATE TABLE IF NOT EXISTS "%[1]s".nodes (
    id BIGINT,
    add BOOLEAN,
    modify BOOLEAN,
    delete BOOLEAN,
    changeset INT,
    geometry GEOMETRY(Point, 4326),
    user_name VARCHAR,
    user_id INT,
    timestamp TIMESTAMP WITH TIME ZONE,
    version INT,
    tags HSTORE,
    PRIMARY KEY (id, version)
);`,
	`CREATE TABLE IF NOT EXISTS "%[1]s".ways (
    id INT NOT NULL,
    add BOOLEAN,
    modify BOOLEAN,
    delete BOOLEAN,
    changeset INT,
    user_name VARCHAR,
    user_id INT,
    timestamp TIMESTAMP WITH TIME ZONE,
    version INT,
    tags HSTORE,
    PRIMARY KEY (id, version)
);`,
	`CREATE INDEX IF NOT EXISTS ways_changset_idx ON "%[1]s".ways USING btree (changeset);`,
	`CREATE TABLE IF NOT EXISTS "%[1]s".nds (
    way_id INT NOT NULL,
    way_version INT NOT NULL,
    idx INT,
    node_id BIGINT NOT NULL,
    PRIMARY KEY (way_id, way_version, idx, node_id),
    FOREIGN KEY (way_id, way_version)
        REFERENCES "%[1]s".ways (id, version)
          ON UPDATE CASCADE
          ON DELETE CASCADE
);`,
	`CREATE TABLE IF NOT EXISTS "%[1]s".relations (
    id INT NOT NULL,
    add BOOLEAN,
    modify BOOLEAN,
    delete BOOLEAN,
    changeset INT,
    geometry GEOMETRY(Point, 4326),
    user_name VARCHAR,
    user_id INT,
    timestamp TIMESTAMP WITH TIME ZONE,
    version INT,
    tags HSTORE,
    PRIMARY KEY (id, version)
);`,
	`CREATE INDEX IF NOT EXISTS relations_changset_idx ON "%[1]s".relations USING btree (changeset);`,
	`CREATE TABLE IF NOT EXISTS "%[1]s".members (
    relation_id INT NOT NULL,
    relation_version INT,
    type VARCHAR,
    role VARCHAR,
    idx INT,
    member_node_id BIGINT,
    member_way_id INT,
    member_relation_id INT,
    FOREIGN KEY (relation_id, relation_version)
        REFERENCES "%[1]s".relations (id, version)
          ON UPDATE CASCADE
          ON DELETE CASCADE
);`,
	`CREATE TABLE IF NOT EXISTS "%[1]s".changesets (
    id INT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE,
    closed_at TIMESTAMP WITH TIME ZONE,
    num_changes INT,
    open BOOLEAN,
    user_name VARCHAR,
    user_id INT,
    tags HSTORE,
    bbox Geometry(POLYGON, 4326),
    PRIMARY KEY (id)
);`,
	`CREATE TABLE IF NOT EXISTS "%[1]s".comments (
    changeset_id BIGINT NOT NULL,
    idx INT,
    user_name VARCHAR,
    user_id INT,
    timestamp TIMESTAMP WITH TIME ZONE,
    text VARCHAR,
    PRIMARY KEY (changeset_id, idx),
    FOREIGN KEY (changeset_id)
        REFERENCES "%[1]s".changesets (id)
          ON UPDATE CASCADE
          ON DELETE CASCADE
);`,
}

type PostGIS struct {
	db               *sql.DB
	tx               *sql.Tx
	nodeStmt         *sql.Stmt
	wayStmt          *sql.Stmt
	ndsStmt          *sql.Stmt
	relStmt          *sql.Stmt
	memberStmt       *sql.Stmt
	changeStmt       *sql.Stmt
	changeUpdateStmt *sql.Stmt
	commentStmt      *sql.Stmt
	schema           string
}

func NewPostGIS(connection string, schema string) (*PostGIS, error) {
	if strings.HasPrefix(connection, "postgis") {
		connection = strings.Replace(connection, "postgis", "postgres", 1)
	}
	params, err := pq.ParseURL(connection)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to parse connection %s", connection)
	}
	db, err := sql.Open("postgres", params)
	if err != nil {
		return nil, err
	}

	if schema == "" {
		schema = "public"
	}

	return &PostGIS{
		db:     db,
		schema: schema,
	}, nil
}

func newSqlError(err error, elem interface{}) error {
	return &sqlError{elem: elem, err: err}
}

type sqlError struct {
	elem interface{}
	err  error
}

func (s *sqlError) Error() string {
	return fmt.Sprintf("error: %s; for %#v", s.err, s.elem)

}

func (p *PostGIS) Init() error {
	tx, err := p.db.Begin()
	if err != nil {
		return err
	}
	for _, s := range initSql {
		stmt := fmt.Sprintf(s, p.schema)
		if _, err := tx.Exec(stmt); err != nil {
			tx.Rollback()
			return fmt.Errorf("error while calling %v: %v", stmt, err)
		}
	}

	for _, statusType := range []string{"changes", "diff"} {
		row := tx.QueryRow(
			fmt.Sprintf(`SELECT EXISTS(SELECT 1 FROM "%[1]s".current_status WHERE type = '%[2]s')`, p.schema, statusType),
		)
		var exists bool
		err := row.Scan(&exists)
		if err != nil {
			return err
		}
		if !exists {
			if _, err := tx.Exec(
				fmt.Sprintf(`INSERT INTO "%[1]s".current_status (type, sequence) VALUES ('%[2]s', -1)`, p.schema, statusType)); err != nil {
				return err
			}
		}
	}
	return tx.Commit()
}

func (p *PostGIS) Begin() error {
	var err error
	p.tx, err = p.db.Begin()
	if err != nil {
		return err
	}
	nodeStmt, err := p.tx.Prepare(
		fmt.Sprintf(`INSERT INTO "%[1]s".nodes (
            id,
            add,
            modify,
            delete,
            geometry,
            user_name,
            user_id,
            timestamp,
            version,
            changeset,
            tags) VALUES ($1, $2, $3, $4, ST_SetSRID(ST_Point($5, $6), 4326), $7, $8, $9, $10, $11, $12)`, p.schema),
	)
	if err != nil {
		return err
	}
	p.nodeStmt = nodeStmt

	wayStmt, err := p.tx.Prepare(
		fmt.Sprintf(`INSERT INTO "%[1]s".ways (
            id,
            add,
            modify,
            delete,
            user_name,
            user_id,
            timestamp,
            version,
            changeset,
            tags
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`, p.schema),
	)
	if err != nil {
		return err
	}
	p.wayStmt = wayStmt

	ndsStmt, err := p.tx.Prepare(
		fmt.Sprintf(`INSERT INTO "%[1]s".nds (
            way_id,
            way_version,
            idx,
            node_id
        ) VALUES ($1, $2, $3, $4)`, p.schema),
	)
	if err != nil {
		return err
	}
	p.ndsStmt = ndsStmt

	relStmt, err := p.tx.Prepare(
		fmt.Sprintf(`INSERT INTO "%[1]s".relations (
            id,
            add,
            modify,
            delete,
            user_name,
            user_id,
            timestamp,
            version,
            changeset,
            tags
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`, p.schema),
	)
	if err != nil {
		return err
	}
	p.relStmt = relStmt

	memberStmt, err := p.tx.Prepare(
		fmt.Sprintf(`INSERT INTO "%[1]s".members (
            relation_id,
            relation_version,
            type,
            role,
            idx,
            member_node_id,
            member_way_id,
            member_relation_id
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`, p.schema),
	)
	if err != nil {
		return err
	}
	p.memberStmt = memberStmt

	changeStmt, err := p.tx.Prepare(
		fmt.Sprintf(`INSERT INTO "%[1]s".changesets (
            id,
            created_at,
            closed_at,
            open,
            num_changes,
            user_name,
            user_id,
            bbox,
            tags
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, ST_GeomFromText($8), $9)`, p.schema),
	)
	if err != nil {
		return err
	}
	p.changeStmt = changeStmt

	changeUpdateStmt, err := p.tx.Prepare(
		fmt.Sprintf(`UPDATE "%[1]s".changesets SET
            created_at=$2,
            closed_at=$3,
            open=$4,
            num_changes=$5,
            user_name=$6,
            user_id=$7,
            bbox=ST_GeomFromText($8),
            tags=$9
        WHERE id = $1`, p.schema),
	)
	if err != nil {
		return err
	}
	p.changeUpdateStmt = changeUpdateStmt

	commentStmt, err := p.tx.Prepare(
		fmt.Sprintf(`INSERT INTO "%[1]s".comments (
            changeset_id,
            idx,
            user_name,
            user_id,
            timestamp,
            text
        ) VALUES ($1, $2, $3, $4, $5, $6)`, p.schema),
	)
	if err != nil {
		return err
	}
	p.commentStmt = commentStmt

	return nil
}

func (p *PostGIS) Commit() error {
	return p.tx.Commit()
}

func (p *PostGIS) CleanupElements(bbox [4]float64) error {
	tx, err := p.db.Begin()
	if err != nil {
		return err
	}

	changesetsStmt := `
SELECT id FROM "%[1]s".changesets
WHERE NOT open
AND NOT (bbox && ST_MakeEnvelope($1, $2, $3, $4))
`
	for _, table := range []string{"ways", "relations"} {
		stmt := fmt.Sprintf(`
DELETE FROM "%[1]s".%[2]s WHERE changeset IN (`+changesetsStmt+`)`,
			p.schema, table,
		)
		r, err := tx.Exec(stmt, bbox[0], bbox[1], bbox[2], bbox[3])
		if err != nil {
			tx.Rollback()
			return newSqlError(err, stmt)
		}
		rows, err := r.RowsAffected()
		if err != nil {
			tx.Rollback()
			return err
		}
		log.Printf("removed %d from %s", rows, table)
	}
	return tx.Commit()
}

func (p *PostGIS) CleanupChangesets(bbox [4]float64, olderThen time.Duration) error {
	tx, err := p.db.Begin()
	if err != nil {
		return err
	}
	before := time.Now().Add(-olderThen).UTC()

	stmt := `
DELETE FROM "%[1]s".%[2]s
WHERE closed_at < $5
AND NOT (bbox && ST_MakeEnvelope($1, $2, $3, $4))
`
	r, err := tx.Exec(stmt, bbox[0], bbox[1], bbox[2], bbox[3], before)
	if err != nil {
		tx.Rollback()
		return newSqlError(err, stmt)
	}
	rows, err := r.RowsAffected()
	if err != nil {
		tx.Rollback()
		return err
	}
	log.Printf("removed %d from changesets", rows)
	return tx.Commit()
}

func (p *PostGIS) ImportElem(elem diff.Element) (err error) {
	_, err = p.tx.Exec("SAVEPOINT insert")
	if err != nil {
		return
	}

	defer func() {
		if err != nil {
			p.tx.Exec("ROLLBACK TO SAVEPOINT insert")
		} else {
			_, err = p.tx.Exec("RELEASE SAVEPOINT insert")
		}
	}()
	var add, mod, del bool
	if elem.Mod {
		mod = true
	} else if elem.Add {
		add = true
	} else if elem.Del {
		del = true
	}
	if elem.Node != nil {
		nd := elem.Node
		if _, err = p.nodeStmt.Exec(
			nd.Id,
			add,
			mod,
			del,
			nd.Long, nd.Lat,
			nd.Metadata.UserName,
			nd.Metadata.UserId,
			nd.Metadata.Timestamp.UTC(),
			nd.Metadata.Version,
			nd.Metadata.Changeset,
			hstoreString(nd.Tags),
		); err != nil {
			return newSqlError(err, elem.Node)
		}
	} else if elem.Way != nil {
		w := elem.Way
		if _, err = p.wayStmt.Exec(
			w.Id,
			add,
			mod,
			del,
			w.Metadata.UserName,
			w.Metadata.UserId,
			w.Metadata.Timestamp.UTC(),
			w.Metadata.Version,
			w.Metadata.Changeset,
			hstoreString(w.Tags),
		); err != nil {
			return newSqlError(err, elem.Way)
		}
		for i, ref := range elem.Way.Refs {
			if _, err = p.ndsStmt.Exec(
				w.Id,
				w.Metadata.Version,
				i,
				ref,
			); err != nil {
				return newSqlError(err, elem.Way)
			}
		}
	} else if elem.Rel != nil {
		rel := elem.Rel
		if _, err = p.relStmt.Exec(
			rel.Id,
			add,
			mod,
			del,
			rel.Metadata.UserName,
			rel.Metadata.UserId,
			rel.Metadata.Timestamp.UTC(),
			rel.Metadata.Version,
			rel.Metadata.Changeset,
			hstoreString(rel.Tags),
		); err != nil {
			return newSqlError(err, elem.Rel)
		}
		for i, m := range elem.Rel.Members {
			var nodeId, wayId, relId interface{}
			switch m.Type {
			case element.NODE:
				nodeId = m.Id
			case element.WAY:
				wayId = m.Id
			case element.RELATION:
				relId = m.Id
			}
			if _, err = p.memberStmt.Exec(
				rel.Id,
				rel.Metadata.Version,
				m.Type,
				m.Role,
				i,
				nodeId,
				wayId,
				relId,
			); err != nil {
				return newSqlError(err, elem.Rel)
			}
		}
	}

	return nil
}

func (p *PostGIS) SaveChangesetStatus(sequence int, timestamp time.Time) error {
	return p.saveStatus("changes", sequence, timestamp)
}

func (p *PostGIS) SaveDiffStatus(sequence int, timestamp time.Time) error {
	return p.saveStatus("diff", sequence, timestamp)
}

func (p *PostGIS) saveStatus(statusType string, sequence int, timestamp time.Time) error {
	_, err := p.tx.Exec(
		fmt.Sprintf(
			`UPDATE "%[1]s".current_status SET sequence = $1, timestamp = $2 WHERE type = '%[2]s'`,
			p.schema, statusType,
		), sequence, timestamp,
	)
	return err
}

func (p *PostGIS) ReadChangesetStatus() (int, error) {
	return p.readStatus("changes")
}

func (p *PostGIS) ReadDiffStatus() (int, error) {
	return p.readStatus("diff")
}

func (p *PostGIS) readStatus(statusType string) (int, error) {
	row := p.db.QueryRow(
		fmt.Sprintf(
			`SELECT sequence FROM "%[1]s".current_status WHERE type = '%[2]s'`,
			p.schema, statusType,
		),
	)
	var sequence int
	err := row.Scan(&sequence)
	return sequence, err
}

func (p *PostGIS) ImportChangeset(c changeset.Changeset) error {
	bbox := bboxPolygon(c)
	if _, err := p.tx.Exec("SAVEPOINT insert_changeset"); err != nil {
		return err
	}
	// insert null if closedAt isZero
	var closedAt *time.Time
	if !c.ClosedAt.IsZero() {
		closedUtc := c.ClosedAt.UTC()
		closedAt = &closedUtc
	}
	if _, err := p.changeStmt.Exec(
		c.Id,
		c.CreatedAt.UTC(),
		closedAt,
		c.Open,
		c.NumChanges,
		c.User,
		c.UserId,
		bbox,
		hstoreStringChangeset(c.Tags),
	); err != nil {
		if _, err := p.tx.Exec("ROLLBACK TO SAVEPOINT insert_changeset"); err != nil {
			return err
		}
		if _, err := p.changeUpdateStmt.Exec(
			c.Id,
			c.CreatedAt.UTC(),
			closedAt,
			c.Open,
			c.NumChanges,
			c.User,
			c.UserId,
			bbox,
			hstoreStringChangeset(c.Tags),
		); err != nil {
			return newSqlError(err, c)
		}
		if _, err := p.tx.Exec(fmt.Sprintf(`DELETE FROM "%[1]s".comments WHERE changeset_id = $1`, p.schema), c.Id); err != nil {
			return err
		}
	}

	for i, com := range c.Comments {
		if _, err := p.commentStmt.Exec(
			c.Id,
			i,
			com.User,
			com.UserId,
			com.Date.UTC(),
			com.Text,
		); err != nil {
			return newSqlError(err, c)
		}
	}

	if _, err := p.tx.Exec("RELEASE SAVEPOINT insert_changeset"); err != nil {
		return err
	}

	return nil
}

var hstoreReplacer = strings.NewReplacer("\\", "\\\\", "\"", "\\\"")

func hstoreString(tags element.Tags) string {
	kv := make([]string, 0, len(tags))
	for k, v := range tags {
		kv = append(kv, `"`+hstoreReplacer.Replace(k)+`"=>"`+hstoreReplacer.Replace(v)+`"`)
	}
	return strings.Join(kv, ", ")
}

func hstoreStringChangeset(tags []changeset.Tag) string {
	kv := make([]string, 0, len(tags))
	for _, t := range tags {
		kv = append(kv, `"`+hstoreReplacer.Replace(t.Key)+`"=>"`+hstoreReplacer.Replace(t.Value)+`"`)
	}
	return strings.Join(kv, ", ")
}

func bboxPolygon(c changeset.Changeset) interface{} {
	if c.MinLon != 0.0 && c.MaxLon != 0.0 && c.MinLat != 0.0 && c.MaxLat != 0.0 {
		return fmt.Sprintf(
			"SRID=4326; POLYGON((%[1]f %[2]f, %[1]f %[4]f, %[3]f %[4]f, %[3]f %[2]f, %[1]f %[2]f))",
			c.MinLon, c.MinLat, c.MaxLon, c.MaxLat,
		)
	}
	return nil
}
