package database

import (
	"bytes"
	"database/sql"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/lib/pq"
	"github.com/pkg/errors"

	osm "github.com/omniscale/go-osm"
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
	`CREATE TABLE IF NOT EXISTS "%[1]s".members (
    relation_id INT NOT NULL,
    relation_version INT,
    type VARCHAR,
    role VARCHAR,
    idx INT,
    member_node_id BIGINT,
    member_way_id INT,
    member_relation_id INT,
    PRIMARY KEY (relation_id, relation_version, idx),
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

var initIndexSql = []struct {
	name string
	sql  string
}{
	{"nodes_geometry_idx", `CREATE INDEX "%[2]s" ON "%[1]s".nodes USING GIST (geometry);`},
	{"ways_changset_idx", `CREATE INDEX "%[2]s" ON "%[1]s".ways USING btree (changeset);`},
	{"relations_changset_idx", `CREATE INDEX "%[2]s" ON "%[1]s".relations USING btree (changeset);`},
	{"changesets_bbox_idx", `CREATE INDEX "%[2]s" ON "%[1]s".changesets USING GIST (bbox);`},
}

type PostGIS struct {
	db               *sql.DB
	tx               *sql.Tx
	nodeStmt         *sql.Stmt
	wayStmt          *sql.Stmt
	wayLimitStmt     *sql.Stmt
	ndsStmt          *sql.Stmt
	relStmt          *sql.Stmt
	relLimitStmt     *sql.Stmt
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

	// CREATE INDEX IF NOT EXISTS is only supported for PostgreSQL >= 9.5
	// do manual check
	for _, s := range initIndexSql {
		row := tx.QueryRow(`SELECT COUNT(*) > 0 FROM pg_indexes WHERE schemaname = $1 AND indexname = $2`, p.schema, s.name)
		var exists bool
		err := row.Scan(&exists)
		if err != nil {
			return err
		}
		if !exists {
			idxStmt := fmt.Sprintf(s.sql, p.schema, s.name)
			if _, err := tx.Exec(idxStmt); err != nil {
				tx.Rollback()
				return fmt.Errorf("error while calling %v: %v", idxStmt, err)
			}
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
	    tags) SELECT $1, $2, $3, $4, ST_SetSRID(ST_Point($5, $6), 4326), $7, $8, $9, $10, $11, $12
	WHERE NOT EXISTS (
		SELECT 1 FROM "%[1]s".nodes where id = $1 AND version = $10
	)`, p.schema),
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

	wayLimitStmt, err := p.tx.Prepare(
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
        ) SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, $10
	WHERE EXISTS (
		SELECT 1 FROM "%[1]s".nodes WHERE id = ANY ($11)
	)
	AND NOT EXISTS (
		SELECT 1 FROM "%[1]s".ways WHERE id = $1 AND version = $8
	)
	`, p.schema),
	)
	if err != nil {
		return err
	}
	p.wayLimitStmt = wayLimitStmt

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

	relLimitStmt, err := p.tx.Prepare(
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
        ) SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, $10
	WHERE (
	    EXISTS (SELECT 1 FROM "%[1]s".ways WHERE id = ANY($11) LIMIT 1)
	 OR EXISTS (SELECT 1 FROM "%[1]s".nodes WHERE id = ANY($12) LIMIT 1)
	 OR EXISTS (SELECT 1 FROM "%[1]s".relations WHERE id = ANY($13) LIMIT 1)
	) AND NOT EXISTS (
		SELECT 1 FROM "%[1]s".relations WHERE id = $1 AND version = $8
	)
	`, p.schema),
	)
	if err != nil {
		return err
	}
	p.relLimitStmt = relLimitStmt

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
	err := p.tx.Commit()
	p.tx = nil
	return err
}

func (p *PostGIS) Close() error {
	if p.tx != nil {
		if err := p.tx.Rollback(); err != nil {
			p.db.Close()
			return err
		}
		p.tx = nil
	}
	return p.db.Close()
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
	for _, table := range []string{"nodes", "ways", "relations"} {
		stmt := fmt.Sprintf(`
DELETE FROM "%[1]s".%[2]s WHERE changeset IN (`+changesetsStmt+`)`,
			p.schema, table,
		)
		r, err := tx.Exec(stmt, bbox[0], bbox[1], bbox[2], bbox[3])
		if err != nil {
			tx.Rollback()
			return errors.Wrap(err, "cleanup elements")
		}
		rows, err := r.RowsAffected()
		if err != nil {
			tx.Rollback()
			return err
		}
		log.Printf("debug: removed %d from %s", rows, table)
	}
	return tx.Commit()
}

func (p *PostGIS) CleanupChangesets(bbox [4]float64, olderThen time.Duration) error {
	tx, err := p.db.Begin()
	if err != nil {
		return err
	}
	before := time.Now().Add(-olderThen).UTC()

	stmt := fmt.Sprintf(`
DELETE FROM "%[1]s".changesets
WHERE closed_at < $5
AND NOT (bbox && ST_MakeEnvelope($1, $2, $3, $4))
`,
		p.schema,
	)
	r, err := tx.Exec(stmt, bbox[0], bbox[1], bbox[2], bbox[3], before)
	if err != nil {
		tx.Rollback()
		return errors.Wrap(err, "cleanup changesets")
	}
	rows, err := r.RowsAffected()
	if err != nil {
		tx.Rollback()
		return err
	}
	log.Printf("debug: removed %d from changesets", rows)
	return tx.Commit()
}

// ImportElem imports a single Diff element.
// Ways and relations are only imported if at least one referenced node (or way) is
// included in the database. This keeps the database sparse for imports with limitto.
func (p *PostGIS) ImportElem(elem osm.Diff) (err error) {
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
	if elem.Modify {
		mod = true
	} else if elem.Create {
		add = true
	} else if elem.Delete {
		del = true
	}
	if elem.Node != nil {
		nd := elem.Node
		if _, err = p.nodeStmt.Exec(
			nd.ID,
			add,
			mod,
			del,
			nd.Long, nd.Lat,
			nd.Metadata.UserName,
			nd.Metadata.UserID,
			nd.Metadata.Timestamp.UTC(),
			nd.Metadata.Version,
			nd.Metadata.Changeset,
			hstoreString(nd.Tags),
		); err != nil {
			return errors.Wrapf(err, "importing node %v", elem.Node)
		}
	} else if elem.Way != nil {
		w := elem.Way
		res, err := p.wayLimitStmt.Exec(
			w.ID,
			add,
			mod,
			del,
			w.Metadata.UserName,
			w.Metadata.UserID,
			w.Metadata.Timestamp.UTC(),
			w.Metadata.Version,
			w.Metadata.Changeset,
			hstoreString(w.Tags),
			pq.Array(w.Refs),
		)
		if err != nil {
			return errors.Wrapf(err, "importing way %v", elem.Way)
		}
		if n, _ := res.RowsAffected(); n == 1 {
			for i, ref := range elem.Way.Refs {
				if _, err = p.ndsStmt.Exec(
					w.ID,
					w.Metadata.Version,
					i,
					ref,
				); err != nil {
					return errors.Wrapf(err, "importing nds %v of way %v", ref, elem.Way)
				}
			}
		}
	} else if elem.Rel != nil {
		rel := elem.Rel
		nodeRefs := make([]int64, 0)
		wayRefs := make([]int64, 0, len(rel.Members)) // most relations have way members
		relRefs := make([]int64, 0)
		for _, m := range rel.Members {
			if m.Type == osm.NODE {
				nodeRefs = append(nodeRefs, m.ID)
			}
			if m.Type == osm.WAY {
				wayRefs = append(wayRefs, m.ID)
			}
			if m.Type == osm.RELATION {
				relRefs = append(relRefs, m.ID)
			}
		}
		res, err := p.relLimitStmt.Exec(
			rel.ID,
			add,
			mod,
			del,
			rel.Metadata.UserName,
			rel.Metadata.UserID,
			rel.Metadata.Timestamp.UTC(),
			rel.Metadata.Version,
			rel.Metadata.Changeset,
			hstoreString(rel.Tags),
			pq.Array(wayRefs),
			pq.Array(nodeRefs),
			pq.Array(relRefs),
		)
		if err != nil {
			return errors.Wrapf(err, "importing relation %v", elem.Rel)
		}
		if n, _ := res.RowsAffected(); n == 1 {
			for i, m := range elem.Rel.Members {
				var nodeID, wayID, relID interface{}
				switch m.Type {
				case osm.NODE:
					nodeID = m.ID
				case osm.WAY:
					wayID = m.ID
				case osm.RELATION:
					relID = m.ID
				}
				if _, err = p.memberStmt.Exec(
					rel.ID,
					rel.Metadata.Version,
					m.Type,
					m.Role,
					i,
					nodeID,
					wayID,
					relID,
				); err != nil {
					return errors.Wrapf(err, "importing member %v of relation %v", m, elem.Rel)
				}
			}
		}
	}

	return nil
}

// ImportNodes inserts all Nodes in a single COPY statement.
func (p *PostGIS) ImportNodes(nds []osm.Node) (err error) {
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
	nodeStmt, err := p.tx.Prepare(
		fmt.Sprintf(`COPY "%[1]s".nodes (
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
            tags) FROM STDIN`, p.schema),
	)
	if err != nil {
		return err
	}
	for _, nd := range nds {
		wkb, err := EWKBHex(nd, 4326)
		if _, err = nodeStmt.Exec(
			nd.ID,
			true,
			false,
			false,
			string(wkb),
			nd.Metadata.UserName,
			nd.Metadata.UserID,
			nd.Metadata.Timestamp.UTC(),
			nd.Metadata.Version,
			nd.Metadata.Changeset,
			hstoreString(nd.Tags),
		); err != nil {
			return errors.Wrapf(err, "importing node %v", nd)
		}
	}
	if _, err := nodeStmt.Exec(); err != nil {
		return err
	}
	return nodeStmt.Close()
}

// ImportWays inserts all Ways in a single COPY statement.
func (p *PostGIS) ImportWays(ws []osm.Way) (err error) {
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
	wayStmt, err := p.tx.Prepare(
		fmt.Sprintf(`COPY "%[1]s".ways (
            id,
            add,
            modify,
            delete,
            user_name,
            user_id,
            timestamp,
            version,
            changeset,
            tags) FROM STDIN`, p.schema),
	)
	if err != nil {
		return err
	}
	for _, w := range ws {
		if _, err = wayStmt.Exec(
			w.ID,
			true,
			false,
			false,
			w.Metadata.UserName,
			w.Metadata.UserID,
			w.Metadata.Timestamp.UTC(),
			w.Metadata.Version,
			w.Metadata.Changeset,
			hstoreString(w.Tags),
		); err != nil {
			return errors.Wrapf(err, "importing way %v", ws)
		}
	}
	if _, err := wayStmt.Exec(); err != nil {
		return err
	}
	if err := wayStmt.Close(); err != nil {
		return err
	}

	ndsStmt, err := p.tx.Prepare(
		fmt.Sprintf(`COPY "%[1]s".nds (
            way_id,
            way_version,
            idx,
	    node_id) FROM STDIN`, p.schema),
	)
	if err != nil {
		return err
	}
	for _, w := range ws {
		for i, ref := range w.Refs {
			if _, err = ndsStmt.Exec(
				w.ID,
				w.Metadata.Version,
				i,
				ref,
			); err != nil {
				return errors.Wrapf(err, "importing nds %v of way %v", ref, w)
			}
		}
	}
	if _, err := ndsStmt.Exec(); err != nil {
		return err
	}
	return ndsStmt.Close()
}

// ImportRelations inserts all Relations in a single COPY statement.
func (p *PostGIS) ImportRelations(rs []osm.Relation) (err error) {
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
	relStmt, err := p.tx.Prepare(
		fmt.Sprintf(`COPY "%[1]s".relations (
            id,
            add,
            modify,
            delete,
            user_name,
            user_id,
            timestamp,
            version,
            changeset,
            tags) FROM STDIN`, p.schema),
	)
	if err != nil {
		return err
	}

	for _, rel := range rs {
		if _, err = relStmt.Exec(
			rel.ID,
			true,
			false,
			false,
			rel.Metadata.UserName,
			rel.Metadata.UserID,
			rel.Metadata.Timestamp.UTC(),
			rel.Metadata.Version,
			rel.Metadata.Changeset,
			hstoreString(rel.Tags),
		); err != nil {
			return errors.Wrap(err, "importing relations")
		}
	}

	if _, err := relStmt.Exec(); err != nil {
		return errors.Wrap(err, "importing relations")
	}
	if err := relStmt.Close(); err != nil {
		return errors.Wrap(err, "importing relations")
	}

	memberStmt, err := p.tx.Prepare(
		fmt.Sprintf(`COPY "%[1]s".members (
            relation_id,
            relation_version,
            type,
            role,
            idx,
            member_node_id,
            member_way_id,
            member_relation_id
	    ) FROM STDIN`, p.schema),
	)
	if err != nil {
		return err
	}

	for _, rel := range rs {
		for i, m := range rel.Members {
			var nodeID, wayID, relID interface{}
			switch m.Type {
			case osm.NODE:
				nodeID = m.ID
			case osm.WAY:
				wayID = m.ID
			case osm.RELATION:
				relID = m.ID
			}
			if _, err = memberStmt.Exec(
				rel.ID,
				rel.Metadata.Version,
				m.Type,
				m.Role,
				i,
				nodeID,
				wayID,
				relID,
			); err != nil {
				return errors.Wrap(err, "importing relation members")
			}
		}
	}
	if _, err := memberStmt.Exec(); err != nil {
		return errors.Wrap(err, "importing relation members")
	}
	if err := memberStmt.Close(); err != nil {
		return errors.Wrap(err, "importing relations")
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

func (p *PostGIS) ImportChangeset(c osm.Changeset) error {
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
		c.ID,
		c.CreatedAt.UTC(),
		closedAt,
		c.Open,
		c.NumChanges,
		c.UserName,
		c.UserID,
		bbox,
		hstoreString(c.Tags),
	); err != nil {
		if _, err := p.tx.Exec("ROLLBACK TO SAVEPOINT insert_changeset"); err != nil {
			return err
		}
		if _, err := p.changeUpdateStmt.Exec(
			c.ID,
			c.CreatedAt.UTC(),
			closedAt,
			c.Open,
			c.NumChanges,
			c.UserName,
			c.UserID,
			bbox,
			hstoreString(c.Tags),
		); err != nil {
			return errors.Wrapf(err, "updating changeset: %v", c)
		}
		if _, err := p.tx.Exec(fmt.Sprintf(`DELETE FROM "%[1]s".comments WHERE changeset_id = $1`, p.schema), c.ID); err != nil {
			return errors.Wrapf(err, "deleting comments of %v", c)
		}
	}

	for i, com := range c.Comments {
		if _, err := p.commentStmt.Exec(
			c.ID,
			i,
			com.UserName,
			com.UserID,
			com.CreatedAt.UTC(),
			com.Text,
		); err != nil {
			return errors.Wrapf(err, "inserting comments for %v", c)
		}
	}

	if _, err := p.tx.Exec("RELEASE SAVEPOINT insert_changeset"); err != nil {
		return err
	}

	return nil
}

var hstoreReplacer = strings.NewReplacer("\\", "\\\\", "\"", "\\\"")

func hstoreString(tags osm.Tags) string {
	kv := make([]string, 0, len(tags))
	for k, v := range tags {
		kv = append(kv, `"`+hstoreReplacer.Replace(k)+`"=>"`+hstoreReplacer.Replace(v)+`"`)
	}
	return strings.Join(kv, ", ")
}

func bboxPolygon(c osm.Changeset) interface{} {
	if c.MaxExtent != [4]float64{0, 0, 0, 0} {
		return fmt.Sprintf(
			"SRID=4326; POLYGON((%[1]f %[2]f, %[1]f %[4]f, %[3]f %[4]f, %[3]f %[2]f, %[1]f %[2]f))",
			c.MaxExtent[0], c.MaxExtent[1], c.MaxExtent[2], c.MaxExtent[3],
		)
	}
	return nil
}

const (
	wkbSridFlag  = 0x20000000
	wkbPointType = 1
)

func EWKBHex(nd osm.Node, srid int) ([]byte, error) {
	buf := &bytes.Buffer{}
	binary.Write(buf, binary.LittleEndian, uint8(1)) // little endian
	if srid != 0 {
		binary.Write(buf, binary.LittleEndian, uint32(wkbPointType|wkbSridFlag))
		binary.Write(buf, binary.LittleEndian, uint32(srid))
	} else {
		binary.Write(buf, binary.LittleEndian, uint32(wkbPointType))
	}

	binary.Write(buf, binary.LittleEndian, nd.Long)
	binary.Write(buf, binary.LittleEndian, nd.Lat)

	src := buf.Bytes()
	dst := make([]byte, hex.EncodedLen(len(src)))
	hex.Encode(dst, src)
	return dst, nil
}
