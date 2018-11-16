package database

import (
	"bytes"
	"database/sql"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	"github.com/lib/pq"
	"github.com/pkg/errors"

	"github.com/omniscale/go-osm"
)

var initSql = []string{
	`CREATE SCHEMA IF NOT EXISTS "%[1]s";`,
	`
CREATE TABLE IF NOT EXISTS "%[1]s".current_status (
	type VARCHAR UNIQUE,
	sequence INT,
	timestamp TIMESTAMP WITH TIME ZONE
);`,
	`
CREATE TABLE IF NOT EXISTS "%[1]s".nodes (
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
	`
CREATE TABLE IF NOT EXISTS "%[1]s".ways (
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
	`
CREATE TABLE IF NOT EXISTS "%[1]s".nds (
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
	`
CREATE TABLE IF NOT EXISTS "%[1]s".relations (
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
	`
CREATE TABLE IF NOT EXISTS "%[1]s".members (
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
	`
CREATE TABLE IF NOT EXISTS "%[1]s".changesets (
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
	`
CREATE TABLE IF NOT EXISTS "%[1]s".comments (
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
	// geometry index for quick intersection
	{"nodes_geometry_idx", `CREATE INDEX "%[2]s" ON "%[1]s".nodes USING GIST (geometry);`},
	{"changesets_bbox_idx", `CREATE INDEX "%[2]s" ON "%[1]s".changesets USING GIST (bbox);`},

	// changeset IDs to filter elements of a specific changeset
	{"nodes_changset_idx", `CREATE INDEX "%[2]s" ON "%[1]s".nodes USING btree (changeset);`},
	{"ways_changset_idx", `CREATE INDEX "%[2]s" ON "%[1]s".ways USING btree (changeset);`},
	{"relations_changset_idx", `CREATE INDEX "%[2]s" ON "%[1]s".relations USING btree (changeset);`},

	// node_id to get ways affected by a node change
	{"nds_node_idx", `CREATE INDEX "%[2]s" ON "%[1]s".nds USING btree (node_id);`},
}

var errNoTx = errors.New("no open transaction")

type PostGIS struct {
	connection string
	db         *sql.DB
	tx         *sql.Tx
	schema     string

	// prepared statements:
	stmtsPrepared bool

	// insert nodes
	nodeStmt *sql.Stmt
	// insert way->node references
	ndsStmt *sql.Stmt
	// insert way if at least one referenced node is present
	wayLimitStmt *sql.Stmt
	// insert relation if at least one referenced node/way/relation is present
	relLimitStmt *sql.Stmt
	// insert members
	memberStmt *sql.Stmt
	// insert changeset
	changeStmt *sql.Stmt
	// update changeset
	changeUpdateStmt *sql.Stmt
	// insert comment
	commentStmt *sql.Stmt
}

func NewPostGIS(connection string, schema string) (*PostGIS, error) {
	if strings.HasPrefix(connection, "postgis") {
		connection = strings.Replace(connection, "postgis", "postgres", 1)
	}

	if strings.HasPrefix(connection, "postgres: ") {
		connection = connection[len("postgres: "):]
	}

	if schema == "" {
		schema = "public"
	}

	p := &PostGIS{
		connection: connection,
		schema:     schema,
	}

	return p, p.open()
}

func (p *PostGIS) open() error {
	if p.db != nil {
		return nil
	}
	var err error
	p.db, err = sql.Open("postgres", p.connection)
	if err != nil {
		return errors.Wrap(err, "opening db connection")
	}

	return nil
}

// Init creates all tables necessary.
func (p *PostGIS) Init() error {
	if p.tx == nil {
		return errNoTx
	}

	for _, s := range initSql {
		stmt := fmt.Sprintf(s, p.schema)
		if _, err := p.tx.Exec(stmt); err != nil {
			p.rollback()
			return errors.Wrapf(err, "executing DDL statement: %v", stmt)
		}
	}

	for _, statusType := range []string{"changes", "diff"} {
		row := p.tx.QueryRow(
			fmt.Sprintf(`SELECT EXISTS(SELECT 1 FROM "%[1]s".current_status WHERE type = '%[2]s')`, p.schema, statusType),
		)
		var exists bool
		err := row.Scan(&exists)
		if err != nil {
			p.rollback()
			return errors.Wrap(err, "query current_status")
		}
		if !exists {
			if _, err := p.tx.Exec(
				fmt.Sprintf(`INSERT INTO "%[1]s".current_status (type, sequence) VALUES ($1, -1)`,
					p.schema),
				statusType,
			); err != nil {
				p.rollback()
				return errors.Wrap(err, "init current_status")
			}
		}
	}
	return nil
}

// ResetLastState resets sequences in current_status tables.
func (p *PostGIS) ResetLastState() error {
	if p.tx == nil {
		return errNoTx
	}

	for _, statusType := range []string{"changes", "diff"} {
		if _, err := p.tx.Exec(
			fmt.Sprintf(`UPDATE "%[1]s".current_status SET sequence = -1 WHERE type = $1`, p.schema),
			statusType,
		); err != nil {
			p.rollback()
			return errors.Wrap(err, "update current_status")
		}
	}
	return nil
}

// InitIndices builds all predefined indices.
func (p *PostGIS) InitIndices() error {
	if p.tx == nil {
		return errNoTx
	}
	// CREATE INDEX IF NOT EXISTS is only supported for PostgreSQL >= 9.5
	// do manual check
	for _, s := range initIndexSql {
		row := p.tx.QueryRow(`SELECT COUNT(*) > 0 FROM pg_indexes WHERE schemaname = $1 AND indexname = $2`, p.schema, s.name)
		var exists bool
		err := row.Scan(&exists)
		if err != nil {
			return err
		}
		if !exists {
			idxStmt := fmt.Sprintf(s.sql, p.schema, s.name)
			if _, err := p.tx.Exec(idxStmt); err != nil {
				p.rollback()
				return errors.Wrapf(err, "creating index %v", idxStmt)
			}
		}
	}
	return nil
}

// Begin starts a new transaction and prepares all insert statements.
func (p *PostGIS) Begin() error {
	if p.tx != nil {
		return errors.New("transaction already open")
	}
	if err := p.open(); err != nil {
		return err
	}
	var err error
	p.tx, err = p.db.Begin()
	if err != nil {
		return errors.Wrap(err, "creating db transaction")
	}
	return nil
}

func (p *PostGIS) prepareStmts() (err error) {
	if p.tx == nil {
		return errNoTx
	}
	if p.stmtsPrepared {
		return nil
	}
	defer func() {
		if err != nil {
			p.rollback()
		}
	}()

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
		return errors.Wrap(err, "preparing statement")
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
		return errors.Wrap(err, "preparing statement")
	}
	p.ndsStmt = ndsStmt

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
		return errors.Wrap(err, "preparing statement")
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
		return errors.Wrap(err, "preparing statement")
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
		return errors.Wrap(err, "preparing statement")
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
		return errors.Wrap(err, "preparing statement")
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
		return errors.Wrap(err, "preparing statement")
	}
	p.commentStmt = commentStmt

	p.stmtsPrepared = true
	return nil
}

// Commit commit current transaction.
func (p *PostGIS) Commit() error {
	if p.tx == nil {
		return errNoTx
	}
	err := p.tx.Commit()
	p.tx = nil
	p.stmtsPrepared = false
	return err
}

// Close closes database connection. Open transactions are rolled back.
func (p *PostGIS) Close() error {
	if err := p.rollback(); err != nil {
		p.db.Close()
		p.db = nil
		return err
	}
	if p.db == nil {
		return nil
	}
	err := p.db.Close()
	p.db = nil
	return err
}

func (p *PostGIS) rollback() error {
	if p.tx == nil {
		return nil
	}
	p.stmtsPrepared = false
	err := p.tx.Rollback()
	p.tx = nil
	return err

}

// ImportElem imports a single Diff element.
// Ways and relations are only imported if at least one referenced node (or way) is
// included in the database. This keeps the database sparse for imports with limitto.
func (p *PostGIS) ImportElem(elem osm.Diff) (imported bool, err error) {
	if p.tx == nil {
		return false, errNoTx
	}
	if !p.stmtsPrepared {
		if err := p.prepareStmts(); err != nil {
			p.rollback()
			return false, err
		}
	}

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
			return false, errors.Wrapf(err, "importing node %v", elem.Node)
		}
		imported = true
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
			return false, errors.Wrapf(err, "importing way %v", elem.Way)
		}
		if n, _ := res.RowsAffected(); n == 1 {
			imported = true
			for i, ref := range elem.Way.Refs {
				if _, err = p.ndsStmt.Exec(
					w.ID,
					w.Metadata.Version,
					i,
					ref,
				); err != nil {
					return false, errors.Wrapf(err, "importing nds %v of way %v", ref, elem.Way)
				}
			}
		}
	} else if elem.Rel != nil {
		rel := elem.Rel
		nodeRefs := make([]int64, 0)
		wayRefs := make([]int64, 0, len(rel.Members)) // most relations have way members
		relRefs := make([]int64, 0)
		for _, m := range rel.Members {
			if m.Type == osm.NodeMember {
				nodeRefs = append(nodeRefs, m.ID)
			}
			if m.Type == osm.WayMember {
				wayRefs = append(wayRefs, m.ID)
			}
			if m.Type == osm.RelationMember {
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
			return false, errors.Wrapf(err, "importing relation %v", elem.Rel)
		}
		if n, _ := res.RowsAffected(); n == 1 {
			imported = true
			for i, m := range elem.Rel.Members {
				var nodeID, wayID, relID interface{}
				switch m.Type {
				case osm.NodeMember:
					nodeID = m.ID
				case osm.WayMember:
					wayID = m.ID
				case osm.RelationMember:
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
					return false, errors.Wrapf(err, "importing member %v of relation %v", m, elem.Rel)
				}
			}
		}
	}

	return imported, nil
}

// ImportNodes inserts all Nodes in a single COPY statement.
func (p *PostGIS) ImportNodes(nds []osm.Node) (err error) {
	if p.tx == nil {
		return errNoTx
	}
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
		wkb, err := ewkbHexPoint(nd, 4326)
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
	if p.tx == nil {
		return errNoTx
	}
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
	if p.tx == nil {
		return errNoTx
	}
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
			case osm.NodeMember:
				nodeID = m.ID
			case osm.WayMember:
				wayID = m.ID
			case osm.RelationMember:
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
	if p.tx == nil {
		return errNoTx
	}
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

// ImportChangeset imports a single changeset.
func (p *PostGIS) ImportChangeset(c osm.Changeset) error {
	if p.tx == nil {
		return errNoTx
	}
	if !p.stmtsPrepared {
		if err := p.prepareStmts(); err != nil {
			p.rollback()
			return err
		}
	}
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

func ewkbHexPoint(nd osm.Node, srid int) ([]byte, error) {
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
