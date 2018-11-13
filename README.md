Imposm-Changes
==============

Imposm-Changes imports OpenStreetMap changeset information and metadata into a PostgreSQL/PostGIS database.

The resulting database can be used to build change monitoring and QA applications.

Uses an limitations
-------------------

Imposm-Changes imports planet dumps and extracts (.osm.pbf), diff files (.osc.gz) and changeset files (.osm.gz).

It should work for whole planet imports but is not optimized for. You will
typically use the ``changes_bbox`` option to monitor a smaller region/country
extent. This allows you to monitor the changes of the last few weeks with
moderate server requirements.


Installation
------------

    go get -u github.com/omniscale/imposm-changes
    go install github.com/omniscale/imposm-changes/cmd/imposm-changes

Run
---

    mkdir imposm-changes
    cd imposm-changes
    cp $GOPATH/src/github.com/omniscale/imposm-changes/config.json ./

    # Edit config.json with database connection and changes_bbox.

    # Call `import` to make initial import.
    imposm-changes -config config.json import path/to/initial.osm.pbf

    # Call `run` to continuously download and import diff and changes files. 
    # Start this via systemctl or similar in production.
    # Make sure initial_history in config.json is large enough to cover all
    # changes since the creation of your initial osm.pbf file.
    imposm-changes -config config.json run


Example queries
---------------

Here are a few example queries, that demonstrate what you can do with the resulting database.

Count changes in the last 8 hours within a bounding box:

    SELECT count(*) FROM changesets
    WHERE bbox IS NOT NULL AND bbox && ST_MakeEnvelope(6.8, 50.8, 7.2, 52.6)
    AND closed_at > (NOW() - INTERVAL '8 hours');


Count how many nodes where added, modified or deleted:

    SELECT sum(case when add then 1 else 0 end) AS add,
           sum(case when modify then 1 else 0 end) AS modify,
           sum(case when delete then 1 else 0 end) AS delete
    FROM nodes
    WHERE changeset IN (
        SELECT id
        FROM changesets
        WHERE closed_at > (NOW() - INTERVAL '8 hours')
    );


Query information about nodes that where added or modified by Open Wheelmap.Org:

    SELECT id, add, modify, version, ST_AsText(geometry), tags, timestamp
    FROM nodes
    WHERE (add OR modify)
    AND changeset IN (
        SELECT id
        FROM changesets
        WHERE tags->'comment' = 'Modified via wheelmap.org'
    );


