Imposm-Changes
==============

Imposm-Changes imports OpenStreetMap changeset information and metadata into a PostgreSQL/PostGIS database.

The resulting database can be used to build change monitoring and QA applications.

Uses an limitations
-------------------

Imposm-Changes only imports OSM change files (.osc.gz) and not complete planet datasets.
You won't be able to construct the geometry of ways or relations if they were only modified or deleted.

You will typically use the ``limitto`` option to monitor a region/country extent. This allows you to monitor the changes of the last few weeks with moderate server requirements (only a few GB disk).


Installation
------------

    go get -u github.com/omniscale/imposm-changes
    go install github.com/omniscale/imposm-changes/cmd/imposm-changes

Run
---

    mkdir imposm-changes
    cd imposm-changes
    cp $GOPATH/src/github.com/omniscale/imposm-changes/config.json ./
    imposm-changes -config config.json


Example queries
---------------

Here are a few example queries, that demonstrate what you can do with the resulting database.
Note that you should always filter changesets ob the bbox as the datasets temporarily contains changesets outside of ``limitto``.


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
        WHERE bbox IS NOT NULL AND bbox && ST_MakeEnvelope(6.8, 50.8, 7.2, 52.6)
        AND closed_at > (NOW() - INTERVAL '8 hours')
    );


Query information about nodes that where added or modified by Open Wheelmap.Org:

    SELECT id, add, modify, version, ST_AsText(geometry), tags, timestamp
    FROM nodes
    WHERE (add OR modify)
    AND geometry && ST_MakeEnvelope(6.8, 50.8, 7.2, 52.6)
    AND changeset IN (
        SELECT id
        FROM changesets
        WHERE bbox IS NOT NULL AND bbox && ST_MakeEnvelope(6.8, 50.8, 7.2, 52.6)
        AND tags->'comment' = 'Modified via wheelmap.org'
    );


Imposm-Change does not contain geometries of ways or relations, but you can look them up from different tables.
The following query returns all new and modified buildings from the last two days. The buildings are loaded from the `osm_buildings` table, which was imported by Imposm3.

    SELECT name, geometry
    FROM osm_buildings
    WHERE osm_id IN (
        SELECT id
        FROM ways
        WHERE (add OR modify)
        AND changeset IN (
            SELECT id
            FROM changesets
            WHERE bbox IS NOT NULL AND bbox && ST_MakeEnvelope(5.7, 50.2, 9.6, 52.6)
            AND created_at > (NOW() - INTERVAL '2 days')
        )
    );

Make sure these queries are fast enough before you run them in production. For example, you should have an index on ``osm_id`` in the ``osm_buildings`` table to avoid a slow table scan. You could also filter the ways with ``tags ? 'buildings'`` etc.
