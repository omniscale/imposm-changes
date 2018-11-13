package changes

import (
	"reflect"
	"testing"

	osm "github.com/omniscale/go-osm"
)

func TestFilterNodes(t *testing.T) {
	for _, tc := range []struct {
		bbox        LimitTo
		nodes       []osm.Node
		want        []osm.Node
		numInserted int
	}{
		{
			bbox:        [4]float64{0, 0, 10, 10},
			nodes:       []osm.Node{},
			want:        []osm.Node{},
			numInserted: 0,
		},
		{
			bbox:        [4]float64{0, 0, 10, 10},
			nodes:       []osm.Node{{Long: 0, Lat: 0, Element: osm.Element{ID: 1}}},
			want:        []osm.Node{{Long: 0, Lat: 0, Element: osm.Element{ID: 1}}},
			numInserted: 1,
		},
		{
			bbox:        [4]float64{0, 0, 10, 10},
			nodes:       []osm.Node{{Long: -0.0001, Lat: 0, Element: osm.Element{ID: 1}}},
			want:        []osm.Node{},
			numInserted: 0,
		},
		{
			bbox:        [4]float64{0, 0, 10, 10},
			nodes:       []osm.Node{{Long: 1, Lat: 10.01, Element: osm.Element{ID: 1}}},
			want:        []osm.Node{},
			numInserted: 0,
		},
		{
			bbox: [4]float64{0, 0, 10, 10},
			nodes: []osm.Node{
				{Long: 1, Lat: 1, Element: osm.Element{ID: 1}},
				{Long: 20, Lat: 0, Element: osm.Element{ID: 2}},
				{Long: 2, Lat: 2, Element: osm.Element{ID: 3}},
			},
			want: []osm.Node{
				{Long: 1, Lat: 1, Element: osm.Element{ID: 1}},
				{Long: 2, Lat: 2, Element: osm.Element{ID: 3}},
			},
			numInserted: 2,
		},
		{
			bbox: [4]float64{0, 0, 10, 10},
			nodes: []osm.Node{
				{Long: 1, Lat: 1, Element: osm.Element{ID: 1}},
				{Long: 2, Lat: 2, Element: osm.Element{ID: 2}},
				{Long: 20, Lat: 2, Element: osm.Element{ID: 3}},
			},
			want: []osm.Node{
				{Long: 1, Lat: 1, Element: osm.Element{ID: 1}},
				{Long: 2, Lat: 2, Element: osm.Element{ID: 2}},
			},
			numInserted: 2,
		},
	} {
		insertedNodes := map[int64]struct{}{}
		got := filterNodes(tc.nodes, &tc.bbox, insertedNodes)
		if !reflect.DeepEqual(got, tc.want) {
			t.Errorf("unexpected result got:\n\t%v\nwant:\n\t%v", got, tc.want)
		}
		if len(insertedNodes) != tc.numInserted {
			t.Errorf("expected %d entries in insertedNodes, got %d", tc.numInserted, len(insertedNodes))
		}
	}
}
