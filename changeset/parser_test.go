package changeset

import "testing"

func TestParse(t *testing.T) {
	changes, err := Parse("999.osm.gz")
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range changes {
		t.Log(c)
	}
}
