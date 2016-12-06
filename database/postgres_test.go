package database

import "testing"

func TestSchema(t *testing.T) {
	p, err := NewPostGIS("sslmode=disable", "changes")
	if err != nil {
		t.Fatal(err)
	}

	if err := p.Init(); err != nil {
		t.Fatal(err)
	}
}
