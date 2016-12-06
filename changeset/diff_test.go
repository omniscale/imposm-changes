package changeset

import (
	"testing"

	"github.com/omniscale/imposm3/diff/parser"
)

func TestParseDiff(t *testing.T) {
	elems, errc := parser.Parse("612.osc.gz")

	for {
		select {
		case elem, ok := <-elems:
			if !ok {
				elems = nil
				break
			}
			if elem.Way != nil {
				t.Logf("%v", elem.Way.OSMElem.Metadata.Timestamp)
			}
		case err, ok := <-errc:
			if !ok {
				errc = nil
				break
			}
			t.Fatal(err)
		}
		if errc == nil && elems == nil {
			break
		}
	}
}
