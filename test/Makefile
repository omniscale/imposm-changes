.PHONY: all test clean files

ifdef VERBOSE
TESTOPTS = -v
else
TESTOPTS =
endif

all: test

clean:
	rm -rf build

PBF_FILES=$(addprefix build/,$(patsubst %.osm,%.pbf,$(wildcard *.osm)))
CHANGES_FILES=$(addprefix build/,$(patsubst %.osmcs,%.osm.gz,$(wildcard *.osmcs)))
OSCGZ_FILES=$(addprefix build/,$(patsubst %.osc,%.osc.gz,$(wildcard *.osc)))

build/%.pbf: %.osm
	@mkdir -p build
	osmosis --read-xml $< --sort type="TypeThenId" --write-pbf $@ 

build/%.osc.gz: %.osc
	@mkdir -p build
	gzip --stdout $< > $@

build/%.osm.gz: %.osmcs
	@mkdir -p build
	gzip --stdout $< > $@

files: $(PBF_FILES) $(OSCGZ_FILES) $(CHANGES_FILES)

test: files
	(cd .. && go test ./test $(TESTOPTS))

