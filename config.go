package changes

import (
	"encoding/json"
	"os"
	"time"

	"github.com/omniscale/imposm3/config"
	"github.com/pkg/errors"
)

type Config struct {
	Connection string      `json:"connection"`
	Schemas    Schemas     `json:"schemas"`
	LimitTo    *[4]float64 `json:"limitto"`
	DiffDir    string      `json:"diffdir"`
	ChangesDir string      `json:"changesdir"`

	DiffFromDiffDir   bool                   `json:"replication_from_diffdir"`
	DiffUrl           string                 `json:"replication_url"`
	DiffInterval      config.MinutesInterval `json:"replication_interval"`
	ChangesetUrl      string                 `json:"changeset_url"`
	ChangesetInterval config.MinutesInterval `json:"changeset_interval"`
	InitialHistory    config.MinutesInterval `json:"initial_history"`
}

type Schemas struct {
	Changes string `json:"changes"`
}

func LoadConfig(filename string) (*Config, error) {
	conf := &Config{
		InitialHistory:    config.MinutesInterval{Duration: time.Hour},
		DiffUrl:           "http://planet.openstreetmap.org/replication/minute/",
		DiffInterval:      config.MinutesInterval{Duration: time.Minute},
		ChangesetUrl:      "http://planet.openstreetmap.org/replication/changesets/",
		ChangesetInterval: config.MinutesInterval{Duration: time.Minute},
		Schemas:           Schemas{Changes: "changes"},
	}

	f, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	decoder := json.NewDecoder(f)

	err = decoder.Decode(&conf)
	if err != nil {
		return nil, err
	}
	if conf.Connection == "" {
		return nil, errors.New("missing connection option")
	}

	if conf.DiffDir == "" {
		return nil, errors.New("missing diffdir option")
	}

	if conf.ChangesDir == "" {
		return nil, errors.New("missing changesdir option")
	}

	return conf, nil
}
