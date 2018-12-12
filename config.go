package changes

import (
	"encoding/json"
	"os"
	"time"

	"github.com/omniscale/imposm3/config"
	"github.com/pkg/errors"
)

type Config struct {
	Connection string   `json:"connection"`
	Schemas    Schemas  `json:"schemas"`
	LimitTo    *LimitTo `json:"changes_bbox"`
	DiffDir    string   `json:"diffdir"`
	ChangesDir string   `json:"changesdir"`

	DiffFromDiffDir         bool                   `json:"replication_from_diffdir"`
	DiffUrl                 string                 `json:"replication_url"`
	DiffInterval            config.MinutesInterval `json:"replication_interval"`
	ChangesetUrl            string                 `json:"changeset_url"`
	ChangesetInterval       config.MinutesInterval `json:"changeset_interval"`
	ChangesetFromChangesDir bool                   `json:"changeset_from_changesdir"`
	InitialHistory          config.MinutesInterval `json:"initial_history"`
}

type Schemas struct {
	Changes string `json:"changes"`
}

var DefaultConfig = Config{
	InitialHistory:    config.MinutesInterval{Duration: 2 * time.Hour},
	DiffUrl:           "https://planet.openstreetmap.org/replication/minute/",
	DiffInterval:      config.MinutesInterval{Duration: time.Minute},
	ChangesetUrl:      "https://planet.openstreetmap.org/replication/changesets/",
	ChangesetInterval: config.MinutesInterval{Duration: time.Minute},
	Schemas:           Schemas{Changes: "changes"},
}

func LoadConfig(filename string) (*Config, error) {
	conf := Config(DefaultConfig)

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

	return &conf, nil
}
