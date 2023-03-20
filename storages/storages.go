package storages

import (
	"encoding/json"
	"fmt"
	"io"

	"github.com/rprtr258/simpdb"
)

type Entity = simpdb.Entity

func jsonDecode[E Entity](r io.Reader) (map[string]E, error) {
	var res map[string]E
	if err := json.NewDecoder(r).Decode(&res); err != nil {
		return nil, fmt.Errorf("json storage decode: %w", err)
	}

	return res, nil
}

type tableFilename string

func (f tableFilename) Filename() string {
	return string(f)
}
