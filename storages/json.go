package storages

import (
	"encoding/json"
	"fmt"
	"io"
	"path/filepath"

	"github.com/rprtr258/simpdb"
)

type jsonStorage[E Entity] struct {
	tableFilename
}

func (s *jsonStorage[E]) Read(r io.Reader) (map[string]E, error) {
	res, err := jsonDecode[E](r)
	if err != nil {
		return nil, fmt.Errorf("json storage decode: %w", err)
	}

	return res, nil
}

func (s *jsonStorage[E]) Write(w io.Writer, entities map[string]E) error {
	if err := json.NewEncoder(w).Encode(entities); err != nil {
		return fmt.Errorf("json storage encode: %w", err)
	}

	return nil
}

func NewJSONStorage[E Entity]() simpdb.StorageConfig[E] {
	return simpdb.FuncStorageConfig[E](
		func(dir, tableName string) simpdb.Storage[E] {
			filename := filepath.Join(dir, tableName+".json")
			return &jsonStorage[E]{tableFilename(filename)}
		},
	)
}
