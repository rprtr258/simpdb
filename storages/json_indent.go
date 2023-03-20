package storages

import (
	"encoding/json"
	"fmt"
	"io"
	"path/filepath"

	"github.com/rprtr258/simpdb"
)

type jsonIndentStorage[E Entity] struct {
	tableFilename
}

func (s *jsonIndentStorage[E]) Read(r io.Reader) (map[string]E, error) {
	res, err := jsonDecode[E](r)
	if err != nil {
		return nil, fmt.Errorf("indent json storage decode: %w", err)
	}

	return res, nil
}

func (s *jsonIndentStorage[E]) Write(w io.Writer, entities map[string]E) error {
	encoder := json.NewEncoder(w)
	encoder.SetIndent("", "\t")
	if err := encoder.Encode(entities); err != nil {
		return fmt.Errorf("indent json storage encode: %w", err)
	}

	return nil
}

func NewJSONIndentStorage[E Entity]() simpdb.StorageConfig[E] {
	return simpdb.FuncStorageConfig[E](
		func(dir, tableName string) simpdb.Storage[E] {
			filename := filepath.Join(dir, tableName+".json")
			return &jsonIndentStorage[E]{tableFilename(filename)}
		},
	)
}
