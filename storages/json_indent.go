package storages

import (
	"encoding/json"
	"fmt"
	"io"
	"path/filepath"

	"github.com/rprtr258/simpdb"
)

type jsonIndentStorage[E simpdb.Entity] struct {
	// filename of table file
	filename string
}

func (s *jsonIndentStorage[E]) Filename() string {
	return s.filename
}

func (s *jsonIndentStorage[E]) Read(r io.Reader) (map[string]E, error) {
	var res map[string]E
	if err := json.NewDecoder(r).Decode(&res); err != nil {
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

type jsonIndentStorageConfig[E simpdb.Entity] struct{}

func NewJSONIndentStorage[E simpdb.Entity]() simpdb.StorageConfig[E] {
	return jsonIndentStorageConfig[E]{}
}

func (c jsonIndentStorageConfig[E]) Build(dir, tableName string) simpdb.Storage[E] {
	return &jsonIndentStorage[E]{
		filename: filepath.Join(dir, tableName+".json"),
	}
}
