package storages

import (
	"fmt"
	"io"
	"path/filepath"

	"gopkg.in/yaml.v3"

	"github.com/rprtr258/simpdb"
)

type yamlStorage[E simpdb.Entity] struct {
	// filename of table file
	filename string
}

func (s *yamlStorage[E]) Filename() string {
	return s.filename
}

func (s *yamlStorage[E]) Read(r io.Reader) (map[string]E, error) {
	var res map[string]E
	if err := yaml.NewDecoder(r).Decode(&res); err != nil {
		return nil, fmt.Errorf("json storage decode: %w", err)
	}

	return res, nil
}

func (s *yamlStorage[E]) Write(w io.Writer, entities map[string]E) error {
	if err := yaml.NewEncoder(w).Encode(entities); err != nil {
		return fmt.Errorf("json storage encode: %w", err)
	}

	return nil
}

type yamlStorageConfig[E simpdb.Entity] struct{}

func NewYAMLStorage[E simpdb.Entity]() simpdb.StorageConfig[E] {
	return yamlStorageConfig[E]{}
}

func (c yamlStorageConfig[E]) Build(dir, tableName string) simpdb.Storage[E] {
	return &yamlStorage[E]{
		filename: filepath.Join(dir, tableName+".json"),
	}
}
