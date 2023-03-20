package storages

import (
	"encoding/json"
	"fmt"
	"io"
	"path/filepath"

	"github.com/rprtr258/simpdb"
)

type jsonStorage[E simpdb.Entity] struct {
	// filename of table file
	filename string
	intend   bool
}

func (s *jsonStorage[E]) Filename() string {
	return s.filename
}

func (s *jsonStorage[E]) Read(r io.Reader) (map[string]E, error) {
	var res map[string]E
	if err := json.NewDecoder(r).Decode(&res); err != nil {
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

// func (s *jsonStorage[E]) marshal(entities map[string]E) ([]byte, error) {
// 	if s.intend {
// 		res, err := json.MarshalIndent(entities, "", "\t")
// 		if err != nil {
// 			return nil, fmt.Errorf("write, encode intended json: %w", err)
// 		}

// 		return res, nil
// 	}

// 	res, err := json.Marshal(entities)
// 	if err != nil {
// 		return nil, fmt.Errorf("write, encode intended json: %w", err)
// 	}

// 	return res, nil
// }

type jsonStorageConfig[E simpdb.Entity] struct {
	// indent indicates whether to do json indenting when encoding entities
	indent bool
}

func NewJSONStorage[E simpdb.Entity](indent bool) simpdb.StorageConfig[E] {
	return jsonStorageConfig[E]{
		indent: indent,
	}
}

func (c jsonStorageConfig[E]) Build(dir, tableName string) simpdb.Storage[E] {
	return &jsonStorage[E]{
		filename: filepath.Join(dir, tableName+".json"),
		intend:   c.indent,
	}
}
