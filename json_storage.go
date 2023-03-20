package simpdb

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

type jsonStorage[E Entity] struct {
	// filename of table file
	filename string
	intend   bool
}

func (s *jsonStorage[E]) Filename() string {
	return s.filename
}

func ensureFile(filename string) error {
	dir := filepath.Dir(filename)

	if _, err := os.Stat(dir); err != nil {
		if !os.IsNotExist(err) {
			return fmt.Errorf("check directory %s: %w", dir, err)
		}

		// TODO: mkdirall
		if err := os.Mkdir(dir, 0o755); err != nil {
			return fmt.Errorf("create directory %s: %w", dir, err)
		}
	}

	if _, err := os.Stat(filename); err != nil {
		if !os.IsNotExist(err) {
			return fmt.Errorf("check table file %s: %w", filename, err)
		}

		file, err := os.OpenFile(filename, os.O_CREATE|os.O_RDWR, 0o600)
		if err != nil {
			return fmt.Errorf("create table file %s: %w", filename, err)
		}
		defer file.Close()

		if _, err := file.Write([]byte("{}")); err != nil {
			return fmt.Errorf("initialize table file %s: %w", filename, err)
		}
	}

	return nil
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

type jsonStorageConfig[E Entity] struct {
	// indent indicates whether to do json indenting when encoding entities
	indent bool
}

func NewJSONStorage[E Entity](indent bool) StorageConfig[E] {
	return jsonStorageConfig[E]{
		indent: indent,
	}
}

func (c jsonStorageConfig[E]) Build(dir, tableName string) Storage[E] {
	return &jsonStorage[E]{
		filename: filepath.Join(dir, tableName+".json"),
		intend:   c.indent,
	}
}
