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

func NewJSONStorage[E Entity](dir, tableName string, indent bool) (Storage[E], error) {
	basename := tableName + ".json"

	filename := filepath.Join(dir, basename)

	return &jsonStorage[E]{
		filename: filename,
		intend:   indent,
	}, nil
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

// Read all records from table file.
func Read[E Entity](storage Storage[E]) (map[string]E, error) {
	if err := ensureFile(storage.Filename()); err != nil {
		var e Entity
		return nil, fmt.Errorf(
			"read, check %T table file %q: %w",
			e, storage.Filename(),
			err,
		)
	}

	f, err := os.Open(storage.Filename())
	if err != nil {
		return nil, fmt.Errorf("read, open file %q: %w", storage.Filename(), err)
	}

	res, err := storage.Read(f)
	if err != nil {
		return nil, fmt.Errorf("read entities: %w", err)
	}

	return res, nil
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

// write all entities to table file.
func write[E Entity](storage Storage[E], entities map[string]E) error {
	if err := ensureFile(storage.Filename()); err != nil {
		var e Entity
		return fmt.Errorf(
			"write, check %T table file %q: %w",
			e, storage.Filename(),
			err,
		)
	}

	file, err := os.Create(storage.Filename())
	if err != nil {
		return fmt.Errorf("write, recreate file: %w", err)
	}
	defer file.Close()

	if err := storage.Write(file, entities); err != nil {
		return fmt.Errorf("write entities: %w", err)
	}

	return nil
}
