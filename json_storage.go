package simpdb

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

type jsonStorage[E Entity] struct {
	// filename of table file
	filename string
	intend   bool
}

func newJSONStorage[E Entity](dir, tableName string, indent bool) (*jsonStorage[E], error) {
	basename := tableName + ".json"

	if _, err := os.Stat(dir); err != nil {
		if !os.IsNotExist(err) {
			return nil, fmt.Errorf("check directory %s: %w", dir, err)
		}

		// TODO: mkdirall
		if err := os.Mkdir(dir, 0o755); err != nil {
			return nil, fmt.Errorf("creat directory %s: %w", dir, err)
		}
	}

	filename := filepath.Join(dir, basename)

	if _, err := os.Stat(filename); err != nil {
		if !os.IsNotExist(err) {
			return nil, fmt.Errorf("check table file %s: %w", filename, err)
		}

		file, err := os.OpenFile(filename, os.O_CREATE|os.O_RDWR, 0o600)
		if err != nil {
			return nil, fmt.Errorf("create table file %s: %w", filename, err)
		}
		defer file.Close()

		if _, err := file.Write([]byte("{}")); err != nil {
			return nil, fmt.Errorf("initialize table file %s: %w", filename, err)
		}
	}

	return &jsonStorage[E]{
		filename: filename,
		intend:   indent,
	}, nil
}

// Read all records from table that satisfy predicate.
func (s *jsonStorage[E]) Read() (map[string]E, error) {
	bytes, err := os.ReadFile(s.filename)
	if err != nil {
		return nil, fmt.Errorf("read, read file: %w", err)
	}

	var res map[string]E
	if err := json.Unmarshal(bytes, &res); err != nil {
		return nil, fmt.Errorf("read, decode data: %w", err)
	}

	return res, nil
}

func (s *jsonStorage[E]) marshal(entities map[string]E) ([]byte, error) {
	if s.intend {
		res, err := json.MarshalIndent(entities, "", "\t")
		if err != nil {
			return nil, fmt.Errorf("write, encode intended json: %w", err)
		}

		return res, nil
	}

	res, err := json.Marshal(entities)
	if err != nil {
		return nil, fmt.Errorf("write, encode intended json: %w", err)
	}

	return res, nil
}

// Write fills table with entities.
func (s *jsonStorage[E]) Write(entities map[string]E) error {
	bytes, err := s.marshal(entities)
	if err != nil {
		return err
	}

	if err := os.WriteFile(s.filename, bytes, 0o600); err != nil {
		return fmt.Errorf("write, write file: %w", err)
	}

	return nil
}
