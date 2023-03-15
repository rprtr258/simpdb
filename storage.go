package simpdb

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

type jsonStorage[E Entity] struct {
	intend bool
	// files directory
	dir string
	// name of table file
	name string
}

func newJSONStorage[E Entity](dir, name string, indent bool) (*jsonStorage[E], error) {
	if _, err := os.Stat(dir); err != nil {
		if !os.IsNotExist(err) {
			return nil, fmt.Errorf("check directory %s: %w", dir, err)
		}

		// TODO: mkdirall
		if err := os.Mkdir(dir, 0755); err != nil {
			return nil, fmt.Errorf("creat directory %s: %w", dir, err)
		}
	}

	filename := filepath.Join(dir, name)

	if _, err := os.Stat(filename); err != nil {
		if !os.IsNotExist(err) {
			return nil, fmt.Errorf("check table file %s: %w", filename, err)
		}

		file, err := os.OpenFile(filename, os.O_CREATE|os.O_RDWR, 0644)
		if err != nil {
			return nil, fmt.Errorf("create table file %s: %w", filename, err)
		}
		defer file.Close()

		if _, err := file.Write([]byte("{}")); err != nil {
			return nil, fmt.Errorf("initialize table file %s: %w", filename, err)
		}
	}

	return &jsonStorage[E]{
		dir:    dir,
		name:   name,
		intend: indent,
	}, nil
}

// Read all records from table that satisfy predicate.
func (t *jsonStorage[E]) Read(filter func(E) bool) (map[string]E, error) {
	filename := filepath.Join(t.dir, t.name)

	bytes, err := os.ReadFile(filename)
	if err != nil {
		return nil, fmt.Errorf("read, read file: %w", err)
	}

	var all map[string]E
	if err := json.Unmarshal(bytes, &all); err != nil {
		return nil, fmt.Errorf("read, decode data: %w", err)
	}

	res := make(map[string]E, len(all))
	for id, entity := range all {
		if filter(entity) {
			res[id] = entity
		}
	}

	return res, nil
}

func (t *jsonStorage[E]) marshal(entities map[string]E) ([]byte, error) {
	if t.intend {
		return json.MarshalIndent(entities, "", "\t")
	} else {
		return json.Marshal(entities)
	}
}

// Write fills table with entities.
func (t *jsonStorage[E]) Write(entities map[string]E) error {
	bytes, err := t.marshal(entities)
	if err != nil {
		return fmt.Errorf("write, encode json: %w", err)
	}

	filename := filepath.Join(t.dir, t.name)

	if err := os.WriteFile(filename, bytes, 0644); err != nil {
		return fmt.Errorf("write, write file: %w", err)
	}

	return nil
}
