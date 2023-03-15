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

func (t *jsonStorage[E]) ensureFileExists() error {
	if _, err := os.Stat(t.dir); err != nil {
		if !os.IsNotExist(err) {
			return fmt.Errorf("checking directory %s: %w", t.dir, err)
		}

		// TODO: mkdirall
		if err := os.Mkdir(t.dir, 0755); err != nil {
			return fmt.Errorf("creating directory %s: %w", t.dir, err)
		}
	}

	filename := filepath.Join(t.dir, t.name)

	if _, err := os.Stat(filename); err != nil {
		if !os.IsNotExist(err) {
			return fmt.Errorf("check table file %s: %w", filename, err)
		}

		file, err := os.OpenFile(filename, os.O_CREATE|os.O_RDWR, 0644)
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

// Read all records from table that satisfy predicate.
func (t *jsonStorage[E]) Read(filter func(E) bool) (map[string]E, error) {
	if err := t.ensureFileExists(); err != nil {
		return nil, fmt.Errorf("read: %w", err)
	}

	filename := filepath.Join(t.dir, t.name)

	bytes, err := os.ReadFile(filename)
	if err != nil {
		return nil, fmt.Errorf("read, reading file: %w", err)
	}

	var all map[string]E
	if err := json.Unmarshal(bytes, &all); err != nil {
		return nil, fmt.Errorf("read, decoding data: %w", err)
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
	if err := t.ensureFileExists(); err != nil {
		return fmt.Errorf("write: %w", err)
	}

	bytes, err := t.marshal(entities)
	if err != nil {
		return fmt.Errorf("write, encoding json: %w", err)
	}

	filename := filepath.Join(t.dir, t.name)

	if err := os.WriteFile(filename, bytes, 0644); err != nil {
		return fmt.Errorf("write, writing file: %w", err)
	}

	return nil
}
