package simpdb

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

type jsonStorage[E Entity] struct {
	intend bool
}

func (t *Table[E]) ensureFileExists() error {
	if _, err := os.Stat(t.dir); err != nil {
		if !os.IsNotExist(err) {
			return fmt.Errorf("failed checking directory %s: %w", t.dir, err)
		}

		// TODO: mkdirall
		if err := os.Mkdir(t.dir, 0755); err != nil {
			return fmt.Errorf("failed creating directory %s: %w", t.dir, err)
		}
	}

	filename := filepath.Join(t.dir, t.name)

	if _, err := os.Stat(filename); err != nil {
		if !os.IsNotExist(err) {
			return fmt.Errorf("failed checking table file %s: %w", filename, err)
		}

		file, err := os.OpenFile(filename, os.O_CREATE|os.O_RDWR, 0644)
		if err != nil {
			return fmt.Errorf("failed creating table file %s: %w", filename, err)
		}
		defer file.Close()

		if _, err := file.Write([]byte("{}")); err != nil {
			return fmt.Errorf("failed filling table file %s with inital data: %w", filename, err)
		}
	}

	return nil
}

// Read all records from table that satisfy predicate.
func (t *Table[E]) Read(filter func(E) bool) (map[string]E, error) {
	if err := t.ensureFileExists(); err != nil {
		return nil, fmt.Errorf("read failed: %w", err)
	}

	filename := filepath.Join(t.dir, t.name)

	bytes, err := os.ReadFile(filename)
	if err != nil {
		return nil, fmt.Errorf("read failed while reading file: %w", err)
	}

	var all map[string]E
	if err := json.Unmarshal(bytes, &all); err != nil {
		return nil, fmt.Errorf("read failed while decoding data: %w", err)
	}

	res := make(map[string]E, len(all))
	for id, entity := range all {
		if filter(entity) {
			res[id] = entity
		}
	}

	return res, nil
}

// Write fills table with entities.
func (t *Table[E]) Write(entities map[string]E) error {
	if err := t.ensureFileExists(); err != nil {
		return fmt.Errorf("write failed: %w", err)
	}

	bytes, err := json.Marshal(entities)
	if err != nil {
		return fmt.Errorf("write failed while encoding json: %w", err)
	}

	filename := filepath.Join(t.dir, t.name)

	if err := os.WriteFile(filename, bytes, 0644); err != nil {
		return fmt.Errorf("write failed while writing file: %w", err)
	}

	return nil
}
