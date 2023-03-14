package simpdb

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/rprtr258/xerr"
)

// Entity is interface for all table entities. Structs must implement it for DB
// to be able to store them. Only entities with different IDs will be stored.
type Entity interface {
	ID() string
	Name() string
}

// Table is access point for storage of one entity type.
type Table[E Entity] struct {
	// filename with JSON map for the table
	filename string
	// name of entity in the table
	name string
}

func (t *Table[E]) ensureFileExists() error {
	if _, err := os.Stat(t.filename); err != nil {
		if !os.IsNotExist(err) {
			return xerr.NewW(err)
		}

		file, err := os.Create(t.filename)
		if err != nil {
			return xerr.NewW(err)
		}

		if _, err := file.Write([]byte("{}")); err != nil {
			return xerr.NewW(err)
		}

		return file.Close()
	}

	return nil
}

// Read all records from table that satisfy predicate.
func (t *Table[E]) Read(filter func(E) bool) (map[string]E, error) {
	if err := t.ensureFileExists(); err != nil {
		return nil, fmt.Errorf("read failed: %w", err)
	}

	bytes, err := os.ReadFile(t.filename)
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

	if err := os.WriteFile(t.filename, bytes, 0644); err != nil {
		return fmt.Errorf("write failed while writing file: %w", err)
	}

	return nil
}

// Update all records in table.
func (t *Table[E]) Update(f func(map[string]E) map[string]E) error {
	all, err := t.Read(func(e E) bool { return true })
	if err != nil {
		return fmt.Errorf("update failed: %w", err)
	}

	if err := t.Write(all); err != nil {
		return fmt.Errorf("update failed: %w", err)
	}

	return nil
}

// GetAll
// Get single by id, returns optional
// GetBy predicate
// Insert - error if exists
// Upsert - rewrite if exists
// Delete by id
// DeleteBy predicate
