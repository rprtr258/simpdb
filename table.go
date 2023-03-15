package simpdb

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

var (
	ErrAlreadyPresent = errors.New("can't insert, entity already exists")
	ErrNoneFound      = errors.New("entity was not found")
)

// Entity is interface for all table entities. Structs must implement it for DB
// to be able to store them. Only entities with different IDs will be stored.
type Entity interface {
	// ID - get ID of an entity. All entities inside table must have unique IDs.
	ID() string
	// TableName - get table name for entity. All tables must have unique name.
	TableName() string
}

// Table is access point for storage of one entity type.
type Table[E Entity] struct {
	// filename with JSON map for the table
	filename string
	// name of entity in the table
	name string
}

func (t *Table[E]) ensureFileExists() error {
	dir := filepath.Dir(t.filename)
	if _, err := os.Stat(dir); err != nil {
		if !os.IsNotExist(err) {
			return fmt.Errorf("failed checking directory %s: %w", dir, err)
		}

		if err := os.Mkdir(dir, 0755); err != nil {
			return fmt.Errorf("failed creating directory %s: %w", dir, err)
		}
	}

	if _, err := os.Stat(t.filename); err != nil {
		if !os.IsNotExist(err) {
			return fmt.Errorf("failed checking table file %s: %w", t.filename, err)
		}

		file, err := os.OpenFile(t.filename, os.O_CREATE|os.O_RDWR, 0644)
		if err != nil {
			return fmt.Errorf("failed creating table file %s: %w", t.filename, err)
		}
		defer file.Close()

		if _, err := file.Write([]byte("{}")); err != nil {
			return fmt.Errorf("failed filling table file %s with inital data: %w", t.filename, err)
		}
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

	if err := t.Write(f(all)); err != nil {
		return fmt.Errorf("update failed: %w", err)
	}

	return nil
}

// GetAll records in table.
func (t *Table[E]) GetAll() (map[string]E, error) {
	return t.Read(func(E) bool { return true })
}

// Get single record by id. If none found and no errors happened returns
// ErrNoneFound as error.
func (t *Table[E]) Get(id string) (E, error) {
	res, err := t.Read(func(entity E) bool { return entity.ID() == id })
	if err != nil {
		var e E
		return e, err
	}

	if len(res) == 0 {
		var e E
		return e, ErrNoneFound
	}

	for _, entity := range res {
		return entity, nil
	}

	panic("unreachable")
}

// GetBy - get all records for which filter returned true.
func (t *Table[E]) GetBy(filter func(E) bool) (map[string]E, error) {
	return t.Read(filter)
}

// Insert entity into database. If entity already present gives ErrAlreadyPresent.
func (t *Table[E]) Insert(entity E) error {
	id := entity.ID()
	alreadyPresent := false
	err := t.Update(func(m map[string]E) map[string]E {
		if _, ok := m[id]; ok {
			alreadyPresent = true
		} else {
			m[id] = entity
		}
		return m
	})
	if err != nil {
		return err
	}

	if alreadyPresent {
		return ErrAlreadyPresent
	}

	return nil
}

// Upsert - insert entity into database. If entity already present, overwrites it.
func (t *Table[E]) Upsert(entity E) error {
	return t.Update(func(m map[string]E) map[string]E {
		m[entity.ID()] = entity
		return m
	})
}

// Delete entity by id. If entity with such id does not exist, does nothing.
func (t *Table[E]) Delete(id string) error {
	return t.Update(func(m map[string]E) map[string]E {
		delete(m, id)
		return m
	})
}

// DeleteBy - delete all entities for which filter returns true.
func (t *Table[E]) DeleteBy(filter func(E) bool) error {
	return t.Update(func(m map[string]E) map[string]E {
		for id, entity := range m {
			if filter(entity) {
				delete(m, id)
			}
		}
		return m
	})
}
