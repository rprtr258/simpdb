package simpdb

import (
	"fmt"
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
	storage *jsonStorage[E]
	data    map[string]E
}

func newTable[E Entity](storage *jsonStorage[E]) (*Table[E], error) {
	data, err := storage.Read()
	if err != nil {
		return nil, fmt.Errorf("new table: %w", err)
	}

	return &Table[E]{
		storage: storage,
		data:    data,
	}, nil
}

// Close table, dumps updated data to file.
func (t *Table[E]) Close() error {
	if err := t.storage.Write(t.data); err != nil {
		return fmt.Errorf("close table: %w", err)
	}

	return nil
}

// Update all records in table.
func (t *Table[E]) Update(f func(map[string]E) map[string]E) {
	t.data = f(t.data)
}

// GetAll records in table.
func (t *Table[E]) GetAll() map[string]E {
	return t.data
}

// Get single record by id. If none found, false returned as second result.
func (t *Table[E]) Get(id string) (E, bool) {
	res, ok := t.data[id]
	return res, ok
}

// Filter - get all records for which filter returned true.
func (t *Table[E]) Filter(by func(E) bool) map[string]E {
	res := make(map[string]E)
	for id, entity := range t.data {
		if by(entity) {
			res[id] = entity
		}
	}
	return res
}

// Insert entity into database. If entity already present, does nothing and
// returns false.
func (t *Table[E]) Insert(entity E) bool {
	id := entity.ID()
	_, alreadyPresent := t.data[id]
	if alreadyPresent {
		return false
	}

	t.data[id] = entity
	return true
}

// Upsert - insert entity into database. If entity already present, overwrites it.
func (t *Table[E]) Upsert(entity E) {
	id := entity.ID()
	t.data[id] = entity
}

// Delete entity by id. If entity was not found, does nothing. Boolean indicates
// whether entity was actually deleted.
func (t *Table[E]) Delete(id string) bool {
	_, present := t.data[id]
	if present {
		delete(t.data, id)
		return true
	}

	return false
}

// DeleteBy - delete all entities for which filter returns true. Returns number
// of deleted items.
func (t *Table[E]) DeleteBy(filter func(E) bool) int {
	deleted := 0
	for id, entity := range t.data {
		if filter(entity) {
			delete(t.data, id)
			deleted++
		}
	}
	return deleted
}
