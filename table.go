package simpdb

import (
	"fmt"
)

// Entity is interface for all table entities. Structs must implement it for DB
// to be able to store them. Only entities with different IDs will be stored.
type Entity interface {
	// ID - get ID of an entity. All entities inside table must have unique IDs.
	ID() string
}

type selectQuery[E Entity] struct {
	data   map[string]E
	filter func(string, E) bool
}

// All - get records in table.
func (t selectQuery[E]) All() map[string]E {
	res := make(map[string]E)
	for id, entity := range t.data {
		if t.filter(id, entity) {
			res[id] = entity
		}
	}
	return res
}

// Filter records matching given filter. Filter accepts id and entity and must
// return true for all entities to keep.
func (t selectQuery[E]) Filter(filter func(string, E) bool) selectQuery[E] {
	return selectQuery[E]{
		data: t.data,
		filter: func(id string, entity E) bool {
			return t.filter(id, entity) && filter(id, entity)
		},
	}
}

// Delete - delete all filtered entities. Returns number of deleted items.
func (t selectQuery[E]) Delete() int {
	deleted := 0
	for id, entity := range t.data {
		if t.filter(id, entity) {
			delete(t.data, id)
			deleted++
		}
	}
	return deleted
}

// Update entities using fn function.
func (t selectQuery[E]) Update(fn func(E) E) {
	for id, entity := range t.data {
		if !t.filter(id, entity) {
			continue
		}

		newEntity := fn(entity)
		newID := newEntity.ID()
		if id != newID {
			delete(t.data, id)
		}
		t.data[newID] = newEntity
	}
}

// Table is access point for storage of one entity type.
type Table[E Entity] struct {
	selectQuery[E]
	storage *jsonStorage[E]
}

func newTable[E Entity](storage *jsonStorage[E]) (*Table[E], error) {
	data, err := storage.Read()
	if err != nil {
		return nil, fmt.Errorf("new table: %w", err)
	}

	return &Table[E]{
		storage: storage,
		selectQuery: selectQuery[E]{
			data:   data,
			filter: func(s string, e E) bool { return true },
		},
	}, nil
}

// Close table, dumps updated data to file.
func (t *Table[E]) Close() error {
	if err := t.storage.Write(t.selectQuery.data); err != nil {
		return fmt.Errorf("close table: %w", err)
	}

	return nil
}

// Get single record by id. If none found, false returned as second result.
func (t *Table[E]) Get(id string) (E, bool) {
	res, ok := t.data[id]
	return res, ok
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

// DeleteByID - delete entity by id. If entity was not found, does nothing.
// Boolean indicates whether entity was actually deleted.
func (t *Table[E]) DeleteByID(id string) bool {
	_, present := t.data[id]
	if present {
		delete(t.data, id)
		return true
	}

	return false
}
