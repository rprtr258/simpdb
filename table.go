package simpdb

import (
	"fmt"
)

// Entity is interface for all table entities. Structs must implement it for DB
// to be able to store them. Only entities with different IDs will be stored.
type Entity interface {
	// ID - get ID of an entity. All entities inside table will have unique IDs.
	ID() string
}

// Table is access point for storage of one entity type.
type Table[E Entity] struct {
	selectQuery[E]
	storage Storage[E]
}

func newTable[E Entity](storage Storage[E]) (*Table[E], error) {
	data, err := read(storage)
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

// Flush table, dumps updated data to file.
func (t *Table[E]) Flush() error {
	if err := write(t.storage, t.selectQuery.data); err != nil {
		return fmt.Errorf("flush table: %w", err)
	}

	return nil
}

// Get entity by id.
func (t *Table[E]) Get(id string) (E, bool) {
	res, ok := t.data[id]
	return res, ok
}

// Insert entity into database. If entity already present, does nothing and
// returns false.
func (t *Table[E]) Insert(entity E) bool {
	entityID := entity.ID()
	_, alreadyPresent := t.data[entityID]
	if alreadyPresent {
		return false
	}

	t.data[entityID] = entity

	return true
}

// Upsert - insert entities into database. If entities overlap, overrides old
// one.
func (t *Table[E]) Upsert(entities ...E) {
	for _, entity := range entities {
		t.data[entity.ID()] = entity
	}
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
