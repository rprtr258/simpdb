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
func (t *Table[E]) Get(id string) Optional[E] {
	res, ok := t.data[id]
	return Optional[E]{
		Value: res,
		Valid: ok,
	}
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
