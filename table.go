package simpdb

import (
	"errors"
	"fmt"
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
	jsonStorage[E]
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
