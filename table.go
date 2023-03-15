package simpdb

import (
	"fmt"
	"sort"
)

// Entity is interface for all table entities. Structs must implement it for DB
// to be able to store them. Only entities with different IDs will be stored.
type Entity interface {
	// ID - get ID of an entity. All entities inside table must have unique IDs.
	ID() string
}

type listQuery[E Entity] struct {
	selectQuery[E]
	less func(E, E) bool
}

// Sort entities list by given less function.
func (q listQuery[E]) Sort(less func(E, E) bool) listQuery[E] {
	return listQuery[E]{
		selectQuery: q.selectQuery,
		less:        less,
	}
}

// Min - get minimal/first entitiy in list. If none, boolean is false.
func (q listQuery[E]) Min() (E, bool) {
	atLeastOneFound := false
	var min E
	for id, entity := range q.data {
		if q.filter(id, entity) {
			if !atLeastOneFound {
				atLeastOneFound = true
				min = entity
			} else if q.less(entity, min) {
				min = entity
			}
		}
	}

	if !atLeastOneFound {
		return min, false
	}

	return min, true
}

// Max - get maximum/last entitiy in list. If none, boolean is false.
func (q listQuery[E]) Max() (E, bool) {
	atLeastOneFound := false
	var max E
	for id, entity := range q.data {
		if q.filter(id, entity) {
			if !atLeastOneFound {
				atLeastOneFound = true
				max = entity
			} else if q.less(max, entity) {
				max = entity
			}
		}
	}

	if !atLeastOneFound {
		return max, false
	}

	return max, true
}

// All - get all entities in list.
func (q listQuery[E]) All() []E {
	res := make([]E, 0, len(q.data))
	for id, entity := range q.data {
		if q.filter(id, entity) {
			res = append(res, entity)
		}
	}
	sort.Slice(res, func(i, j int) bool {
		return q.less(res[i], res[j])
	})
	return res
}

type selectQuery[E Entity] struct {
	data   map[string]E
	filter func(string, E) bool
}

// All - get records in table.
func (q selectQuery[E]) All() map[string]E {
	res := make(map[string]E)
	for id, entity := range q.data {
		if q.filter(id, entity) {
			res[id] = entity
		}
	}
	return res
}

// List - get records in table as list. By default, they are sorted by ID.
func (q selectQuery[E]) List() listQuery[E] {
	return listQuery[E]{
		selectQuery: q,
		less: func(e1, e2 E) bool {
			return e1.ID() < e2.ID()
		},
	}
}

// Filter records matching given filter. Filter accepts id and entity and must
// return true for all entities to keep.
func (q selectQuery[E]) Filter(filter func(string, E) bool) selectQuery[E] {
	return selectQuery[E]{
		data: q.data,
		filter: func(id string, entity E) bool {
			return q.filter(id, entity) && filter(id, entity)
		},
	}
}

// Delete - delete all filtered entities. Returns number of deleted items.
func (q selectQuery[E]) Delete() int {
	deleted := 0
	for id, entity := range q.data {
		if q.filter(id, entity) {
			delete(q.data, id)
			deleted++
		}
	}
	return deleted
}

// Update entities using fn function.
func (q selectQuery[E]) Update(fn func(E) E) {
	for id, entity := range q.data {
		if !q.filter(id, entity) {
			continue
		}

		newEntity := fn(entity)
		newID := newEntity.ID()
		if id != newID {
			delete(q.data, id)
		}
		q.data[newID] = newEntity
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
