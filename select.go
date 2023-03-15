package simpdb

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
