package simpdb

type selectQuery[E Entity] struct {
	data   map[string]E
	filter func(string, E) bool
}

// Iter over selected entities. fn accepts ID and entity and returns whether
// iteration should continue. There are no order
// guarantees.
func (q selectQuery[E]) Iter(fn func(string, E) bool) {
	for id, entity := range q.data {
		if q.filter(id, entity) {
			if !fn(id, entity) {
				return
			}
		}
	}
}

// All - get records in table.
func (q selectQuery[E]) All() map[string]E {
	res := make(map[string]E)
	q.Iter(func(id string, entity E) bool {
		res[id] = entity
		return true
	})

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

// Sort entities using given function.
func (q selectQuery[E]) Sort(less func(E, E) bool) listQuery[E] {
	return listQuery[E]{
		selectQuery: q,
		less:        less,
	}
}

// Where - get records matching given filter. Where accepts id and entity and
// must return true for all entities to keep.
func (q selectQuery[E]) Where(filter func(string, E) bool) selectQuery[E] {
	return selectQuery[E]{
		data: q.data,
		filter: func(id string, entity E) bool {
			return q.filter(id, entity) && filter(id, entity)
		},
	}
}

// Delete - delete all filtered entities. Returns IDs of deleted items.
func (q selectQuery[E]) Delete() []E {
	deleted := []E{}
	q.Iter(func(id string, entity E) bool {
		delete(q.data, id)
		deleted = append(deleted, entity)
		return true
	})

	return deleted
}

// Count entities matching filter.
func (q selectQuery[E]) Count() int {
	res := 0
	q.Iter(func(string, E) bool {
		res++
		return true
	})

	return res
}

// Update entities using fn function.
func (q selectQuery[E]) Update(fn func(E) E) {
	q.Iter(func(id string, entity E) bool {
		newEntity := fn(entity)
		newID := newEntity.ID()
		if id != newID {
			delete(q.data, id)
		}
		q.data[newID] = newEntity

		return true
	})
}
