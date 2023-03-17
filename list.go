package simpdb

import "sort"

type listQuery[E Entity] struct {
	selectQuery[E]
	less func(E, E) bool
}

// iter over list of entities no order guaranteed. fn accepts ID and entity and
// returns whether iteration should continue.
func (q listQuery[E]) iter(fn func(string, E) bool) {
	for id, entity := range q.data {
		if q.filter(id, entity) {
			if !fn(id, entity) {
				return
			}
		}
	}
}

// Iter over list of entities in sorted order. fn accepts ID and entity and
// returns whether iteration should continue.
func (q listQuery[E]) Iter(iteratee func(string, E) bool) {
	res := make([]E, 0, len(q.data))
	q.iter(func(_ string, entity E) bool {
		res = append(res, entity)
		return true
	})
	sort.Slice(res, func(i, j int) bool {
		return q.less(res[i], res[j])
	})
	for _, entity := range res {
		id := entity.ID()
		if q.filter(id, entity) {
			if !iteratee(id, entity) {
				return
			}
		}
	}
}

// Sort entities list by given less function.
func (q listQuery[E]) Sort(less func(E, E) bool) listQuery[E] {
	return listQuery[E]{
		selectQuery: q.selectQuery,
		less:        less,
	}
}

// Min - get minimal/first entitiy in list. If none, boolean is false.
func (q listQuery[E]) Min() Optional[E] {
	atLeastOneFound := false
	var min E
	q.iter(func(_ string, entity E) bool {
		if !atLeastOneFound {
			atLeastOneFound = true
			min = entity
		} else if q.less(entity, min) {
			min = entity
		}

		return true
	})

	if !atLeastOneFound {
		return Optional[E]{}
	}

	return Optional[E]{
		Value: min,
		Valid: true,
	}
}

// Max - get maximum/last entitiy in list. If none, boolean is false.
func (q listQuery[E]) Max() Optional[E] {
	atLeastOneFound := false
	var max E
	q.iter(func(_ string, entity E) bool {
		if !atLeastOneFound {
			atLeastOneFound = true
			max = entity
		} else if q.less(max, entity) {
			max = entity
		}

		return true
	})

	if !atLeastOneFound {
		return Optional[E]{}
	}

	return Optional[E]{
		Value: max,
		Valid: true,
	}
}

// All - get all entities in list in sorted order.
func (q listQuery[E]) All() []E {
	res := make([]E, 0, len(q.data))
	q.iter(func(_ string, entity E) bool {
		res = append(res, entity)
		return true
	})
	sort.Slice(res, func(i, j int) bool {
		return q.less(res[i], res[j])
	})

	return res
}
