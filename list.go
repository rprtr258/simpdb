package simpdb

import "sort"

type listQuery[E Entity] struct {
	selectQuery[E]
	less func(E, E) bool
}

// Iter over list of entities in sorted order. fn accepts ID and entity.
func (q listQuery[E]) Iter(fn func(string, E)) {
	for id, entity := range q.data {
		if q.filter(id, entity) {
			fn(id, entity)
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
	q.Iter(func(_ string, entity E) {
		if !atLeastOneFound {
			atLeastOneFound = true
			min = entity
		} else if q.less(entity, min) {
			min = entity
		}
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
	q.Iter(func(_ string, entity E) {
		if !atLeastOneFound {
			atLeastOneFound = true
			max = entity
		} else if q.less(max, entity) {
			max = entity
		}
	})

	if !atLeastOneFound {
		return Optional[E]{}
	}

	return Optional[E]{
		Value: max,
		Valid: true,
	}
}

// All - get all entities in list.
func (q listQuery[E]) All() []E {
	res := make([]E, 0, len(q.data))
	q.Iter(func(_ string, entity E) {
		res = append(res, entity)
	})
	sort.Slice(res, func(i, j int) bool {
		return q.less(res[i], res[j])
	})
	return res
}
