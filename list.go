package simpdb

import "sort"

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
