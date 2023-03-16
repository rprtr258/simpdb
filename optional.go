package simpdb

type Optional[T any] struct {
	Value T
	Valid bool
}
