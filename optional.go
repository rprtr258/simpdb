package simpdb

// Optional value. It is safe to get Value only if it is Valid.
type Optional[T any] struct {
	Value T
	Valid bool
}
