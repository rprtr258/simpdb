package simpdb

import (
	"path/filepath"
	"reflect"
)

// DB handler for database directory
type DB struct {
	dir string
}

// New creates database handler. dir must be name of existing directory. Table
// files will be put inside this directory.
func New(dir string) *DB {
	return &DB{
		dir: dir,
	}
}

// GetTable for the entity E.
func GetTable[E Entity](db *DB) *Table[E] {
	var e E
	entityName := reflect.TypeOf(e).Name()
	return &Table[E]{
		filename: filepath.Join(db.dir, entityName),
		name:     entityName,
	}
}
