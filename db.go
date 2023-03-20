package simpdb

import (
	"io"
)

// DB handler for database directory.
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

type TableConfig struct {
	// Indent indicates whether to do json indenting when writing file
	Indent bool
}

type Storage[E Entity] interface {
	Filename() string
	Read(io.Reader) (map[string]E, error)
	Write(io.Writer, map[string]E) error
}

// GetTable for the entity E.
func GetTable[E Entity](
	db *DB,
	tableName string,
	storage Storage[E],
) (*Table[E], error) {
	// storage, err := newJSONStorage[E](db.dir, tableName, config.Indent)
	// if err != nil {
	// 	return nil, fmt.Errorf("get table: %w", err)
	// }

	return newTable(storage)
}
