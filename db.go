package simpdb

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

// GetTable for the entity E.
func GetTable[E Entity](
	db *DB,
	tableName string,
	storage StorageConfig[E],
) (*Table[E], error) {
	return newTable(storage.Build(db.dir, tableName))
}
