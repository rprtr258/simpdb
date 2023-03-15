package simpdb

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

type TableConfig struct {
	// Indent indicates whether to do json indenting when writing file
	Indent bool
}

// GetTable for the entity E.
func GetTable[E Entity](db *DB, config TableConfig) *Table[E] {
	var e E
	entityName := e.TableName()
	return &Table[E]{
		jsonStorage: jsonStorage[E]{
			dir:    db.dir,
			name:   entityName,
			intend: config.Indent,
		},
	}
}
