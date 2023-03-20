package simpdb

import (
	"fmt"
	"io"
	"os"
)

type StorageConfig[E Entity] interface {
	Build(dir, tableName string) Storage[E]
}

type Storage[E Entity] interface {
	Filename() string
	Read(io.Reader) (map[string]E, error)
	Write(io.Writer, map[string]E) error
}

// read all records from table file.
func read[E Entity](storage Storage[E]) (map[string]E, error) {
	if err := ensureFile(storage.Filename()); err != nil {
		var e Entity
		return nil, fmt.Errorf(
			"read, check %T table file %q: %w",
			e, storage.Filename(),
			err,
		)
	}

	f, err := os.Open(storage.Filename())
	if err != nil {
		return nil, fmt.Errorf("read, open file %q: %w", storage.Filename(), err)
	}

	res, err := storage.Read(f)
	if err != nil {
		return nil, fmt.Errorf("read entities: %w", err)
	}

	return res, nil
}

// write all entities to table file.
func write[E Entity](storage Storage[E], entities map[string]E) error {
	if err := ensureFile(storage.Filename()); err != nil {
		var e Entity
		return fmt.Errorf(
			"write, check %T table file %q: %w",
			e, storage.Filename(),
			err,
		)
	}

	file, err := os.Create(storage.Filename())
	if err != nil {
		return fmt.Errorf("write, recreate file: %w", err)
	}
	defer file.Close()

	if err := storage.Write(file, entities); err != nil {
		return fmt.Errorf("write entities: %w", err)
	}

	return nil
}
