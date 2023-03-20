package simpdb

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
)

type StorageConfig[E Entity] interface {
	Build(dir, tableName string) Storage[E]
}

type FuncStorageConfig[E Entity] func(dir, tableName string) Storage[E]

func (f FuncStorageConfig[E]) Build(dir, tableName string) Storage[E] {
	return f(dir, tableName)
}

type Storage[E Entity] interface {
	Filename() string
	Read(io.Reader) (map[string]E, error)
	Write(io.Writer, map[string]E) error
}

func ensureFile(filename string) error {
	dir := filepath.Dir(filename)

	if _, err := os.Stat(dir); err != nil {
		if !os.IsNotExist(err) {
			return fmt.Errorf("check directory %s: %w", dir, err)
		}

		// TODO: mkdirall
		if err := os.Mkdir(dir, 0o755); err != nil {
			return fmt.Errorf("create directory %s: %w", dir, err)
		}
	}

	if _, err := os.Stat(filename); err != nil {
		if !os.IsNotExist(err) {
			return fmt.Errorf("check table file %s: %w", filename, err)
		}

		file, err := os.OpenFile(filename, os.O_CREATE|os.O_RDWR, 0o600)
		if err != nil {
			return fmt.Errorf("create table file %s: %w", filename, err)
		}
		defer file.Close()

		if _, err := file.Write([]byte("{}")); err != nil {
			return fmt.Errorf("initialize table file %s: %w", filename, err)
		}
	}

	return nil
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
