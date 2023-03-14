package simpdb

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"

	"github.com/rprtr258/xerr"
)

type DB struct {
	dir string
}

func New(dir string) *DB {
	return &DB{
		dir: dir,
	}
}

type entity interface {
	ID() string
}

func entityName[E entity]() string {
	var e E
	return reflect.TypeOf(e).Name()
}

func ensureTableFile(name string) error {
	if _, err := os.Stat(name); err != nil {
		if !os.IsNotExist(err) {
			return xerr.NewW(err)
		}

		file, err := os.Create(name)
		if err != nil {
			return xerr.NewW(err)
		}

		if _, err := file.Write([]byte("{}")); err != nil {
			return xerr.NewW(err)
		}

		return file.Close()
	}

	return nil
}

func Read[E entity](r DB, filter func(E) bool) (map[string]E, error) {
	tableFilename := filepath.Join(r.dir, entityName[E]())

	if err := ensureTableFile(tableFilename); err != nil {
		return nil, xerr.NewW(err)
	}

	bytes, err := os.ReadFile(tableFilename)
	if err != nil {
		return nil, xerr.NewWM(err, "can't open table file",
			xerr.Field("entity", entityName[E]()))
	}

	var all map[string]E
	if err := json.Unmarshal(bytes, &all); err != nil {
		return nil, xerr.NewW(err)
	}

	res := make(map[string]E, len(all))
	for id, entity := range all {
		if filter(entity) {
			res[id] = entity
		}
	}

	return res, nil
}

func Write[E entity](r DB, entities []E) error {
	tableFilename := filepath.Join(r.dir, entityName[E]())

	if err := ensureTableFile(tableFilename); err != nil {
		return xerr.NewW(err)
	}

	all := make(map[string]E, len(entities))
	for _, entity := range entities {
		all[entity.ID()] = entity
	}

	bytes, err := json.Marshal(all)
	if err != nil {
		return xerr.NewW(err)
	}

	if err := os.WriteFile(tableFilename, bytes, 0644); err != nil {
		return xerr.NewW(err)
	}

	return nil
}
