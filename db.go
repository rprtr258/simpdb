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

func read[E entity](r DB, filter func(E) bool) ([]E, error) {
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

	res := make([]E, 0, len(all))
	for _, entity := range all {
		if filter(entity) {
			res = append(res, entity)
		}
	}

	return res, nil
}

func write[E entity](r DB, entities []E) error {
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
