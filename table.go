package simpdb

import (
	"encoding/json"
	"os"

	"github.com/rprtr258/xerr"
)

// Entity is interface for all table entities. Structs must implement it for DB
// to be able to store them. Only entities with different IDs will be stored.
type Entity interface {
	ID() string
}

// Table is access point for storage of one entity type.
type Table[E Entity] struct {
	// filename with JSON map for the table
	filename string
	// name of entity in the table
	name string
}

func (t *Table[E]) ensureFileExists() error {
	if _, err := os.Stat(t.filename); err != nil {
		if !os.IsNotExist(err) {
			return xerr.NewW(err)
		}

		file, err := os.Create(t.filename)
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

func (t *Table[E]) Read(r DB, filter func(E) bool) (map[string]E, error) {
	if err := t.ensureFileExists(); err != nil {
		return nil, xerr.NewW(err)
	}

	bytes, err := os.ReadFile(t.filename)
	if err != nil {
		return nil, xerr.NewWM(err, "can't open table file",
			xerr.Field("entity", t.name))
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

func (t *Table[E]) Write(r DB, entities []E) error {
	if err := t.ensureFileExists(); err != nil {
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

	if err := os.WriteFile(t.filename, bytes, 0644); err != nil {
		return xerr.NewW(err)
	}

	return nil
}
