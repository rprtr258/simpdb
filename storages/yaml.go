package storages

// import (
// 	"encoding/json"
// 	"fmt"
// 	"io"
// 	"path/filepath"

// 	"github.com/rprtr258/simpdb"
// )

// type jsonStorage[E simpdb.Entity] struct {
// 	// filename of table file
// 	filename string
// }

// func (s *jsonStorage[E]) Filename() string {
// 	return s.filename
// }

// func (s *jsonStorage[E]) Read(r io.Reader) (map[string]E, error) {
// 	var res map[string]E
// 	if err := json.NewDecoder(r).Decode(&res); err != nil {
// 		return nil, fmt.Errorf("json storage decode: %w", err)
// 	}

// 	return res, nil
// }

// func (s *jsonStorage[E]) Write(w io.Writer, entities map[string]E) error {
// 	if err := json.NewEncoder(w).Encode(entities); err != nil {
// 		return fmt.Errorf("json storage encode: %w", err)
// 	}

// 	return nil
// }

// type jsonStorageConfig[E simpdb.Entity] struct{}

// func NewJSONStorage[E simpdb.Entity]() simpdb.StorageConfig[E] {
// 	return jsonStorageConfig[E]{}
// }

// func (c jsonStorageConfig[E]) Build(dir, tableName string) simpdb.Storage[E] {
// 	return &jsonStorage[E]{
// 		filename: filepath.Join(dir, tableName+".json"),
// 	}
// }
