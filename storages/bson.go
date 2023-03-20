package storages

import (
	"fmt"
	"io"
	"path/filepath"

	"github.com/rprtr258/simpdb"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/bsonrw"
)

type bsonStorage[E Entity] struct {
	tableFilename
}

func (s *bsonStorage[E]) Read(r io.Reader) (map[string]E, error) {
	bytes, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("bson read all: %w", err)
	}

	decoder, err := bson.NewDecoder(bsonrw.NewBSONDocumentReader(bytes))
	if err != nil {
		return nil, fmt.Errorf("bson decoder: %w", err)
	}

	var res map[string]E
	if err := decoder.Decode(&res); err != nil {
		return nil, fmt.Errorf("bson storage decode: %w", err)
	}

	return res, nil
}

func (s *bsonStorage[E]) Write(w io.Writer, entities map[string]E) error {
	valueWriter, err := bsonrw.NewBSONValueWriter(w)
	if err != nil {
		return fmt.Errorf("bson value writer: %w", err)
	}

	encoder, err := bson.NewEncoder(valueWriter)
	if err != nil {
		return fmt.Errorf("bson encoder: %w", err)
	}

	if err := encoder.Encode(entities); err != nil {
		return fmt.Errorf("bson encode: %w", err)
	}

	return nil
}

func NewBSONStorage[E Entity]() simpdb.StorageConfig[E] {
	return simpdb.FuncStorageConfig[E](
		func(dir, tableName string) simpdb.Storage[E] {
			filename := filepath.Join(dir, tableName+".bson")
			return &bsonStorage[E]{tableFilename(filename)}
		},
	)
}
