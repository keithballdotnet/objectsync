package objectsync

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
)

// Storage is a storage interface
type Storage interface {
	GetName() string
	Set(ctx context.Context, object *GenericObject) error
	Get(ctx context.Context, id string) (*GenericObject, Hash, error)
	GetAll(ctx context.Context) (GenericObjectCollection, map[string]Hash, error)
	Delete(ctx context.Context, id string) error
	GetHashes(ctx context.Context) (map[string]Hash, error)
}

// InMemoryStorage ...
type InMemoryStorage struct {
	name      string
	idIndex   map[string]*GenericObject
	hashIndex map[string]Hash
}

// NewInMemoryStorage ...
func NewInMemoryStorage(name string) *InMemoryStorage {
	idIndex := make(map[string]*GenericObject)
	hashIndex := make(map[string]Hash)
	return &InMemoryStorage{name: name, idIndex: idIndex, hashIndex: hashIndex}
}

// GetName ...
func (s *InMemoryStorage) GetName() string {
	return s.name
}

// Set ...
func (s *InMemoryStorage) Set(ctx context.Context, object *GenericObject) error {
	s.idIndex[object.ID] = object

	// TODO: We should find a better serialization tool
	jsonData, err := json.Marshal(object)
	if err != nil {
		return err
	}

	hash := sha256.Sum256(jsonData)
	s.hashIndex[object.ID] = Hash(hash[:])
	return nil
}

// Get ...
func (s *InMemoryStorage) Get(ctx context.Context, id string) (*GenericObject, Hash, error) {
	object, ok := s.idIndex[id]
	if !ok {
		return nil, nil, errors.New("not found")
	}

	return object, s.hashIndex[id], nil
}

// GetAll will return all objects
func (s *InMemoryStorage) GetAll(ctx context.Context) (GenericObjectCollection, map[string]Hash, error) {
	objects := make([]*GenericObject, len(s.idIndex))
	i := 0
	for _, object := range s.idIndex {
		objects[i] = object
		i++
	}

	return GenericObjectCollection(objects), s.hashIndex, nil
}

// Delete will remove a entry from the storage
func (s *InMemoryStorage) Delete(ctx context.Context, id string) error {
	delete(s.hashIndex, id)
	delete(s.idIndex, id)
	return nil
}

// GetHashes will return all the hashes of items
func (s *InMemoryStorage) GetHashes(ctx context.Context) (map[string]Hash, error) {
	return s.hashIndex, nil
}
