package objectsync

import (
	"context"
	"crypto/sha256"
)

// Storage is a storage interface
type Storage interface {
	GetName() string
	Set(ctx context.Context, object *GenericObject) error
	Get(ctx context.Context, id string) (*GenericObject, error)
	GetAll(ctx context.Context) (GenericObjectCollection, error)
	Delete(ctx context.Context, id string) error
}

// InMemoryStorage ...
type InMemoryStorage struct {
	name    string
	idIndex map[string]*GenericObject
}

// NewInMemoryStorage ...
func NewInMemoryStorage(name string) *InMemoryStorage {
	idIndex := make(map[string]*GenericObject)
	return &InMemoryStorage{name: name, idIndex: idIndex}
}

// GetName ...
func (s *InMemoryStorage) GetName() string {
	return s.name
}

// Set ...
func (s *InMemoryStorage) Set(ctx context.Context, object *GenericObject) error {
	hash := sha256.Sum256([]byte(object.Value))
	object.Hash = Hash(hash[:])
	s.idIndex[object.ID] = object
	return nil
}

// Get ...
func (s *InMemoryStorage) Get(ctx context.Context, id string) (*GenericObject, error) {
	object, ok := s.idIndex[id]
	if !ok {
		return nil, ErrorNotFound
	}

	return object, nil
}

// GetAll will return all objects
func (s *InMemoryStorage) GetAll(ctx context.Context) (GenericObjectCollection, error) {
	objects := make([]*GenericObject, len(s.idIndex))
	i := 0
	for _, object := range s.idIndex {
		objects[i] = object
		i++
	}

	return GenericObjectCollection(objects), nil
}

// Delete will remove a entry from the storage
func (s *InMemoryStorage) Delete(ctx context.Context, id string) error {
	delete(s.idIndex, id)
	return nil
}
