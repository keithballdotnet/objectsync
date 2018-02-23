package objectsync

import (
	"context"
	"errors"
)

// StatusStorage is a storage interface
type StatusStorage interface {
	Set(ctx context.Context, object *SyncStatus) error
	Get(ctx context.Context, id string) (*SyncStatus, error)
	GetAll(ctx context.Context) ([]*SyncStatus, error)
	Delete(ctx context.Context, id string) error
}

// InMemoryStatusStorage ...
type InMemoryStatusStorage struct {
	db map[string]*SyncStatus
}

// NewInMemoryStatusStorage ...
func NewInMemoryStatusStorage() *InMemoryStatusStorage {
	db := make(map[string]*SyncStatus)
	return &InMemoryStatusStorage{db: db}
}

// Set ...
func (s *InMemoryStatusStorage) Set(ctx context.Context, object *SyncStatus) error {
	s.db[object.ID] = object
	return nil
}

// Get ...
func (s *InMemoryStatusStorage) Get(ctx context.Context, id string) (*SyncStatus, error) {
	object, ok := s.db[id]
	if !ok {
		return nil, errors.New("not found")
	}

	return object, nil
}

// GetAll ...
func (s *InMemoryStatusStorage) GetAll(ctx context.Context) ([]*SyncStatus, error) {
	objects := make([]*SyncStatus, len(s.db))
	i := 0
	for _, object := range s.db {
		objects[i] = object
		i++
	}

	return objects, nil
}

// Delete ...
func (s *InMemoryStatusStorage) Delete(ctx context.Context, id string) error {
	delete(s.db, id)
	return nil
}
