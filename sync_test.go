package objectsync

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// Check the interface
var _ Storage = &InMemoryStorage{}

func TestTree(t *testing.T) {

	ctx := context.TODO()

	t.Run("NoChange", func(t *testing.T) {
		t.SkipNow()

		store := NewInMemoryStorage()

		err := AddObjectsToStore(ctx, store, 4)
		if err != nil {
			t.Errorf("Error: %v", err)
		}

		store2, err := Copy(store)
		if err != nil {
			t.Errorf("Error: %v", err)
		}

		err = Sync(ctx, store, store2)
		if err != nil {
			t.Errorf("Error: %v", err)
		}

	})
	t.Run("SimpleAppend", func(t *testing.T) {

		store := NewInMemoryStorage()

		err := AddObjectsToStore(ctx, store, 10)
		if err != nil {
			t.Errorf("Error: %v", err)
		}

		store2, err := Copy(store)
		if err != nil {
			t.Errorf("Error: %v", err)
		}

		err = AddObjectsToStore(ctx, store2, 1)
		if err != nil {
			t.Errorf("Error: %v", err)
		}

		err = Sync(ctx, store, store2)
		if err != nil {
			t.Errorf("Error: %v", err)
		}

		allFromStore, err := store.GetAll(ctx)
		if err != nil {
			t.Errorf("Error: %v", err)
		}
		if len(allFromStore) != 11 {
			t.Errorf("Incorrect len = %v, want %v", len(allFromStore), 11)
		}

	})
}

func AddObjectsToStore(ctx context.Context, store Storage, len int) error {
	for i := 0; i < len; i++ {
		now := time.Now().UTC()
		err := store.Set(ctx, &GenericObject{
			ID:       fmt.Sprintf("%v", now.UnixNano()),
			Modified: now,
			Value:    fmt.Sprintf("Object%v", i),
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func Copy(s *InMemoryStorage) (*InMemoryStorage, error) {
	store := NewInMemoryStorage()
	objects, err := s.GetAll(nil)
	if err != nil {
		return nil, err
	}
	for _, object := range objects {
		err := store.Set(nil, &(*object))
		if err != nil {
			return nil, err
		}
	}
	return store, nil
}
