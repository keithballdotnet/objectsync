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

	t.Run("SimpleAppend", func(t *testing.T) {

		// Wipe status
		status := NewInMemoryStatusStorage()

		itemCount := 3

		// Create first item set
		store := NewInMemoryStorage("local")

		_, err := AddObjectsToStore(ctx, store, itemCount)
		if err != nil {
			t.Errorf("Error: %v", err)
		}

		fmt.Println("Run sync...")

		// sync items
		store2 := NewInMemoryStorage("remote")
		err = Sync(ctx, store, store2, status)
		if err != nil {
			t.Errorf("Error: %v", err)
		}

		checkStore(ctx, store, itemCount, t)
		checkStore(ctx, store2, itemCount, t)

		increment := 2

		_, err = AddObjectsToStore(ctx, store2, increment)
		if err != nil {
			t.Errorf("Error: %v", err)
		}
		checkStore(ctx, store2, itemCount+increment, t)

		fmt.Println("Run sync...")

		err = Sync(ctx, store, store2, status)
		if err != nil {
			t.Errorf("Error: %v", err)
		}

		checkStore(ctx, store, itemCount+increment, t)
		checkStore(ctx, store2, itemCount+increment, t)

	})
	t.Run("SimpleRemove", func(t *testing.T) {

		// Status
		status := NewInMemoryStatusStorage()

		itemCount := 3

		// Create first item set
		store := NewInMemoryStorage("local")

		addedObjects, err := AddObjectsToStore(ctx, store, itemCount)
		if err != nil {
			t.Errorf("Error: %v", err)
		}

		fmt.Println("Run sync...")

		// sync items
		store2 := NewInMemoryStorage("remote")
		err = Sync(ctx, store, store2, status)
		if err != nil {
			t.Errorf("Error: %v", err)
		}

		checkStore(ctx, store, itemCount, t)
		checkStore(ctx, store2, itemCount, t)

		// Remove some items
		decrement := 2
		for i := 0; i < decrement; i++ {
			store2.Delete(ctx, addedObjects[i].ID)
		}
		checkStore(ctx, store2, itemCount-decrement, t)

		fmt.Println("Run sync...")

		err = Sync(ctx, store, store2, status)
		if err != nil {
			t.Errorf("Error: %v", err)
		}

		checkStore(ctx, store, itemCount-decrement, t)
		checkStore(ctx, store2, itemCount-decrement, t)

	})
}

func checkStore(ctx context.Context, store Storage, expectedLen int, t *testing.T) {
	allFromStore, _, err := store.GetAll(ctx)
	if err != nil {
		t.Errorf("Error: %v", err)
	}
	if len(allFromStore) != expectedLen {
		t.Errorf("Incorrect len = %v, want %v", len(allFromStore), expectedLen)
	}
}

func AddObjectsToStore(ctx context.Context, store Storage, len int) ([]*GenericObject, error) {
	addedObects := make([]*GenericObject, len)
	for i := 0; i < len; i++ {
		now := time.Now().UTC()
		o := &GenericObject{
			ID:       fmt.Sprintf("%v", now.UnixNano()),
			Modified: now,
			Value:    fmt.Sprintf("Object%v", i),
		}
		addedObects[i] = o
		err := store.Set(ctx, o)
		if err != nil {
			return nil, err
		}
	}
	return addedObects, nil
}
