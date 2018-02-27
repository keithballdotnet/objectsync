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

		status := NewInMemoryStatusStorage()

		itemCount := 3

		// Create first item set
		store := NewInMemoryStorage("local")

		_, err := addObjectsToStore(ctx, store, itemCount)
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

		checkStore(ctx, store, itemCount, nil, t)
		checkStore(ctx, store2, itemCount, nil, t)

		increment := 2

		_, err = addObjectsToStore(ctx, store2, increment)
		if err != nil {
			t.Errorf("Error: %v", err)
		}
		checkStore(ctx, store2, itemCount+increment, nil, t)

		fmt.Println("Run sync...")

		err = Sync(ctx, store, store2, status)
		if err != nil {
			t.Errorf("Error: %v", err)
		}

		checkStore(ctx, store, itemCount+increment, nil, t)
		checkStore(ctx, store2, itemCount+increment, nil, t)

	})
	t.Run("SimpleRemove", func(t *testing.T) {

		status := NewInMemoryStatusStorage()

		itemCount := 3

		// Create first item set
		store := NewInMemoryStorage("local")

		addedObjects, err := addObjectsToStore(ctx, store, itemCount)
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

		checkStore(ctx, store, itemCount, nil, t)
		checkStore(ctx, store2, itemCount, nil, t)

		// Remove some items
		decrement := 2
		for i := 0; i < decrement; i++ {
			store2.Delete(ctx, addedObjects[i].ID)
		}
		checkStore(ctx, store2, itemCount-decrement, nil, t)

		fmt.Println("Run sync...")

		err = Sync(ctx, store, store2, status)
		if err != nil {
			t.Errorf("Error: %v", err)
		}

		checkStore(ctx, store, itemCount-decrement, nil, t)
		checkStore(ctx, store2, itemCount-decrement, nil, t)

	})
	t.Run("DualAppend", func(t *testing.T) {

		status := NewInMemoryStatusStorage()

		itemCount := 5

		// Create first item set
		store1 := NewInMemoryStorage("local")

		expectedStore1Objects := []*GenericObject{}
		expectedStore2Objects := []*GenericObject{}

		addedObjects, err := addObjectsToStore(ctx, store1, itemCount)
		if err != nil {
			t.Errorf("Error: %v", err)
		}
		expectedStore1Objects = append(expectedStore1Objects, addedObjects...)
		expectedStore2Objects = append(expectedStore2Objects, addedObjects...)

		// sync items
		store2 := NewInMemoryStorage("remote")

		fmt.Println("Run sync...")
		err = Sync(ctx, store1, store2, status)
		if err != nil {
			t.Errorf("Error: %v", err)
		}

		checkStore(ctx, store1, itemCount, expectedStore1Objects, t)
		checkStore(ctx, store2, itemCount, expectedStore2Objects, t)

		increment := 4
		addedObjectsStore1, err := addObjectsToStore(ctx, store1, increment)
		if err != nil {
			t.Errorf("Error: %v", err)
		}
		addedObjectsStore2, err := addObjectsToStore(ctx, store2, increment)
		if err != nil {
			t.Errorf("Error: %v", err)
		}
		expectedStore2Objects = append(expectedStore2Objects, addedObjectsStore1...)
		expectedStore2Objects = append(expectedStore2Objects, addedObjectsStore2...)
		expectedStore1Objects = append(expectedStore1Objects, addedObjectsStore1...)
		expectedStore1Objects = append(expectedStore1Objects, addedObjectsStore2...)

		fmt.Println("Run sync...")

		err = Sync(ctx, store1, store2, status)
		if err != nil {
			t.Errorf("Error: %v", err)
		}

		checkStore(ctx, store1, itemCount+increment+increment, expectedStore1Objects, t)
		checkStore(ctx, store2, itemCount+increment+increment, expectedStore2Objects, t)
	})
	t.Run("ComplexSync", func(t *testing.T) {

		status := NewInMemoryStatusStorage()

		itemCount := 2

		// Create first item set
		store1 := NewInMemoryStorage("local")

		expectedStore1Objects := []*GenericObject{}
		expectedStore2Objects := []*GenericObject{}

		addedObjects, err := addObjectsToStore(ctx, store1, itemCount)
		if err != nil {
			t.Errorf("Error: %v", err)
		}
		expectedStore1Objects = append(expectedStore1Objects, addedObjects...)
		expectedStore2Objects = append(expectedStore2Objects, addedObjects...)

		fmt.Println("Run sync...")

		// sync items
		store2 := NewInMemoryStorage("remote")
		err = Sync(ctx, store1, store2, status)
		if err != nil {
			t.Errorf("Error: %v", err)
		}

		checkStore(ctx, store1, itemCount, expectedStore1Objects, t)
		checkStore(ctx, store2, itemCount, expectedStore2Objects, t)

		increment := 1
		addedObjectsStore1, err := addObjectsToStore(ctx, store1, increment)
		if err != nil {
			t.Errorf("Error: %v", err)
		}
		expectedStore1Objects = append(expectedStore1Objects, addedObjectsStore1...)
		expectedStore2Objects = append(expectedStore2Objects, addedObjectsStore1...)

		checkStore(ctx, store1, len(expectedStore1Objects), expectedStore1Objects, t)

		// // Remove some items
		// decrement := 1
		// for i := 0; i < decrement; i++ {
		// 	store2.Delete(ctx, addedObjects[i].ID)
		// 	//expectedStore1Objects = remove(expectedStore1Objects, addedObjects[i].ID)
		// 	expectedStore2Objects = remove(expectedStore2Objects, addedObjects[i].ID)
		// }
		// checkStore(ctx, store2, len(expectedStore2Objects), expectedStore2Objects, t)

		fmt.Println("Run sync...")

		err = Sync(ctx, store1, store2, status)
		if err != nil {
			t.Errorf("Error: %v", err)
		}

		checkStore(ctx, store1, len(expectedStore1Objects), expectedStore1Objects, t)
		checkStore(ctx, store2, len(expectedStore2Objects), expectedStore2Objects, t)

	})
}

func checkStore(ctx context.Context, store Storage, expectedLen int, expectedObjects []*GenericObject, t *testing.T) {
	allFromStore, _, err := store.GetAll(ctx)
	if err != nil {
		t.Errorf("Error: %v", err)
	}
	if len(allFromStore) != expectedLen {
		t.Errorf("Incorrect len = %v, want %v", len(allFromStore), expectedLen)
	}
	if expectedObjects != nil {
		// These objects need to be present
		for _, expectedObject := range expectedObjects {
			found := false
			for _, foundObject := range allFromStore {
				if expectedObject.ID == foundObject.ID {
					found = true
				}
			}
			if !found {
				t.Fatalf("Unexpected not found id = %s in %s", expectedObject.ID, store.GetName())
			}
		}

		// Check for objects that should NOT be there
		for _, storeObject := range allFromStore {
			found := false
			for _, expectedObject := range expectedObjects {
				if storeObject.ID == expectedObject.ID {
					found = true
				}
			}
			if !found {
				t.Fatalf("Unexpected found id = %s in %s", storeObject.ID, store.GetName())
			}
		}
	}
}

func addObjectsToStore(ctx context.Context, store Storage, len int) ([]*GenericObject, error) {
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

func remove(s []*GenericObject, id string) []*GenericObject {
	var r []*GenericObject
	for _, o := range s {
		if o.ID != id {
			r = append(r, o)
		}
	}
	return r
}
