package objectsync

import (
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"testing"
	"time"
)

// Check the interface
var _ Storage = &InMemoryStorage{}

func TestTree(t *testing.T) {

	ctx := context.TODO()
	t.Run("ConflictResolution", func(t *testing.T) {

		status := NewInMemoryStatusStorage()

		itemCount := 3
		expectedStore1Objects := []*GenericObject{}
		expectedStore2Objects := []*GenericObject{}

		// Create first item set
		store1 := NewInMemoryStorage("local")

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

		checkStore(ctx, store1, len(expectedStore1Objects), expectedStore1Objects, t)
		checkStore(ctx, store2, len(expectedStore2Objects), expectedStore2Objects, t)

		// Make sure nothing changes if we change nothing
		err = Sync(ctx, store1, store2, status)
		if err != nil {
			t.Errorf("Error: %v", err)
		}

		checkStore(ctx, store1, len(expectedStore1Objects), expectedStore1Objects, t)
		checkStore(ctx, store2, len(expectedStore2Objects), expectedStore2Objects, t)

		localObject := addedObjects[0]
		localObject.Value = "Consistency is the playground of dull minds."
		localObject.Modified = time.Now().UTC()

		// update store 1
		err = store1.Set(ctx, localObject)
		if err != nil {
			t.Errorf("Error: %v", err)
		}

		remoteObject := &(*localObject)
		remoteObject.Value = "NewValue"
		remoteObject.Modified = time.Now().UTC()

		// update store 2
		err = store2.Set(ctx, remoteObject)
		if err != nil {
			t.Errorf("Error: %v", err)
		}

		// Make sure nothing changes if we change nothing
		err = Sync(ctx, store1, store2, status)
		if err != nil {
			t.Errorf("Error: %v", err)
		}

		// Check that local has the right value
		checkObject1, err := store1.Get(ctx, localObject.ID)
		if err != nil {
			t.Errorf("Error: %v", err)
		}
		if checkObject1.Value != remoteObject.Value {
			t.Fatalf("Unexpected value = %s expected %s", checkObject1.Value, remoteObject.Value)
		}

		// Newer object in local store, should beat remote change...

		newRemoteObject := &GenericObject{
			ID:       localObject.ID,
			Value:    "NewValueAgain",
			Modified: time.Now().UTC(),
		}

		// update store 2
		err = store2.Set(ctx, newRemoteObject)
		if err != nil {
			t.Errorf("Error: %v", err)
		}

		// Update the local store again and sync again.
		newLocalObject := &GenericObject{
			ID:       localObject.ID,
			Value:    "Consistency is the playground of dull minds.",
			Modified: time.Now().UTC(),
		}

		// update store 1
		err = store1.Set(ctx, newLocalObject)
		if err != nil {
			t.Errorf("Error: %v", err)
		}

		// Make sure nothing changes if we change nothing
		err = Sync(ctx, store1, store2, status)
		if err != nil {
			t.Errorf("Error: %v", err)
		}

		// Check that local has the right value
		checkObject1, err = store1.Get(ctx, newLocalObject.ID)
		if err != nil {
			t.Errorf("Error: %v", err)
		}
		if checkObject1.Value != newLocalObject.Value {
			t.Fatalf("Unexpected value = %s expected %s", checkObject1.Value, newLocalObject.Value)
		}

		// Check that remote has the right value
		checkObject2, err := store2.Get(ctx, newLocalObject.ID)
		if err != nil {
			t.Errorf("Error: %v", err)
		}
		if checkObject2.Value != newLocalObject.Value {
			t.Fatalf("Unexpected value = %s expected %s", checkObject2.Value, newLocalObject.Value)
		}

	})
	t.Run("SimpleUpdate", func(t *testing.T) {

		status := NewInMemoryStatusStorage()

		itemCount := 3
		expectedStore1Objects := []*GenericObject{}
		expectedStore2Objects := []*GenericObject{}

		// Create first item set
		store1 := NewInMemoryStorage("local")

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

		checkStore(ctx, store1, len(expectedStore1Objects), expectedStore1Objects, t)
		checkStore(ctx, store2, len(expectedStore2Objects), expectedStore2Objects, t)

		// Make sure nothing changes if we change nothing
		err = Sync(ctx, store1, store2, status)
		if err != nil {
			t.Errorf("Error: %v", err)
		}

		checkStore(ctx, store1, len(expectedStore1Objects), expectedStore1Objects, t)
		checkStore(ctx, store2, len(expectedStore2Objects), expectedStore2Objects, t)

		firstObject := addedObjects[0]
		firstObject.Value = "Consistency is the playground of dull minds."
		firstObject.Modified = time.Now().UTC()

		// update store 1
		err = store1.Set(ctx, firstObject)
		if err != nil {
			t.Errorf("Error: %v", err)
		}

		err = Sync(ctx, store1, store2, status)
		if err != nil {
			t.Errorf("Error: %v", err)
		}

		checkStore(ctx, store1, len(expectedStore1Objects), expectedStore1Objects, t)
		checkStore(ctx, store2, len(expectedStore2Objects), expectedStore2Objects, t)

		addedObjectsStore2, err := addObjectsToStore(ctx, store2, 2)
		if err != nil {
			t.Errorf("Error: %v", err)
		}

		expectedStore1Objects = append(expectedStore1Objects, addedObjectsStore2...)
		expectedStore2Objects = append(expectedStore2Objects, addedObjectsStore2...)

		err = Sync(ctx, store1, store2, status)
		if err != nil {
			t.Errorf("Error: %v", err)
		}

		checkStore(ctx, store1, len(expectedStore1Objects), expectedStore1Objects, t)
		checkStore(ctx, store2, len(expectedStore2Objects), expectedStore2Objects, t)

		remoteChange := addedObjectsStore2[0]
		remoteChange.Value = "What one programmer can do in one month, two programmers can do in two months."
		remoteChange.Modified = time.Now().UTC()

		// update store 1
		err = store2.Set(ctx, remoteChange)
		if err != nil {
			t.Errorf("Error: %v", err)
		}

		err = Sync(ctx, store1, store2, status)
		if err != nil {
			t.Errorf("Error: %v", err)
		}

		checkStore(ctx, store1, len(expectedStore1Objects), expectedStore1Objects, t)
		checkStore(ctx, store2, len(expectedStore2Objects), expectedStore2Objects, t)

	})
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

		fmt.Println("Run sync...")

		// sync items
		store2 := NewInMemoryStorage("remote")
		err = Sync(ctx, store1, store2, status)
		if err != nil {
			t.Errorf("Error: %v", err)
		}

		checkStore(ctx, store1, itemCount, expectedStore1Objects, t)
		checkStore(ctx, store2, itemCount, expectedStore2Objects, t)

		increment := 3
		addedObjectsStore1, err := addObjectsToStore(ctx, store1, increment)
		if err != nil {
			t.Errorf("Error: %v", err)
		}
		expectedStore1Objects = append(expectedStore1Objects, addedObjectsStore1...)
		expectedStore2Objects = append(expectedStore2Objects, addedObjectsStore1...)

		checkStore(ctx, store1, len(expectedStore1Objects), expectedStore1Objects, t)

		// Remove some items
		decrement := 2
		for i := 0; i < decrement; i++ {
			store2.Delete(ctx, addedObjects[i].ID)
			expectedStore1Objects = remove(expectedStore1Objects, addedObjects[i].ID)
			expectedStore2Objects = remove(expectedStore2Objects, addedObjects[i].ID)
		}

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
	allFromStore, err := store.GetAll(ctx)
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
					if !bytes.Equal(expectedObject.Hash, foundObject.Hash) {
						t.Fatalf("Found id = %s in %s, but hash does not match: %s : %s", expectedObject.ID, store.GetName(), base64.StdEncoding.EncodeToString(expectedObject.Hash), base64.StdEncoding.EncodeToString(foundObject.Hash))
					}
					continue
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
