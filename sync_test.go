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
		var store Storage
		store = NewInMemoryStorage()

		err := AddObjectsToStore(ctx, store, 4)
		if err != nil {
			t.Errorf("Error: %v", err)
		}

		hashes, err := store.GetHashes(ctx)
		if err != nil {
			t.Errorf("Error: %v", err)
		}

		objects, err := GetTreeObjects(ctx, hashes)
		if err != nil {
			t.Errorf("Error: %v", err)
		}

		tree, err := CreateMerkleTree(ctx, objects)
		if err != nil {
			t.Errorf("Error: %v", err)
		}

		fmt.Printf("Tree: %s", tree.ToString(ctx))

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
