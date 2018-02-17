package objectsync

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func TestTree(t *testing.T) {

	ctx := context.TODO()

	coll := GetTestObjectCollection(10)

	objects, err := Serialize(ctx, coll)
	if err != nil {
		t.Errorf("Error: %v", err)
	}

	tree, err := CreateMerkleTree(ctx, objects)
	if err != nil {
		t.Errorf("Error: %v", err)
	}

	fmt.Printf("Tree: %s", tree.ToString(ctx))
}

func GetTestObjectCollection(len int) GenericObjectCollection {
	testObjects := make([]*GenericObject, len)
	for i := 0; i < len; i++ {
		now := time.Now().UTC()
		testObjects[i] = &GenericObject{
			ID:       fmt.Sprintf("%v", now.UnixNano()),
			Modified: now,
			Value:    fmt.Sprintf("Object%v", i),
		}
	}
	return GenericObjectCollection(testObjects)

}
