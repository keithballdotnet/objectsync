package objectsync

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"sort"

	"github.com/keithballdotnet/merkle"
)

// Serialize will return a sorted serialized object collection
func Serialize(ctx context.Context, goc GenericObjectCollection) (SerializedObjectCollection, error) {

	objects := make([]*SerializedObject, len(goc))
	for i := 0; i < len(goc); i++ {
		jsonData, err := json.Marshal(goc[i])
		if err != nil {
			return nil, err
		}
		// Generate hash of data
		hash := sha256.Sum256(jsonData)
		// Create serialized object
		objects[i] = &SerializedObject{Hash: hash[:], Data: jsonData}
	}

	// Sort collection
	sort.Sort(serializedObjectSorter(objects))

	return SerializedObjectCollection(objects), nil
}

// CreateMerkleTree will return a merkle tree for a collection of serialized objects
func CreateMerkleTree(ctx context.Context, objects SerializedObjectCollection) (*merkle.Tree, error) {
	// create a new Sha256 powered MerkleTree
	tree := merkle.NewTree(&merkle.Sha256Hasher{})
	// We will add the hashes to the tree, not the actual data.
	tree.AddContent(ctx, objects.GetHashes())
	// Build the tree
	tree.Build(ctx)

	return tree, nil
}
