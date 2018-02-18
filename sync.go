package objectsync

import (
	"context"
	"sort"

	"github.com/keithballdotnet/merkle"
)

// GetTreeObjects will return a sorted serialized object collection
func GetTreeObjects(ctx context.Context, hashes map[string]Hash) (TreeObjectCollection, error) {

	objects := make([]*TreeObject, len(hashes))
	i := 0
	for id, hash := range hashes {
		// Create tree object
		objects[i] = &TreeObject{ID: id, Hash: hash}
		i++
	}

	// Sort collection
	sort.Sort(treeObjectSorter(objects))

	return TreeObjectCollection(objects), nil
}

// CreateMerkleTree will return a merkle tree for a collection of serialized objects
func CreateMerkleTree(ctx context.Context, objects TreeObjectCollection) (*merkle.Tree, error) {
	// create a new Sha256 powered MerkleTree
	tree := merkle.NewTree(&merkle.Sha256Hasher{})
	// We will add the hashes to the tree, not the actual data.
	// Add the IDs as extra data so we have an ID in the tree
	// we can use to identify the leaves from the tree
	tree.AddContent(ctx, objects.GetHashes(), objects.GetIDs())
	// Build the tree
	tree.Build(ctx)

	return tree, nil
}
