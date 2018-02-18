package objectsync

import (
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"sort"

	"github.com/keithballdotnet/merkle"
)

// Sync will sync together two Storages
// Last Write Wins (LWW) conflict resolution
func Sync(ctx context.Context, local, remote Storage) error {

	localTree, err := getTree(ctx, local)
	if err != nil {
		return err
	}

	remoteTree, err := getTree(ctx, remote)
	if err != nil {
		return err
	}

	// Nothing to sync
	if bytes.Equal(localTree.GetRootHash(), remoteTree.GetRootHash()) {
		fmt.Printf("Tree Root matches: %v", base64.StdEncoding.EncodeToString(localTree.GetRootHash()))
		return nil
	}

	return nil
}

func getTree(ctx context.Context, store Storage) (*merkle.Tree, error) {
	hashes, err := store.GetHashes(ctx)
	if err != nil {
		return nil, err
	}

	objects, err := getTreeObjects(ctx, hashes)
	if err != nil {
		return nil, err
	}

	tree, err := createMerkleTree(ctx, objects)
	if err != nil {
		return nil, err
	}

	fmt.Printf("tree: %v", tree.ToString(ctx))

	return tree, nil
}

// getTreeObjects will return a sorted serialized object collection
func getTreeObjects(ctx context.Context, hashes map[string]Hash) (TreeObjectCollection, error) {

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

// createMerkleTree will return a merkle tree for a collection of serialized objects
func createMerkleTree(ctx context.Context, objects TreeObjectCollection) (*merkle.Tree, error) {
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
