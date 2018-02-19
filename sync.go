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
		fmt.Printf("Tree Root matches: %v\n", base64.StdEncoding.EncodeToString(localTree.GetRootHash()))
		return nil
	}

	// Experiments in walking the tree.
	// Maybe I need to walk the tree at the same time.

	// fmt.Printf("Tree Depth: local: %v remote: %v\n", localTree.Depth, remoteTree.Depth)
	// if localTree.Depth != remoteTree.Depth {
	// 	// Fall back to hash list?
	// } else {
	// 	checkIndexes := []int{0}
	// 	// Under the root check layers
	// 	for l := 1; l < localTree.Depth; l++ {
	// 		//fmt.Printf("Check Layer %v from %v\n", l, len(localTree.Layers))
	// 		localLayer := localTree.Layers[l]
	// 		remoteLayer := remoteTree.Layers[l]

	// 		layerLen := len(localLayer)

	// 		// We check the left nodes
	// 		// And any ODD node for differences
	// 		childrenIndexes := []int{}
	// 		for _, b := range checkIndexes {
	// 			// Check me.  Plus my sibling.
	// 			localNode := localLayer[b]
	// 			remoteNode := remoteLayer[b]

	// 			// fmt.Printf("Check Left Node %v-%v from %v - ", l, b, len(localLayer))

	// 			// fmt.Printf("local: %s %v - remote: %s %v\n",
	// 			// 	base64.StdEncoding.EncodeToString(localNode.Hash),
	// 			// 	localNode.IsLeft,
	// 			// 	base64.StdEncoding.EncodeToString(remoteNode.Hash),
	// 			// 	remoteNode.IsLeft)

	// 			if !bytes.Equal(localNode.Hash, remoteNode.Hash) {
	// 				if localNode.Type == merkle.NodeTypeLeaf {
	// 					fmt.Printf("FOUND A LEAF!!!: %s", string(localNode.ExtraData))
	// 				}

	// 				childrenIndexes = append(childrenIndexes, b*2)
	// 			}

	// 			// Have we looked at a promoted node?
	// 			if b+1 == layerLen {
	// 				continue
	// 			}

	// 			localSiblingNode := localLayer[b+1]
	// 			remoteSiblingNode := remoteLayer[b+1]

	// 			// fmt.Printf("localsib: %s %v - remotesib: %s %v\n",
	// 			// 	base64.StdEncoding.EncodeToString(localSiblingNode.Hash),
	// 			// 	localSiblingNode.IsLeft,
	// 			// 	base64.StdEncoding.EncodeToString(remoteSiblingNode.Hash),
	// 			// 	remoteSiblingNode.IsLeft)

	// 			if !bytes.Equal(localSiblingNode.Hash, remoteSiblingNode.Hash) {
	// 				if localNode.Type == merkle.NodeTypeLeaf {
	// 					fmt.Printf("FOUND A LEAF!!!: %s", string(localNode.ExtraData))
	// 				}
	// 				childrenIndexes = append(childrenIndexes, (b+1)*2)
	// 			}
	// 		}

	// 		// Do we need to search anymore?
	// 		if len(childrenIndexes) == 0 {
	// 			break
	// 		}
	// 		// On next layer check the following indexes
	// 		checkIndexes = childrenIndexes

	// 	}
	// }

	// // Depth of tree is different.
	// if localTree.Depth == remoteTree.Depth {
	// 	fmt.Printf("Tree Depth is same: local: %v remote: %v\n", localTree.Depth, remoteTree.Depth)
	// 	// Descend layers of the tree looking for differences
	// 	// Check children of root
	// 	checkIndexes := []int{0, 1}
	// 	// Skip root layer
	// 	for i := 1; i < localTree.Depth; i++ {
	// 		fmt.Printf("Check Layer %v from %v\n", i, len(localTree.Layers[i]))
	// 		childrenIndexes := []int{}
	// 		for _, b := range checkIndexes {
	// 			fmt.Printf("Check Node %v-%v\n", i, b)
	// 			localNode := localTree.Layers[i][b]
	// 			remoteNode := remoteTree.Layers[i][b]
	// 			// Do the nodes match?  If not follow the branch down
	// 			if !bytes.Equal(localNode.Hash, remoteNode.Hash) {
	// 				fmt.Printf("Node difference %v-%v: local: %s remote: %s\n", i, b, base64.StdEncoding.EncodeToString(localNode.Hash), base64.StdEncoding.EncodeToString(remoteNode.Hash))
	// 				// On next pass drop down to child and check sibling
	// 				childrenIndexes = append(childrenIndexes, b*2)
	// 				childrenIndexes = append(childrenIndexes, (b*2)+1)
	// 			}
	// 		}
	// 		// On next layer check the following indexes
	// 		checkIndexes = childrenIndexes
	// 	}
	//}

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
